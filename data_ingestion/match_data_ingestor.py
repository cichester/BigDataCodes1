# data_ingestion/match_data_ingestor.py

import logging
import json
from kafka import KafkaConsumer
import os

logging.basicConfig(
    level=logging.INFO, # o DEBUG per più dettagli
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MatchDataIngestor:
    def __init__(self, neo4j_connector, data_dragon_downloader):
        self.db_connector = neo4j_connector
        self.data_dragon_downloader = data_dragon_downloader
        logger.info("MatchDataIngestor inizializzato per l'ingestione da flusso Kafka.")
        
    def close(self):
        """Chiude la connessione al database Neo4j."""
        self.db_connector.get_driver().close()

    def ingest_matches_from_kafka(self, topic_name='lol-matches', bootstrap_servers='localhost:9092'):
        """
        Acquisisce partite in tempo reale da un topic Kafka, gestendo errori di decodifica JSON.
        """
        consumer = None
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='neo4j-ingestor-group'
            )
            logger.info(f"Consumatore Kafka connesso al topic '{topic_name}'. In attesa di messaggi...")

            for message in consumer:
                try:
                    match_data = json.loads(message.value.decode('utf-8'))
                    match_id = match_data.get('gameId', 'N/A')
                    self._process_and_ingest_match(match_id, match_data)
                    logger.info(f"Partita {match_id} ingerita con successo.")
                except json.JSONDecodeError as e:
                    logger.error(f"Errore di decodifica JSON per un messaggio dal topic. Messaggio ignorato. Errore: {e}")
                except Exception as e:
                    logger.error(f"Errore durante l'ingestione della partita: {e}")
                    
        except Exception as e:
            logger.error(f"Errore grave nel consumatore Kafka: {e}")
        finally:
            if consumer and consumer.bootstrap_connected():
                consumer.close()
            self.close()

    def _process_and_ingest_match(self, match_id: str, match_data: dict):
        """
        Processa i dati di una singola partita e li ingesta in Neo4j.
        """
        logger.debug(f"Processando dati per la partita: {match_id}")

        with self.db_connector.get_driver().session() as session:
            set_clauses = []
            params = {'match_id': match_id}
            
            game_creation_timestamp = match_data.get('gameCreation')
            if game_creation_timestamp is not None:
                set_clauses.append("m.creation = datetime({epochMillis: $game_creation_timestamp})")
                params['game_creation_timestamp'] = game_creation_timestamp
            
            game_duration = match_data.get('gameDuration')
            if game_duration is not None:
                set_clauses.append("m.duration = $game_duration")
                params['game_duration'] = game_duration
            
            game_end_timestamp = match_data.get('gameEndTimestamp')
            if game_end_timestamp is not None:
                set_clauses.append("m.endTimestamp = datetime({epochMillis: $game_end_timestamp})")
                params['game_end_timestamp'] = game_end_timestamp
            
            game_mode = match_data.get('gameMode')
            
            if game_mode == "CLASSIC":                
                set_clauses.append("m.mode = $game_mode")
                params['game_mode'] = game_mode
            
            game_name = match_data.get('gameName')
            if game_name is not None:
                set_clauses.append("m.name = $game_name")
                params['game_name'] = game_name

            game_start_timestamp = match_data.get('gameStartTimestamp')
            if game_start_timestamp is not None:
                set_clauses.append("m.startTimestamp = datetime({epochMillis: $game_start_timestamp})")
                params['game_start_timestamp'] = game_start_timestamp

            game_type = match_data.get('gameType')
            if game_type is not None:
                set_clauses.append("m.type = $game_type")
                params['game_type'] = game_type
            
            game_version = match_data.get('gameVersion')
            if game_version is not None:
                set_clauses.append("m.version = $game_version")
                params['game_version'] = game_version

            map_id = match_data.get('mapId')
            if map_id is not None:
                set_clauses.append("m.mapId = $map_id")
                params['map_id'] = map_id

            cypher_query = f"MERGE (m:Match {{id: $match_id}})"
            if set_clauses:
                set_clause_str = ", ".join(set_clauses)
                cypher_query += f" ON CREATE SET {set_clause_str} ON MATCH SET {set_clause_str}"
            
            session.run(cypher_query, **params)
            
            participants_data = match_data.get('participants', [])
            for participant in participants_data:
                self._process_participant(session, match_id, participant)

            teams_data = match_data.get('teams', [])
            for team in teams_data:
                self._process_team(session, match_id, team)

            logger.debug(f"Ingestione dettagli completata per la partita: {match_id}")

    def _process_participant(self, session, match_id: str, participant_data: dict):
        puuid = participant_data.get('puuid')
        summoner_id = participant_data.get('summonerId')
        summoner_name = participant_data.get('summonerName')
        champion_id = participant_data.get('championId')
        champion_name = participant_data.get('championName')
        team_id = participant_data.get('teamId')
        team_position = participant_data.get('teamPosition')
        if team_position == "UTILITY":
            team_position = "SUPPORT"
        challenges_data = participant_data.get('challenges', {})
        damage_per_minute = challenges_data.get('damagePerMinute')
        deaths_by_enemy_champs = challenges_data.get('deathsByEnemyChamps')
        dragon_takedowns = challenges_data.get('dragonTakedowns')

        if not champion_name and champion_id:
            champion_info = self.data_dragon_downloader.get_champion_by_id(champion_id)
            champion_name = champion_info.get('name') if champion_info else f"Unknown_Champion_{champion_id}"
            logger.warning(f"ChampionName non trovato nel JSON per ID {champion_id}, recuperato da Data Dragon: {champion_name}")

        # --- Nodo Player ---
        session.run(
            """
            MERGE (p:Player {puuid: $puuid})
            ON CREATE SET
                p.summonerId = $summoner_id,
                p.summonerName = $summoner_name
            ON MATCH SET
                p.summonerId = $summoner_id,
                p.summonerName = $summoner_name
            """,
            puuid=puuid,
            summoner_id=summoner_id,
            summoner_name=summoner_name
        )

        # --- Nodo Champion e relazione PLAYED_AS ---
        session.run(
            """
            MATCH (p:Player {puuid: $puuid})
            MERGE (c:Champion {name: $champion_name})
            MERGE (p)-[:PLAYED_AS]->(c)
            """,
            puuid=puuid,
            champion_name=champion_name
        )
        
        # --- Relazione Player PLAYED_IN Match con statistiche (ottimizzata) ---
        session.run(
            """
            MATCH (p:Player {puuid: $puuid})
            MATCH (m:Match {id: $match_id})
            MERGE (p)-[r:PLAYED_IN]->(m)
            ON CREATE SET
                r.kills = $kills,
                r.deaths = $deaths,
                r.assists = $assists,
                r.win = $win,
                r.goldEarned = $goldEarned,
                r.totalDamageDealtToChampions = $totalDamageDealtToChampions,
                r.champLevel = $champLevel,
                r.totalMinionsKilled = $totalMinionsKilled,
                r.visionScore = $visionScore,
                r.wardsPlaced = $wardsPlaced,
                r.wardsKilled = $wardsKilled,
                r.detectorWardsPlaced = $detectorWardsPlaced,
                r.physicalDamageDealtToChampions = $physicalDamageDealtToChampions,
                r.magicDamageDealtToChampions = $magicDamageDealtToChampions,
                r.trueDamageDealtToChampions = $trueDamageDealtToChampions,
                r.totalDamageDealt = $totalDamageDealt,
                r.physicalDamageTaken = $physicalDamageTaken,
                r.magicDamageTaken = $magicDamageTaken,
                r.trueDamageTaken = $trueDamageTaken,
                r.role = $teamPosition,
                r.teamId = $teamId,
                r.damagePerMinute = $damage_per_minute,
                r.deathsByEnemyChamps = $deaths_by_enemy_champs,
                r.dragonTakedowns = $dragon_takedowns
            ON MATCH SET
                r.kills = $kills,
                r.deaths = $deaths,
                r.assists = $assists,
                r.win = $win,
                r.goldEarned = $goldEarned,
                r.totalDamageDealtToChampions = $totalDamageDealtToChampions,
                r.champLevel = $champLevel,
                r.totalMinionsKilled = $totalMinionsKilled,
                r.visionScore = $visionScore,
                r.wardsPlaced = $wardsPlaced,
                r.wardsKilled = $wardsKilled,
                r.detectorWardsPlaced = $detectorWardsPlaced,
                r.physicalDamageDealtToChampions = $physicalDamageDealtToChampions,
                r.magicDamageDealtToChampions = $magicDamageDealtToChampions,
                r.trueDamageDealtToChampions = $trueDamageDealtToChampions,
                r.totalDamageDealt = $totalDamageDealt,
                r.physicalDamageTaken = $physicalDamageTaken,
                r.magicDamageTaken = $magicDamageTaken,
                r.trueDamageTaken = $trueDamageTaken,
                r.role = $teamPosition,
                r.teamId = $teamId,
                r.damagePerMinute = $damage_per_minute,
                r.deathsByEnemyChamps = $deaths_by_enemy_champs,
                r.dragonTakedowns = $dragon_takedowns
            """,
            puuid=puuid,
            match_id=match_id,
            kills=participant_data.get('kills'),
            deaths=participant_data.get('deaths'),
            assists=participant_data.get('assists'),
            win=participant_data.get('win'),
            goldEarned=participant_data.get('goldEarned'),
            totalDamageDealtToChampions=participant_data.get('totalDamageDealtToChampions'),
            champLevel=participant_data.get('champLevel'),
            totalMinionsKilled=participant_data.get('totalMinionsKilled'),
            visionScore=participant_data.get('visionScore'),
            wardsPlaced=participant_data.get('wardsPlaced'),
            wardsKilled=participant_data.get('wardsKilled'),
            detectorWardsPlaced=participant_data.get('detectorWardsPlaced'),
            physicalDamageDealtToChampions=participant_data.get('physicalDamageDealtToChampions'),
            magicDamageDealtToChampions=participant_data.get('magicDamageDealtToChampions'),
            trueDamageDealtToChampions=participant_data.get('trueDamageDealtToChampions'),
            totalDamageDealt=participant_data.get('totalDamageDealt'),
            physicalDamageTaken=participant_data.get('physicalDamageTaken'),
            magicDamageTaken=participant_data.get('magicDamageTaken'),
            trueDamageTaken=participant_data.get('trueDamageTaken'),
            teamPosition=team_position,
            teamId=team_id,
            damage_per_minute=damage_per_minute,
            deaths_by_enemy_champs=deaths_by_enemy_champs,
            dragon_takedowns=dragon_takedowns
        )

        # --- Relazione Player BELONGS_TO Team (ottimizzata) ---
        session.run(
            """
            MATCH (p:Player {puuid: $puuid})
            MATCH (t:Team {id: $team_id, matchId: $match_id})
            MERGE (p)-[:BELONGS_TO]->(t)
            """,
            puuid=puuid,
            team_id=team_id,
            match_id=match_id
        )

        # --- Processa gli Items (query ottimizzata) ---
        #logger.info(f"Processando gli item per il partecipante: {puuid}")
        for i in range(7):
            item_id = participant_data.get(f'item{i}')
            if item_id and item_id != 0:
                item_id_str = str(item_id)
                logger.debug(f"Trovato Item ID {item_id_str}. Eseguendo la query...")
                session.run(
                    """
                    MATCH (p:Player {puuid: $puuid})
                    MATCH (m:Match {id: $match_id})
                    MATCH (item:Item {id: $item_id_str})
                    MERGE (p)-[:BOUGHT_ITEM_IN {matchId: $match_id}]->(item)
                    """,
                    puuid=puuid,
                    match_id=match_id,
                    item_id_str=item_id_str
                )
            
            else:
                logger.debug(f"Item ID {f'item{i}'} non trovato o uguale a 0.")
        
        # --- Processa Summoner Spells (query ottimizzata) ---
        summoner_spell1_id = participant_data.get('summoner1Id')
        summoner_spell2_id = participant_data.get('summoner2Id')

        if summoner_spell1_id:
            session.run(
                """
                MATCH (p:Player {puuid: $puuid})
                MATCH (m:Match {id: $match_id})
                MERGE (ss:SummonerSpell {id: $summoner_spell_id})
                MERGE (p)-[:USED_SUMMONER_SPELL_IN {matchId: $match_id, slot: 1}]->(ss)
                """,
                puuid=puuid,
                match_id=match_id,
                summoner_spell_id=summoner_spell1_id
            )
        if summoner_spell2_id:
            session.run(
                """
                MATCH (p:Player {puuid: $puuid})
                MATCH (m:Match {id: $match_id})
                MERGE (ss:SummonerSpell {id: $summoner_spell_id})
                MERGE (p)-[:USED_SUMMONER_SPELL_IN {matchId: $match_id, slot: 2}]->(ss)
                """,
                puuid=puuid,
                match_id=match_id,
                summoner_spell_id=summoner_spell2_id
            )

        # --- Processa Rune (perks) (query ottimizzata) ---
        perks_data = participant_data.get('perks', {})
        styles = perks_data.get('styles', [])

        for style in styles:
            primary_style_id = style.get('style')
            selections = style.get('selections', [])

            if primary_style_id:
                session.run(
                    """
                    MATCH (p:Player {puuid: $puuid})
                    MATCH (m:Match {id: $match_id})
                    MERGE (rp:RunePath {id: $rune_path_id})
                    MERGE (p)-[:USED_RUNE_PATH_IN {matchId: $match_id}]->(rp)
                    """,
                    puuid=puuid,
                    match_id=match_id,
                    rune_path_id=primary_style_id
                )

            for selection in selections:
                rune_id = selection.get('perk')
                if rune_id:
                    session.run(
                        """
                        MATCH (p:Player {puuid: $puuid})
                        MATCH (m:Match {id: $match_id})
                        MERGE (r:Rune {id: $rune_id})
                        MERGE (p)-[:SELECTED_RUNE_IN {matchId: $match_id}]->(r)
                        """,
                        puuid=puuid,
                        match_id=match_id,
                        rune_id=rune_id
                    )

    def _process_team(self, session, match_id: str, team_data: dict):
        """
        Crea nodi Team e le relazioni con Match e obiettivi in modo ottimizzato.
        """
        team_id = team_data.get('teamId')
        win = team_data.get('win')

        # Step 1: Crea o aggiorna il nodo Team e la relazione con Match
        session.run(
            """
            MATCH (m:Match {id: $match_id})
            MERGE (t:Team {id: $team_id, matchId: $match_id})
            ON CREATE SET t.win = $win
            ON MATCH SET t.win = $win
            MERGE (t)-[:PLAYED_IN]->(m)
            """,
            team_id=team_id,
            match_id=match_id,
            win=win
        )

        # Step 2: Processa obiettivi
        objectives = team_data.get('objectives', {})
        for objective_name, objective_data in objectives.items():
            if objective_data.get('kills', 0) > 0:
                session.run(
                    """
                    MATCH (m:Match {id: $match_id})
                    MATCH (t:Team {id: $team_id, matchId: m.id})
                    MERGE (o:Objective {name: $objective_name})
                    MERGE (t)-[:SECURED {kills: $kills}]->(o)
                    """,
                    team_id=team_id,
                    match_id=match_id,
                    objective_name=objective_name,
                    kills=objective_data.get('kills')
                )

        # Step 3: Processa i bans
        bans = team_data.get('bans', [])
        for ban in bans:
            champion_id = ban.get('championId')
            pick_turn = ban.get('pickTurn')
            
            if champion_id:
                try:
                    champion_info = self.data_dragon_downloader.get_champion_by_id(champion_id)
                    champion_name = champion_info.get('name') if champion_info else f"Unknown_Champion_{champion_id}"
                except AttributeError:
                    champion_name = f"Unknown_Champion_{champion_id}"
                    logger.error("ERRORE: get_champion_by_id non trovato in DataDragonDownloader. Il nome del campione sarà un placeholder.")
                
                session.run(
                    """
                    MATCH (t:Team {id: $team_id, matchId: $match_id})
                    MATCH (c:Champion {name: $champion_name})
                    MERGE (t)-[:BANNED {pickTurn: $pick_turn}]->(c)
                    """,
                    team_id=team_id,
                    match_id=match_id,
                    champion_name=champion_name,
                    pick_turn=pick_turn
                )