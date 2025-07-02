# data_ingestion/match_data_ingestor.py

import logging
import json
import os
import glob

logger = logging.getLogger(__name__)

class MatchDataIngestor:
    def __init__(self, neo4j_connector, data_dragon_downloader):
        self.db_connector = neo4j_connector
        self.data_dragon_downloader = data_dragon_downloader
        logger.info("MatchDataIngestor inizializzato per l'ingestione da dataset Kaggle.")

    def ingest_matches_from_kaggle(self, kaggle_data_base_path: str):
        """
        Avvia il processo di ingestione dei dati di partita dal dataset Kaggle.
        Adattato alla struttura JSON corretta (direttamente [match_objects] o {"match_data": [...]}).
        """
        logger.info(f"Inizio ingestione partite dal dataset Kaggle nel percorso: {kaggle_data_base_path}")

        match_files_pattern = os.path.join(kaggle_data_base_path, '*_matches_*.json')
        all_match_files = glob.glob(match_files_pattern)

        if not all_match_files:
            logger.warning(f"Nessun file di partite JSON trovato con il pattern '{match_files_pattern}'. Controlla il percorso e la nomenclatura dei file.")
            return

        total_ingested_matches = 0
        for file_path in all_match_files:
            logger.info(f"Processando file: {file_path}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_content = json.load(f)

                    matches_to_process = []
                    # ** Modifica qui per la struttura corretta: nessuna chiave 'root' **
                    if isinstance(raw_content, dict) and 'match_data' in raw_content:
                        matches_to_process = raw_content['match_data']
                    elif isinstance(raw_content, list): # Se il file JSON è direttamente un array di oggetti match
                        matches_to_process = raw_content
                    else:
                        logger.error(f"Struttura JSON inattesa o mancante 'match_data' o non è una lista nel file {file_path}. Saltando la processazione di questo file.")
                        continue

                    for match_data in matches_to_process:
                        # L'ID della partita è 'gameId' nel livello superiore dell'oggetto match
                        match_id = match_data.get('gameId')
                        if not match_id:
                            logger.warning(f"Match ID (gameId) non trovato in una partita dal file {file_path}. Saltando la partita.")
                            continue

                        try:
                            self._process_and_ingest_match(match_id, match_data)
                            total_ingested_matches += 1
                            logger.debug(f"Partita {match_id} ingesta con successo (Totale: {total_ingested_matches}).")
                        except Exception as e:
                            logger.error(f"Errore durante l'ingestione della partita {match_id} dal file {file_path}: {e}")

            except json.JSONDecodeError as e:
                logger.error(f"Errore di decodifica JSON nel file {file_path}: {e}. Assicurati che sia un JSON valido.")
            except Exception as e:
                logger.error(f"Errore generico durante la lettura o processazione del file {file_path}: {e}")

        logger.info(f"Ingestione dati di partita da Kaggle completata. Totale partite ingeste: {total_ingested_matches}.")


    def _process_and_ingest_match(self, match_id: str, match_data: dict):
        """
        Processa i dati di una singola partita e li ingesta in Neo4j.
        Adattato alla struttura del match object fornita.
        """
        logger.debug(f"Processando dati per la partita: {match_id}")

        with self.db_connector.get_driver().session() as session:
            # Estrazione delle proprietà del match
            # I timestamp sono float in millisecondi
            game_creation_timestamp = match_data.get('gameCreation')
            game_duration = match_data.get('gameDuration')
            game_end_timestamp = match_data.get('gameEndTimestamp')
            game_mode = match_data.get('gameMode')
            game_name = match_data.get('gameName')
            game_start_timestamp = match_data.get('gameStartTimestamp')
            game_type = match_data.get('gameType')
            game_version = match_data.get('gameVersion')
            map_id = match_data.get('mapId')
            
            session.run(
                """
                MERGE (m:Match {id: $match_id})
                ON CREATE SET
                    m.creation = datetime({epochMillis: $game_creation_timestamp}),
                    m.duration = $game_duration,
                    m.endTimestamp = datetime({epochMillis: $game_end_timestamp}),
                    m.mode = $game_mode,
                    m.name = $game_name,
                    m.startTimestamp = datetime({epochMillis: $game_start_timestamp}),
                    m.type = $game_type,
                    m.version = $game_version,
                    m.mapId = $map_id
                ON MATCH SET
                    m.creation = datetime({epochMillis: $game_creation_timestamp}),
                    m.duration = $game_duration,
                    m.endTimestamp = datetime({epochMillis: $game_end_timestamp}),
                    m.mode = $game_mode,
                    m.name = $game_name,
                    m.startTimestamp = datetime({epochMillis: $game_start_timestamp}),
                    m.type = $game_type,
                    m.version = $game_version,
                    m.mapId = $map_id
                """,
                match_id=match_id,
                game_creation_timestamp=game_creation_timestamp,
                game_duration=game_duration,
                game_end_timestamp=game_end_timestamp,
                game_mode=game_mode,
                game_name=game_name,
                game_start_timestamp=game_start_timestamp,
                game_type=game_type,
                game_version=game_version,
                map_id=map_id
            )
            
            # Processa i partecipanti
            participants_data = match_data.get('participants', [])
            for participant in participants_data:
                self._process_participant(session, match_id, participant)

            # Il tuo JSON fornito non mostra esplicitamente un array 'teams'
            # allo stesso livello di 'participants'.
            # Se esiste nel tuo dataset completo, dovrai trovare il percorso corretto.
            # Per ora, manterrò la chiamata, ma potresti doverla commentare o adattare.
            # teams_data = match_data.get('teams', []) # <-- VERIFICA QUESTO PERCORSO
            # for team in teams_data:
            #     self._process_team(session, match_id, team)

        logger.debug(f"Ingestione dettagli completata per la partita: {match_id}")

    def _process_participant(self, session, match_id: str, participant_data: dict):
        """
        Crea nodi Player, Champion e le relazioni per un partecipante,
        usando le chiavi fornite dal JSON del dataset Kaggle, inclusi i dati di 'challenges'.
        """
        puuid = participant_data.get('puuid')
        summoner_id = participant_data.get('summonerId')
        summoner_name = participant_data.get('summonerName')
        champion_id = participant_data.get('championId')
        champion_name = participant_data.get('championName') # Presente nel JSON!
        team_id = participant_data.get('teamId') # 100 o 200
        team_position = participant_data.get('teamPosition') # TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY

        # Dati da 'challenges'
        challenges_data = participant_data.get('challenges', {})
        damage_per_minute = challenges_data.get('damagePerMinute')
        deaths_by_enemy_champs = challenges_data.get('deathsByEnemyChamps')
        dragon_takedowns = challenges_data.get('dragonTakedowns')
        # Aggiungi qui altre proprietà da 'challenges' che ti interessano

        # Se championName è già presente nel JSON, lo usiamo. Altrimenti, fallback a Data Dragon.
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
        # Si assume che i campioni siano già stati ingeriti da Data Dragon
        session.run(
            """
            MATCH (c:Champion {name: $champion_name})
            MERGE (p:Player {puuid: $puuid})
            MERGE (p)-[:PLAYED_AS]->(c)
            """,
            puuid=puuid,
            champion_name=champion_name
        )
        
        # --- Relazione Player PLAYED_IN Match con statistiche ---
        session.run(
            """
            MATCH (p:Player {puuid: $puuid}), (m:Match {id: $match_id})
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
                r.teamPosition = $teamPosition,
                r.teamId = $teamId,
                r.damagePerMinute = $damage_per_minute,
                r.deathsByEnemyChamps = $deaths_by_enemy_champs,
                r.dragonTakedowns = $dragon_takedowns
                // Aggiungi qui altre proprietà da 'challenges_data' come desiderato
            ON MATCH SET // Aggiorna le proprietà se la relazione esiste già
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
                r.teamPosition = $teamPosition,
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

        # --- Relazione Player BELONGS_TO Team (se modelliamo nodi Team) ---
        session.run(
            """
            MERGE (t:Team {id: $team_id, matchId: $match_id}) // Crea nodo Team se non esiste
            MERGE (p:Player {puuid: $puuid})
            MERGE (p)-[:BELONGS_TO]->(t)
            """,
            puuid=puuid,
            team_id=team_id,
            match_id=match_id
        )

        # --- Processa gli Items ---
        for i in range(7): # item0, item1, ..., item6
            item_id = participant_data.get(f'item{i}')
            if item_id and item_id != 0: # 0 significa slot vuoto
                session.run(
                    """
                    MATCH (p:Player {puuid: $puuid}), (m:Match {id: $match_id}), (item:Item {id: $item_id})
                    MERGE (p)-[:BOUGHT_ITEM_IN {matchId: $match_id}]->(item)
                    """,
                    puuid=puuid,
                    match_id=match_id,
                    item_id=item_id
                )
        
        # --- Processa Summoner Spells ---
        summoner_spell1_id = participant_data.get('summoner1Id')
        summoner_spell2_id = participant_data.get('summoner2Id')

        if summoner_spell1_id:
            session.run(
                """
                MATCH (p:Player {puuid: $puuid}), (m:Match {id: $match_id}), (ss:SummonerSpell {id: $summoner_spell_id})
                MERGE (p)-[:USED_SUMMONER_SPELL_IN {matchId: $match_id, slot: 1}]->(ss)
                """,
                puuid=puuid,
                match_id=match_id,
                summoner_spell_id=summoner_spell1_id
            )
        if summoner_spell2_id:
            session.run(
                """
                MATCH (p:Player {puuid: $puuid}), (m:Match {id: $match_id}), (ss:SummonerSpell {id: $summoner_spell_id})
                MERGE (p)-[:USED_SUMMONER_SPELL_IN {matchId: $match_id, slot: 2}]->(ss)
                """,
                puuid=puuid,
                match_id=match_id,
                summoner_spell_id=summoner_spell2_id
            )

        # --- Processa Rune (perks) ---
        perks_data = participant_data.get('perks', {})
        styles = perks_data.get('styles', [])

        for style in styles:
            primary_style_id = style.get('style')
            selections = style.get('selections', [])

            if primary_style_id:
                session.run(
                    """
                    MATCH (p:Player {puuid: $puuid}), (m:Match {id: $match_id}), (rp:RunePath {id: $rune_path_id})
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
                        MATCH (p:Player {puuid: $puuid}), (m:Match {id: $match_id}), (r:Rune {id: $rune_id})
                        MERGE (p)-[:SELECTED_RUNE_IN {matchId: $match_id}]->(r)
                        """,
                        puuid=puuid,
                        match_id=match_id,
                        rune_id=rune_id
                    )

    def _process_team(self, session, match_id: str, team_data: dict):
        """
        Crea nodi Team e le relazioni con Match e obiettivi.
        """
        team_id = team_data.get('teamId')
        win = team_data.get('win')
        
        session.run(
            """
            MERGE (t:Team {id: $team_id, matchId: $match_id})
            ON CREATE SET t.win = $win
            ON MATCH SET t.win = $win
            """,
            team_id=team_id,
            match_id=match_id,
            win=win
        )

        # Processa obiettivi (baronKills, dragonKills, etc.)
        objectives = team_data.get('objectives', {})
        for objective_name, objective_data in objectives.items():
            if objective_data.get('kills', 0) > 0:
                session.run(
                    f"""
                    MATCH (t:Team {{id: $team_id, matchId: $match_id}})
                    MERGE (o:{objective_name} {{matchId: $match_id, teamId: $team_id}})
                    ON CREATE SET o.kills = $kills
                    ON MATCH SET o.kills = $kills
                    MERGE (t)-[:ACHIEVED]->(o)
                    """,
                    team_id=team_id,
                    match_id=match_id,
                    kills=objective_data.get('kills')
                )
        
        # Processa i bans
        bans = team_data.get('bans', [])
        for ban in bans:
            champion_id = ban.get('championId')
            pick_turn = ban.get('pickTurn')
            
            if champion_id:
                champion_info = self.data_dragon_downloader.get_champion_by_id(champion_id)
                champion_name = champion_info.get('name') if champion_info else f"Unknown_Champion_{champion_id}"
                
                session.run(
                    """
                    MATCH (t:Team {id: $team_id, matchId: $match_id}), (c:Champion {name: $champion_name})
                    MERGE (t)-[:BANNED {pickTurn: $pick_turn}]->(c)
                    """,
                    team_id=team_id,
                    match_id=match_id,
                    champion_name=champion_name,
                    pick_turn=pick_turn
                )