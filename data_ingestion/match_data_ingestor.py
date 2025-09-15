# data_ingestion/match_data_ingestor.py

import logging
import json
import re
import time # Importa il modulo time per la misurazione
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

class MatchDataIngestor:
    def __init__(self, neo4j_connector, data_dragon_downloader):
        self.db_connector = neo4j_connector
        self.data_dragon_downloader = data_dragon_downloader
        self.champion_lookup_map = self._build_champion_lookup_map()
        logger.info(f"MatchDataIngestor inizializzato. Creata mappa di lookup con {len(self.champion_lookup_map)} voci.")

    # --- METODO DI TEST "LENTO" (UNO PER UNO) ---
    def ingest_matches_from_kafka_single(self, topic_name='lol-matches', bootstrap_servers='localhost:9092'):
        """
        [METODO DI TEST] Consuma messaggi da Kafka e li ingerisce uno per uno.
        """
        logger.info("--- AVVIO TEST: INGESTIONE SINGOLA ---")
        start_time = time.time()
        total_processed = 0
        
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='neo4j-ingestor-group-single', # Gruppo diverso per il test
                consumer_timeout_ms=15000 # Termina dopo 15s di inattività
            )
            logger.info(f"Consumatore (singolo) connesso a '{topic_name}'.")

            for message in consumer:
                try:
                    match_data = json.loads(message.value.decode('utf-8'))
                    if match_data.get('gameMode') == 'CLASSIC':
                        params = self._prepare_match_params(match_data)
                        if params:
                            # Invia un batch contenente una sola partita
                            self._bulk_insert_matches([params])
                            total_processed += 1
                except json.JSONDecodeError:
                    logger.warning(f"Messaggio non JSON. Ignoro.")
            
        except Exception as e:
            logger.critical(f"Errore grave nel consumatore (singolo): {e}", exc_info=True)
        finally:
            end_time = time.time()
            self._print_performance_report("Ingestione Singola", start_time, end_time, total_processed)
            if 'consumer' in locals() and consumer:
                consumer.close()

    # --- METODO OTTIMIZZATO (MICRO-BATCH) ---
    def ingest_matches_from_kafka(self, topic_name='lol-matches', bootstrap_servers='localhost:9092', batch_size=100, batch_timeout_seconds=5):
        """
        [METODO OTTIMIZZATO] Consuma messaggi da Kafka in micro-batch.
        """
        logger.info("--- AVVIO TEST: INGESTIONE A MICRO-BATCH ---")
        start_time = time.time()
        total_processed = 0

        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='neo4j-ingestor-group-batch',
                consumer_timeout_ms= (batch_timeout_seconds + 5) * 1000 # Timeout leggermente più alto
            )
            logger.info(f"Consumatore (batch) connesso a '{topic_name}'.")

            message_batch = []
            while True:
                messages = consumer.poll(timeout_ms=batch_timeout_seconds * 1000, max_records=batch_size)
                if not messages:
                    logger.info("Nessun nuovo messaggio. Rimango in attesa...")
                    #break # Esce se non ci sono più messaggi dopo il timeout

                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            message_batch.append(json.loads(record.value.decode('utf-8')))
                        except json.JSONDecodeError:
                            logger.warning(f"Messaggio non JSON. Ignoro.")
                
                if message_batch:
                    filtered_batch = [data for data in message_batch if data.get('gameMode') == 'CLASSIC']
                    params_batch = [self._prepare_match_params(data) for data in filtered_batch]
                    params_batch = [p for p in params_batch if p is not None]
                    
                    if params_batch:
                        self._bulk_insert_matches(params_batch)
                        total_processed += len(params_batch)

                    message_batch = []
        
        except Exception as e:
            logger.critical(f"Errore grave nel consumatore (batch): {e}", exc_info=True)
        finally:
            end_time = time.time()
            self._print_performance_report("Ingestione a Micro-Batch", start_time, end_time, total_processed)
            if 'consumer' in locals() and consumer:
                consumer.close()

    # --- METODI INTERNI (rimangono invariati) ---
    def _prepare_match_params(self, match_data: dict) -> dict:
        # Il tuo codice esistente va qui... (omesso per brevità)
        match_id=match_data.get('gameId')
        if not match_id:return None
        teams_list=[]
        for t_data in match_data.get('teams',[]):
            banned_champion_keys=[str(ban.get('championId'))for ban in t_data.get('bans',[])if ban.get('championId',-1)!=-1]
            teams_list.append({'teamId':t_data.get('teamId'),'win':t_data.get('win'),'bannedChampions':banned_champion_keys})
        return{'match_props':{'id':match_id,'duration':match_data.get('gameDuration'),'gameMode':match_data.get('gameMode'),'creation':match_data.get('gameCreation')},'participants':[{'puuid':p.get('puuid'),'summonerName':p.get('summonerName'),'championName':self._get_canonical_champion_name(p.get('championName')),'teamId':p.get('teamId'),'stats':{'championName':self._get_canonical_champion_name(p.get('championName')),'win':p.get('win'),'kills':p.get('kills'),'deaths':p.get('deaths'),'assists':p.get('assists'),'goldEarned':p.get('goldEarned'),'role':'SUPPORT'if p.get('teamPosition')=='UTILITY'else p.get('teamPosition')}}for p in match_data.get('participants',[])if p.get('puuid')],'teams':teams_list}

    def _bulk_insert_matches(self, all_matches_data: list):
        # Il tuo codice esistente va qui... (omesso per brevità)
        if not all_matches_data:return
        logger.info(f"Invio di un batch di {len(all_matches_data)} partite al database...")
        query="""UNWIND $all_matches AS match_data MERGE (m:Match {id: match_data.match_props.id})SET m.duration = match_data.match_props.duration, m.gameMode = match_data.match_props.gameMode, m.creation = datetime({epochMillis: match_data.match_props.creation})WITH m, match_data.participants AS participants, match_data.teams AS teams UNWIND participants AS p_data MERGE (p:Player {puuid: p_data.puuid})ON CREATE SET p.summonerName = p_data.summonerName WITH m, p, p_data, teams MERGE (c:Champion {name: p_data.championName})MERGE (p)-[rel:PLAYED_IN]->(m)SET rel += p_data.stats WITH m, teams UNWIND teams AS t_data MERGE (t:Team {matchId: m.id, teamId: t_data.teamId})SET t.win = t_data.win MERGE (t)-[:PARTICIPATED_IN]->(m)WITH m, t, t_data MATCH (p:Player)-[r:PLAYED_IN]->(m)WHERE r.teamId = t_data.teamId MERGE (p)-[:PLAYED_FOR]->(t)WITH t, t_data UNWIND t_data.bannedChampions AS banned_champ_key MATCH (banned_champ:Champion {key: banned_champ_key})MERGE (t)-[:BANNED]->(banned_champ)"""
        params={'all_matches':all_matches_data}
        try:self.db_connector.run_query(query,params);logger.info(f"Batch di {len(all_matches_data)} partite ingerito con successo.")
        except Exception as e:logger.error(f"Errore durante l'esecuzione della query in batch: {e}",exc_info=True)
        
    def _print_performance_report(self, test_name, start_time, end_time, total_processed):
        """Funzione di utilità per stampare i risultati del test."""
        duration = end_time - start_time
        throughput = total_processed / duration if duration > 0 else 0
        
        print("\n" + "="*50)
        print(f"RISULTATI DEL TEST: {test_name}")
        print("="*50)
        print(f"Tempo totale di esecuzione: {duration:.2f} secondi")
        print(f"Partite 'CLASSIC' totali ingerite: {total_processed}")
        print(f"Throughput: {throughput:.2f} partite al secondo")
        print("="*50 + "\n")
        
    def _normalize_name(self, name: str) -> str:
        if not name: return ""
        return re.sub(r'[^a-z0-9]', '', name.lower())

    def _build_champion_lookup_map(self) -> dict:
        try:
            result = self.db_connector.run_query("MATCH (c:Champion) RETURN c.id AS id, c.name AS name")
            lookup_map = {}
            for record in result:
                canonical_name = record['name']
                normalized_id = self._normalize_name(record['id'])
                lookup_map[normalized_id] = canonical_name
                normalized_name = self._normalize_name(canonical_name)
                lookup_map[normalized_name] = canonical_name
            return lookup_map
        except Exception as e:
            logger.error(f"Impossibile costruire la mappa di lookup: {e}")
            return {}

    def _get_canonical_champion_name(self, messy_name: str) -> str:
        if not messy_name: return "Unknown"
        normalized = self._normalize_name(messy_name)
        return self.champion_lookup_map.get(normalized, messy_name)