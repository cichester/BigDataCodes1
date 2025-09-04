# match_producer_v2.py (versione definitiva e funzionante)

import ijson
import os
import time
from kafka import KafkaProducer
import logging
import json
from decimal import Decimal

# Configurazione del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def json_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
        )
        logging.info("Produttore Kafka creato con successo.")
        return producer
    except Exception as e:
        logging.error(f"Impossibile connettersi al broker Kafka. Assicurati che sia in esecuzione: {e}")
        return None

def get_region_and_file(region: str):
    """
    Determina il nome del file della partita in base alla regione.
    """
    if region == "EUW1":
        return "europe_matches_3.json"
    elif region == "NA1":
        return "americas_matches_1.json"
    elif region == "KR":
        return "asia_matches_4.json"
    else:
        return None

def produce_matches_by_puuid(producer, data_dir, puuid_file_path, region, topic_name='lol-matches'):
    """
    Legge i PUUID, trova il file JSON della partita corrispondente e lo invia a Kafka.
    """
    if not producer:
        logging.error("Produttore non disponibile. Interruzione del processo.")
        return

    # Mappa il file dei PUUID al file delle partite
    matches_file_name = get_region_and_file(region)
    if not matches_file_name:
        logging.error(f"Regione non valida per il file: {puuid_file_path}")
        return
    
    matches_file_path = os.path.join(data_dir, matches_file_name)

    try:
        with open(puuid_file_path, 'r', encoding='utf-8') as f:
            puuids_list = json.load(f)
            puuids = set(puuids_list)
        logging.info(f"Trovati {len(puuids)} PUUID da processare dal file {puuid_file_path}.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Errore nella lettura del file dei PUUID: {e}")
        return

    try:
        logging.info(f"Elaborazione del file di partite: {matches_file_path}")
        with open(matches_file_path, 'rb') as f:
            # Scorre tutti gli oggetti nel file usando ijson
            for match_data in ijson.items(f, 'item'):
                # Estrae i PUUID da ogni partita
                participant_puuids = {participant.get('puuid') for participant in match_data.get('info', {}).get('participants', [])}
                
                # Controlla se la partita contiene almeno uno dei PUUID che stiamo cercando
                if not puuids.isdisjoint(participant_puuids):
                    try:
                        producer.send(topic_name, value=match_data)
                        match_id = match_data.get('metadata', {}).get('matchId', 'N/A')
                        logging.info(f"Partita {match_id} ingerita con successo per la regione {region}.")
                    except Exception as e:
                        logging.error(f"Errore di invio a Kafka per la partita {match_id}: {e}")
    except Exception as e:
        logging.error(f"Errore durante l'elaborazione del file {matches_file_path}: {e}")
        return

    producer.flush()
    logging.info(f"Produzione di dati completata per la regione {region}.")

if __name__ == '__main__':
    KAGGLE_DATA_PATH = 'G:\\BigDataCodes\\kaggle_data'
    PLAYERS_DATA_PATH = os.path.join(KAGGLE_DATA_PATH, 'players')

    producer = create_producer()
    
    if producer:
        # Definizione dei file e delle regioni da processare
        regions_config = [
            ("EUW1", os.path.join(PLAYERS_DATA_PATH, 'EUW1_puuids.json')),
            ("NA1", os.path.join(PLAYERS_DATA_PATH, 'NA1_puuids.json')),
            ("KR", os.path.join(PLAYERS_DATA_PATH, 'KR_puuids.json'))
        ]
        
        for region, puuid_file in regions_config:
            produce_matches_by_puuid(producer, KAGGLE_DATA_PATH, puuid_file, region)