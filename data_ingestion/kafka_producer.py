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
    # Aggiungi un controllo di sicurezza per catturare altri tipi di dati
    try:
        return obj.json()
    except:
        logging.error(f"Errore di serializzazione per l'oggetto di tipo: {type(obj)}")
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
        logging.error(f"Impossibile connettersi al broker Kafka: {e}")
        return None

def produce_all_matches(producer, data_dir, topic_name='lol-matches'):
    if not producer:
        logging.error("Produttore non disponibile. Interruzione del processo.")
        return

    match_files = [
        #"europe_matches_1.json",
        #"americas_matches_3.json",
        #"asia_matches_4.json",
        #"americas_matches_reduced_500.json",
        "americas_matches_reduced_10k.json"
    ]

    for match_file_name in match_files:
        match_file_path = os.path.join(data_dir, match_file_name)
        
        try:
            logging.info(f"Elaborazione del file: {match_file_path}")
            
            with open(match_file_path, 'rb') as f:
                # Usa 'matches.item' per una ricerca più specifica se necessario, in base alla struttura del tuo JSON
                for match_data in ijson.items(f, 'match_data.item'):
                    try:
                        # DEBUG: Stampa il tipo di dato per ogni oggetto JSON
                        logging.info(f"DEBUG: Trovato oggetto di tipo {type(match_data)}")
                        
                        # Forza l'invio sincrono e cattura il risultato o l'errore
                        record_metadata = producer.send(topic_name, value=match_data).get(timeout=10)
                        
                        match_id = match_data.get('gameId', 'N/A')
                        logging.info(f"Partita {match_id} inviata. Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                    
                    except Exception as e:
                        match_id = match_data.get('info', {}).get('gameId', 'N/A')
                        logging.error(f"ERRORE GRAVE: Impossibile inviare la partita {match_id}. Errore: {e}")
            producer.flush()
        except FileNotFoundError:
            logging.error(f"File non trovato: {match_file_path}. Saltando.")
        except Exception as e:
            logging.error(f"Errore durante l'elaborazione del file {match_file_path}: {e}")
    
    producer.flush()
    logging.info("Tutti i dati sono stati inviati.")


if __name__ == '__main__':
    KAGGLE_DATA_PATH = 'G:\\BigDataCodes\\kaggle_data'
    
    producer = create_producer()
    
    if producer:
        produce_all_matches(producer, KAGGLE_DATA_PATH)