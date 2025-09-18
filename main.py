# main.py (versione finale)
import logging
import os
import json
from dotenv import load_dotenv

# Carica le variabili d'ambiente dal file .env
load_dotenv()

from config.settings import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
from neo4j_handler.connector import Neo4jConnector
from utils.data_dragon_downloader import DataDragonDownloader
from neo4j_handler.neo4j_ingester import (
    ingest_champions_to_neo4j,
    ingest_items_to_neo4j,
    ingest_runes_to_neo4j,
    ingest_summoner_spells_to_neo4j
    
)
from data_ingestion.match_data_ingestor import MatchDataIngestor
# NOTA: LLMClient non viene più importato.
from data_ingestion.lore_ingestor import LoreIngestor

# Configurazione del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    db_connector = None
    try:
        logger.info("Avvio del progetto League of Legends Knowledge Graph.")

        # --- FASE 1: Connessione a Neo4j ---
        db_connector = Neo4jConnector(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
        db_connector.connect()
        db_connector.create_constraints()

        # --- FASE 2: Ingestione dati statici (Campioni, Oggetti, etc.) ---
        logger.info("Inizio ingestione dati statici...")
        downloader = DataDragonDownloader()
        champion_data = downloader.get_data("championFull", force_download=False)
        if champion_data:
            ingest_champions_to_neo4j(db_connector, champion_data)
        logger.info("Ingestione dati statici completata.")
        
        item_data = downloader.get_data("item", force_download=False)
        if item_data:
            ingest_items_to_neo4j(db_connector, item_data)
        logger.info("Ingestione dati degli oggetti completata.")
        
        rune_data = downloader.get_data("runesReforged", force_download=False)
        if rune_data:
            ingest_runes_to_neo4j(db_connector, rune_data)
        logger.info("Ingestione dati delle rune completata.")
        
        summoner_spell_data = downloader.get_data("summoner", force_download=False)
        if summoner_spell_data:
            ingest_summoner_spells_to_neo4j(db_connector, summoner_spell_data)
        logger.info("Ingestione dati degli incantesimi del evocatore completata.")
        
        # --- FASE 3: Ingestione della lore dal file JSON pre-elaborato ---
        logger.info("Inizio ingestione della lore pre-elaborata da file JSON.")
        lore_json_path = "lore_data.json" # Assicurati che il file sia nella stessa cartella o specifica il path
        
        if os.path.exists(lore_json_path):
            with open(lore_json_path, 'r', encoding='utf-8') as f:
                all_lore_data = json.load(f)
            
            # L'ingestore ora ha bisogno solo della connessione al DB
            lore_ingestor = LoreIngestor(db_connector) 
            lore_ingestor.ingest_from_list(all_lore_data) # Nuovo metodo da creare!
            logger.info("Ingestione della lore completata.")
        else:
            logger.warning(f"File '{lore_json_path}' non trovato. Salto l'ingestione della lore.")

        # --- FASE 4: Ingestione dati delle partite da file CSV ---
        logger.info("Avvio ingestione partite da file JSON.")
        logger.info("Avvio del processo di ingestione partite dal flusso Kafka.")
        ingestor = MatchDataIngestor(neo4j_connector=db_connector, data_dragon_downloader=downloader)
        ingestor.ingest_matches_from_kafka('lol-matches')
        #ingestor.ingest_matches_from_kafka_single('lol-matches') # Metodo di test più lento

    except Exception as e:
        logger.critical(f"Si è verificato un errore critico durante l'esecuzione: {e}", exc_info=True)
    finally:
        if db_connector:
            logger.info("Esecuzione query di validazione finale...")
            db_connector.close()
            logger.info("Connessione a Neo4j chiusa.")


if __name__ == "__main__":
    main()
