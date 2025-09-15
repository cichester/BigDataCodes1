# data_ingestion/extract_lore_data.py

import logging
import os
import json
import time
from dotenv import load_dotenv

load_dotenv()

from llm_handler.llm_client import LLMClient
from config.settings import LORE_FILES_PATH, NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
from neo4j_handler.connector import Neo4jConnector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_lore_from_file(champion_filename: str) -> str:
    lore_file_path = os.path.join(LORE_FILES_PATH, champion_filename)
    # ... (il resto della funzione non cambia)
    try:
        with open(lore_file_path, 'r', encoding='utf-8') as f: return f.read()
    except Exception: return None

def get_champion_records(db_connector: Neo4jConnector) -> list:
    """Recupera una lista di tutti i campioni con i loro id e name."""
    try:
        return db_connector.run_query("MATCH (c:Champion) RETURN c.id AS id, c.name AS name")
    except Exception as e:
        logger.error(f"Impossibile recuperare i record dei campioni: {e}")
        return []

def main():
    logger.info("Avvio dello script di estrazione della lore...")
    
    db_connector = Neo4jConnector(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
    db_connector.connect()
    champion_records = get_champion_records(db_connector)
    db_connector.close()

    if not champion_records:
        logger.critical("Nessun dato campione trovato nel DB. Interruzione.")
        return

    llm_client = LLMClient()
    all_lore_data = []
    
    try:
        lore_files = [f for f in os.listdir(LORE_FILES_PATH) if f.endswith('.txt')]
        logger.info(f"Trovati {len(lore_files)} file di lore da processare.")
        
        for filename in lore_files:
            base_name_from_file = filename.replace(".txt", "")
            
            # --- NUOVA LOGICA DI RICERCA FLESSIBILE ---
            matched_champion_record = None
            for record in champion_records:
                # Controlla se il nome del file corrisponde all'ID o al NOME del campione
                if base_name_from_file.lower() == record['id'].lower() or base_name_from_file.lower() == record['name'].lower():
                    matched_champion_record = record
                    break # Trovato, esci dal ciclo interno
            
            if not matched_champion_record:
                logger.warning(f"Il file '{filename}' non corrisponde a nessun campione nel DB. Salto.")
                continue

            canonical_name = matched_champion_record['name']
            logger.info(f"Processando: file '{filename}' -> campione '{canonical_name}'")
            
            lore_text = read_lore_from_file(filename)
            if lore_text:
                extracted_data = llm_client.extract_lore_entities(lore_text)
                if extracted_data:
                    extracted_data['champion_name'] = canonical_name
                    all_lore_data.append(extracted_data)
                else:
                    logger.warning(f"L'LLM non ha restituito dati per {canonical_name}.")
            time.sleep(1)

    except Exception as e:
        logger.critical(f"Si è verificato un errore imprevisto: {e}", exc_info=True)

    output_filename = "lore_data.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(all_lore_data, f, ensure_ascii=False, indent=4)

    logger.info(f"Estrazione completata. Dati salvati in '{output_filename}'.")

if __name__ == "__main__":
    main()