# main.py
import logging
import os # Importa il modulo os per la gestione dei percorsi
from config.settings import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD # RIOT_API_KEY, RIOT_REGION non più necessari qui per i match
from neo4j_handler.connector import Neo4jConnector
from utils.data_dragon_downloader import DataDragonDownloader
from neo4j_handler.neo4j_ingester import (
    ingest_champions_to_neo4j,
    ingest_items_to_neo4j,
    ingest_runes_to_neo4j,
    ingest_summoner_spells_to_neo4j
)
# from riot_api.client import RiotAPIClient # Non più necessario per l'ingestione dei match
from data_ingestion.match_data_ingestor import MatchDataIngestor

# Configurazione del logger (assicurati che sia configurato una sola volta nel progetto)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Nuovo percorso per il dataset Kaggle ---
# Assicurati che questa cartella contenga tutti i file JSON delle partite scaricati da Kaggle.
KAGGLE_DATASET_PATH = r'G:\BigDataCodes\kaggle_data'


def main():
    db_connector = None # Inizializza a None per il blocco finally
    try:
        logger.info("Avvio del progetto League of Legends Knowledge Graph.")

        # 1. Inizializzazione e connessione al connettore Neo4j
        db_connector = Neo4jConnector(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
        db_connector.connect()
        db_connector.create_constraints()

        # Opzione per pulire il database all'inizio (USA CON CAUTELA!)
        # Decommenta solo se vuoi resettare il DB ogni volta che esegui main.
        # logger.info("Tentativo di pulizia del database Neo4j...")
        # if db_connector.clear_database(): # Assicurati che clear_database sia implementato in connector.py
        #    logger.info("Database Neo4j pulito con successo.")
        # else:
        #    logger.warning("Pulizia del database fallita o non necessaria.")


        # 2. Scarica e Ingerisci i dati statici (Data Dragon) 
        logger.info("Inizio ingestione dati statici (Data Dragon)...")

        # Inizializza il downloader. Ora recupererà automaticamente l'ultima versione.
        downloader = DataDragonDownloader()
        current_dd_version = downloader.version
        logger.info(f"Utilizzando Data Dragon versione: {current_dd_version}")

        # Ingestione Campioni (usando 'championFull' per i dettagli completi)
        logger.info("Inizio ingestione Campioni...")
        champion_data = downloader.get_data("championFull", force_download=False)
        if champion_data:
            ingest_champions_to_neo4j(db_connector, champion_data)
            logger.info("Ingestione campioni completata.")
        else:
            logger.warning("Nessun dato campione scaricato da Data Dragon.")

        # Ingestione Items
        logger.info("Inizio ingestione Items...")
        item_data = downloader.get_data("item", force_download=False)
        if item_data:
            ingest_items_to_neo4j(db_connector, item_data)
            logger.info("Ingestione item completata.")
        else:
            logger.warning("Nessun dato item scaricato da Data Dragon.")

        # Ingestione Rune
        logger.info("Inizio ingestione Rune...")
        rune_data = downloader.get_data("runesReforged", force_download=False)
        if rune_data:
            ingest_runes_to_neo4j(db_connector, rune_data)
            logger.info("Ingestione rune completata.")
        else:
            logger.warning("Nessun dato rune scaricato da Data Dragon.")

        # Ingestione Summoner Spells
        logger.info("Inizio ingestione Summoner Spells...")
        spell_data = downloader.get_data("summoner", force_download=False)
        if spell_data:
            ingest_summoner_spells_to_neo4j(db_connector, spell_data)
            logger.info("Ingestione summoner spells completata.")
        else:
            logger.warning("Nessun dato summoner spells scaricato da Data Dragon.")

        logger.info("Ingestione dati statici completata.")

        # --- Modifica: Inizializzazione e Avvio dell'ingestore per dati Kaggle ---
        logger.info("Avvio del processo di ingestione partite dal dataset Kaggle.")
        # Passa db_connector e downloader a MatchDataIngestor
        ingestor = MatchDataIngestor(neo4j_connector=db_connector, data_dragon_downloader=downloader)

        # Chiama il nuovo metodo per l'ingestione dal dataset Kaggle
        ingestor.ingest_matches_from_kafka('lol-matches')

        logger.info("Processo di ingestione completato.")


    except Exception as e:
        logger.critical(f"Si è verificato un errore critico durante l'esecuzione del main: {e}", exc_info=True)
    finally:
        # Questo blocco viene sempre eseguito, sia che ci siano errori che no.
        if db_connector and db_connector.is_connected():
            try:
                logger.info("\nRisultati finali nel grafo:")

                num_champions = db_connector.run_query("MATCH (c:Champion) RETURN count(c) AS count", single_record=True)
                if num_champions:
                    logger.info(f"- Campioni: {num_champions['count']}")
                else:
                    logger.info("- Campioni: N/A (errore nel recupero)")

                num_items = db_connector.run_query("MATCH (i:Item) RETURN count(i) AS count", single_record=True)
                if num_items:
                    logger.info(f"- Items (SR): {num_items['count']}")
                else:
                    logger.info("- Items (SR): N/A (errore nel recupero)")

                num_runes = db_connector.run_query("MATCH (r:Rune) RETURN count(r) AS count", single_record=True)
                num_rune_paths = db_connector.run_query("MATCH (rp:RunePath) RETURN count(rp) AS count", single_record=True)
                if num_runes and num_rune_paths:
                    logger.info(f"- Rune: {num_runes['count']} (Nodi Rune) + {num_rune_paths['count']} (Nodi RunePath)")
                else:
                    logger.info("- Rune: N/A (errore nel recupero)")

                num_summoner_spells = db_connector.run_query("MATCH (ss:SummonerSpell) RETURN count(ss) AS count", single_record=True)
                if num_summoner_spells:
                    logger.info(f"- Summoner Spells (CLASSIC): {num_summoner_spells['count']}")
                else:
                    logger.info("- Summoner Spells (CLASSIC): N/A (errore nel recupero)")

                # --- Aggiungi qui query per contare i nodi delle partite e dei giocatori se vuoi ---
                num_matches = db_connector.run_query("MATCH (m:Match) RETURN count(m) AS count", single_record=True)
                if num_matches:
                    logger.info(f"- Partite: {num_matches['count']}")
                else:
                    logger.info("- Partite: N/A (errore nel recupero)")

                num_players = db_connector.run_query("MATCH (p:Player) RETURN count(p) AS count", single_record=True)
                if num_players:
                    logger.info(f"- Giocatori: {num_players['count']}")
                else:
                    logger.info("- Giocatori: N/A (errore nel recupero)")

            except Exception as e:
                logger.error(f"Errore nel recupero dei conteggi finali dal grafo: {e}", exc_info=True)
            finally:
                db_connector.close()
                logger.info("Connessione a Neo4j chiusa.")


if __name__ == "__main__":
    main()