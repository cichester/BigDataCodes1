# config/settings.py
import os
import logging
from dotenv import load_dotenv

# Configurazione del logging per l'intero progetto
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Logger specifico per questo modulo

# Carica le variabili d'ambiente dal file .env (assicurati che .env sia nella root del progetto)
load_dotenv()

# --- Configurazione API Riot Games ---
RIOT_API_KEY = os.getenv("RIOT_API_KEY")
RIOT_REGION = os.getenv("RIOT_REGION", "euw1") # Default a euw1 se non specificato

# Controllo per la chiave API
if not RIOT_API_KEY:
    logger.error("ERRORE CRITICO: RIOT_API_KEY non trovata. Assicurati che sia impostata come variabile d'ambiente o nel tuo file .env")
    # In un'applicazione reale potresti voler uscire qui o sollevare un'eccezione

# --- Configurazione Neo4j ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD") # Non impostare un default per la password

# Controllo per la password di Neo4j
if not NEO4J_PASSWORD:
    logger.error("ERRORE CRITICO: NEO4J_PASSWORD non trovata. Assicurati che sia impostata nel tuo file .env")

# --- Altre configurazioni (es. per Data Ingestion) ---
MATCH_DATA_BASE_DIR = "G:\\Matches"
# Puoi aggiungere qui altre configurazioni future, come limiti di partite da scaricare, ecc.