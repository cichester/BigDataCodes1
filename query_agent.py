# query_agent.py (versione stabile)

import logging
from dotenv import load_dotenv

# Importiamo tutto da langchain_community e langchain standard
from langchain_community.graphs import Neo4jGraph
from langchain.chains import GraphCypherQAChain
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. Inizializzazione ---
try:
    # Usiamo la versione di Neo4jGraph da langchain_community, che è compatibile
    # con GraphCypherQAChain. Ignoreremo il suo avviso di deprecazione.
    graph = Neo4jGraph()
    logger.info("Connessione a Neo4j e recupero dello schema completati.")
    logger.info(f"Schema del grafo rilevato:\n{graph.schema}\n")

    llm = ChatGoogleGenerativeAI(model="gemini-1.5-pro", temperature=0)
    logger.info("Modello Gemini (gemini-pro) inizializzato.")

except Exception as e:
    logger.critical(f"Errore durante l'inizializzazione: {e}")
    exit()

# --- 2. Creazione della Catena ---
# Aggiungiamo il parametro per confermare che siamo consapevoli dei rischi
chain = GraphCypherQAChain.from_llm(
    graph=graph,
    llm=llm,
    verbose=True,

    allow_dangerous_requests=True
)

# --- 3. Ciclo di Interrogazione ---
def start_query_loop():
    logger.info("Agente di interrogazione pronto. Digita 'esci' per terminare.")
    while True:
        try:
            question = input("\n👤 La tua domanda: ")
            if question.lower() in ['esci', 'exit', 'quit']:
                break
            
            response = chain.invoke({"query": question})
            
            print("\n🤖 Risposta:")
            final_answer = response.get("result", "Non ho trovato una risposta.")
            print(final_answer)

        except Exception as e:
            logger.error(f"Si è verificato un errore durante l'interrogazione: {e}")

if __name__ == "__main__":
    start_query_loop()
