# llm_handler/llm_client.py (versione aggiornata con LangChain)

import logging
import json
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI

# Assicurati che le variabili d'ambiente siano caricate
load_dotenv()

logger = logging.getLogger(__name__)

class LLMClient:
    def __init__(self):
        """
        Inizializza il client usando lo stesso modello ChatGoogleGenerativeAI
        utilizzato nel resto del progetto.
        """
        try:
            # Il modello legge la GOOGLE_API_KEY automaticamente dalle variabili d'ambiente
            self.model = ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0)
            logger.info("Modello Gemini (via LangChain) pronto per l'uso.")
        except Exception as e:
            logger.error(f"Errore durante l'inizializzazione del modello Gemini via LangChain: {e}")
            raise

    def extract_lore_entities(self, text_content: str) -> dict:
        """
        Estrae le entità e le relazioni dalla lore usando una catena LangChain
        per garantire un output JSON strutturato.
        """
        # 1. Definiamo il parser per l'output: vogliamo un JSON.
        parser = JsonOutputParser()

        # 2. Definiamo il template del prompt
        prompt_template = """
        Estrai le seguenti informazioni dal testo fornito e restituisci un oggetto JSON.
        Il formato JSON deve avere queste chiavi: 'factions', 'allies', 'rivals', 'summary'.
        
        Testo da analizzare:
        {text}
        
        {format_instructions}
        """

        prompt = ChatPromptTemplate.from_template(
            template=prompt_template,
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )

        # 3. Creiamo la catena: prompt -> modello -> parser
        chain = prompt | self.model | parser

        try:
            # 4. Invochiamo la catena con il testo della lore
            response = chain.invoke({"text": text_content})
            return response
            
        except Exception as e:
            logger.error(f"Errore durante l'interazione con la catena LangChain: {e}", exc_info=True)
            return None