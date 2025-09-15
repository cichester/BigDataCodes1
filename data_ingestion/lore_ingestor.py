# data_ingestion/lore_ingestor.py
import logging
from neo4j_handler.connector import Neo4jConnector

logger = logging.getLogger(__name__)

class LoreIngestor:
    def __init__(self, neo4j_connector: Neo4jConnector):
        self.db_connector = neo4j_connector
        self.canonical_champion_names = self._fetch_champion_names()
        logger.info(f"LoreIngestor inizializzato. Caricati {len(self.canonical_champion_names)} nomi di campioni canonici.")

    def _fetch_champion_names(self) -> set:
        """Recupera tutti i nomi dei campioni dal DB per la normalizzazione."""
        try:
            result = self.db_connector.run_query("MATCH (c:Champion) RETURN c.name AS name")
            return {record['name'] for record in result}
        except Exception as e:
            logger.error(f"Impossibile recuperare i nomi dei campioni: {e}")
            return set()

    def _normalize_and_resolve_entities(self, names_from_llm: list) -> list:
        """
        Pulisce, normalizza e confronta i nomi dall'LLM con la lista dei campioni conosciuti.
        """
        resolved_names = set()
        for name in names_from_llm:
            # --- INIZIO LOGICA DI PULIZIA AGGIUNTA ---
            
            # 1. Controlla che 'name' sia una stringa valida e non vuota
            if not name or not isinstance(name, str):
                continue

            # 2. Rimuovi spazi bianchi all'inizio e alla fine. Questa è la correzione chiave.
            clean_name = name.strip()

            # 3. Se dopo la pulizia la stringa è vuota, saltala
            if not clean_name:
                continue
            
            # --- FINE LOGICA DI PULIZIA ---


            # --- LOGICA DI RISOLUZIONE ESISTENTE (ora eseguita su 'clean_name') ---
            
            # Tentativo 1: Corrispondenza esatta
            if clean_name in self.canonical_champion_names:
                resolved_names.add(clean_name)
                continue
            
            # Tentativo 2: Corrispondenza "contenuto in" (es. "Garen" in "Garen, the Might of Demacia")
            found = False
            for canonical_name in self.canonical_champion_names:
                if canonical_name in clean_name:
                    resolved_names.add(canonical_name)
                    found = True
                    break
            
            # Se nessuna corrispondenza trovata, è un personaggio secondario
            if not found:
                resolved_names.add(clean_name)
        
        return list(resolved_names)

    def ingest_from_list(self, all_lore_data: list):
        """Cicla su una lista di dati di lore pre-elaborati e li ingerisce nel grafo."""
        logger.info(f"Inizio ingestione di dati di lore per {len(all_lore_data)} campioni.")
        for lore_item in all_lore_data:
            champion_name = lore_item.get('champion_name')
            if champion_name:
                self._insert_single_champion_lore(champion_name, lore_item)
        logger.info("Ingestione di tutta la lore completata.")

    def _insert_single_champion_lore(self, champion_name: str, data: dict):
        """Inserisce i dati di lore dopo averli normalizzati."""
        
        # --- FASE DI NORMALIZZAZIONE ---
        clean_allies = self._normalize_and_resolve_entities(data.get('allies', []))
        clean_rivals = self._normalize_and_resolve_entities(data.get('rivals', []))

        params = {
            'champion_name': champion_name,
            'summary': data.get('summary'),
            'factions': data.get('factions', []),
            'allies': clean_allies,
            'rivals': clean_rivals
        }

        # La query Cypher rimane identica, ma ora riceve dati puliti
        query = """
        MATCH (c:Champion {name: $champion_name})
        SET c.loreSummary = $summary, c:Character
        
        WITH c
        UNWIND $factions AS faction_name
        MERGE (f:Faction {name: faction_name})
        MERGE (c)-[:IS_FROM_FACTION]->(f)

        WITH c
        UNWIND $allies AS ally_name
        MERGE (ally:Character {name: ally_name})
        MERGE (c)-[:IS_ALLY_WITH]->(ally)

        WITH c
        UNWIND $rivals AS rival_name
        MERGE (rival:Character {name: rival_name})
        MERGE (c)-[:IS_RIVAL_OF]->(rival)
        """

        try:
            self.db_connector.run_query(query, params)
            logger.info(f"Dati di lore inseriti con successo per {champion_name}.")
        except Exception as e:
            logger.error(f"Errore nell'esecuzione della query per {champion_name}: {e}", exc_info=True)