# neo4j_handler/connector.py

from neo4j import GraphDatabase, exceptions
import logging

logger = logging.getLogger(__name__)

class Neo4jConnector:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

    def connect(self):
        """Stabilisce la connessione al database Neo4j."""
        if self._driver is None:
            try:
                self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
                self._driver.verify_connectivity()
                logger.info("Connessione a Neo4j stabilita con successo.")
            except exceptions.ServiceUnavailable:
                logger.critical(f"Impossibile connettersi al database Neo4j all'URI: {self._uri}. Assicurati che Neo4j sia in esecuzione.")
                self._driver = None # Assicurati che il driver sia None in caso di fallimento
                raise # Rilancia l'eccezione per fermare l'esecuzione principale
            except exceptions.AuthError:
                logger.critical("Credenziali Neo4j non valide. Controlla username e password.")
                self._driver = None
                raise
            except Exception as e:
                logger.critical(f"Errore inaspettato durante la connessione a Neo4j: {e}", exc_info=True)
                self._driver = None
                raise
        else:
            logger.info("Il driver Neo4j è già connesso.")

    def close(self):
        """Chiude la connessione al database Neo4j."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info("Connessione a Neo4j chiusa.")

    def get_driver(self):
        """Restituisce l'istanza del driver Neo4j."""
        return self._driver

    def is_connected(self):
        """Verifica se il driver è attivo e la connessione è valida."""
        if self._driver is None:
            return False
        try:
            # Tenta una semplice operazione per verificare la connettività
            self._driver.verify_connectivity()
            return True
        except Exception:
            return False

    def run_query(self, query, parameters=None, single_record=False):
        """Esegue una query Cypher con parametri opzionali."""
        if not self.is_connected():
            logger.error("Non connesso a Neo4j. Impossibile eseguire la query.")
            return None

        try:
            with self._driver.session() as session:
                result = session.run(query, parameters)
                if single_record:
                    return result.single() # Restituisce un solo record
                return [record for record in result]
        except exceptions.ClientError as e:
            logger.error(f"Errore Cypher: {e.code} - {e.message}")
            raise # Rilancia l'errore per gestione superiore
        except Exception as e:
            logger.error(f"Errore durante l'esecuzione della query Cypher: {e}", exc_info=True)
            return None

    def create_constraints(self):
        """Crea gli indici e i vincoli di unicità necessari."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Champion) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Item) REQUIRE i.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (rp:RunePath) REQUIRE rp.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Rune) REQUIRE r.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (ss:SummonerSpell) REQUIRE ss.id IS UNIQUE",
            "CREATE CONSTRAINT character_name_unique IF NOT EXISTS FOR (c:Character) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT champion_key_unique IF NOT EXISTS FOR (c:Champion) REQUIRE c.key IS UNIQUE"
        ]
        for query in constraints:
            try:
                self.run_query(query)
                logger.info(f"Vincolo creato: {query}")
            except exceptions.ClientError as e:
                # Se il vincolo esiste già, Neo4j potrebbe sollevare un'eccezione, ma IF NOT EXISTS la gestisce.
                # Questa è più per errori di sintassi o permessi.
                logger.error(f"Errore nella creazione del vincolo '{query}': {e.code} - {e.message}")
            except Exception as e:
                logger.error(f"Errore generico nella creazione del vincolo '{query}': {e}", exc_info=True)

    def clear_database(self):
        """Elimina tutti i nodi e le relazioni dal database. USA CON CAUTELA!"""
        try:
            # Query ottimizzata per grafi grandi
            self.run_query("MATCH (n) DETACH DELETE n")
            logger.warning("Tutti i dati nel database Neo4j sono stati eliminati.")
            return True
        except Exception as e:
            logger.error(f"Errore durante la pulizia del database: {e}", exc_info=True)
            return False

    def count_nodes(self, label):
        """Conta il numero di nodi con una data etichetta."""
        query = f"MATCH (n:{label}) RETURN count(n) AS count"
        result = self.run_query(query, single_record=True)
        return result['count'] if result else 0