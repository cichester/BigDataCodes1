from neo4j import GraphDatabase, Driver
from utils.data_dragon_downloader import get_ddragon_data
from neo4j_handler.neo4j_ingester import ingest_champions_to_neo4j, ingest_items_to_neo4j, ingest_runes_to_neo4j,ingest_summoner_spells_to_neo4j

# --- Configurazione Neo4j ---
URI = "neo4j://127.0.0.1:7687"
USERNAME = "neo4j"
PASSWORD = "Cichester0706!" # <<< CAMBIA QUI!
                            # Se è la prima volta che usi Neo4j Desktop, ti verrà chiesto di impostarla.

def connect_to_neo4j() -> Driver | None:
    """Tenta di connettersi al database Neo4j."""
    driver = None
    try:
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        driver.verify_connectivity()
        print("Connessione a Neo4j riuscita!")
        return driver
    except Exception as e:
        print(f"Errore di connessione a Neo4j: {e}")
        print("Assicurati che il database Neo4j sia in esecuzione e che le credenziali siano corrette.")
        return None

def main():
    neo4j_driver = connect_to_neo4j()
    print("Pulizia DB")
    neo4j_driver.session().run("MATCH (n) DETACH DELETE n")
    if not neo4j_driver:
        print("Impossibile procedere senza una connessione a Neo4j.")
        return

    try:
        # 1. Scarica i dati dei campioni e ingeriscili
        champion_data = get_ddragon_data("champion")
        if champion_data:
            ingest_champions_to_neo4j(neo4j_driver, champion_data)

        # 2. Scarica i dati degli item e ingeriscili
        item_data = get_ddragon_data("item")
        if item_data:
            ingest_items_to_neo4j(neo4j_driver, item_data)

        # 3. Aggiungi qui le chiamate per Rune, SummonerSpells, ecc.
        rune_data = get_ddragon_data("runesReforged")
        if rune_data:
             ingest_runes_to_neo4j(neo4j_driver, rune_data)

        spell_data = get_ddragon_data("summoner")
        if spell_data:
            ingest_summoner_spells_to_neo4j(neo4j_driver, spell_data)

        # Query di verifica finale
        with neo4j_driver.session() as session:
            num_champions = session.run("MATCH (c:Champion) RETURN count(c) AS count").single()['count']
            num_items = session.run("MATCH (i:Item) RETURN count(i) AS count").single()['count']
            num_runes = session.run("MATCH (r:Rune) RETURN count(r) AS count").single()['count']
            num_spells = session.run("MATCH (s:SummonerSpell) RETURN count(s) AS count").single()['count']
            print(f"\nRisultati finali nel grafo:")
            print(f"  Numero totale di Campioni: {num_champions}")
            print(f"  Numero totale di Item: {num_items}")
            print(f"  Numero totale di Rune: {num_runes}")
            print(f" Numero totale di Summeners Spells: {num_spells}")

    except Exception as e:
        print(f"Si è verificato un errore durante l'ingestione principale: {e}")
    finally:
        if neo4j_driver:
            neo4j_driver.close()
            print("Connessione a Neo4j chiusa.")

if __name__ == "__main__":
    main()