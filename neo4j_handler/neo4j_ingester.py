# In neo4j_handler/neo4j_ingester.py
import logging

import re

import html # Importa il modulo html per la decodifica delle entità

# Configurazione del logger specifico per questo modulo

logger = logging.getLogger(__name__)

def clean_description(description: str) -> str:
    """
    Rimuove i tag HTML, i placeholder di Data Dragon e le entità HTML dalle stringhe di testo.
    """
    if not isinstance(description, str):
        # logger.warning(f"clean_description ha ricevuto un input non-stringa: {type(description)}. Convertito in stringa.")
        description = str(description)

    # 1. Decodifica entità HTML (es. &nbsp;, &quot;)
    cleaned_desc = html.unescape(description)

    # 2. Sostituisci <br> con un newline per mantenere la formattazione
    cleaned_desc = re.sub(r'<br\s*/?>', '\n', cleaned_desc)

    # 3. Rimuovi i placeholder di Data Dragon (es. {{ qdamage }})
    cleaned_desc = re.sub(r'\{\{[^}]*\}\}', '', cleaned_desc)

    # 4. Rimuovi tutti i tag HTML rimanenti
    cleaned_desc = re.sub(r'<[^>]*>', '', cleaned_desc)

    # 5. Pulisci spazi multipli e rimuovi spazi all'inizio/fine
    cleaned_desc = re.sub(r'\s+', ' ', cleaned_desc).strip()
    
    return cleaned_desc

def ingest_champions_to_neo4j(db_connector, champions_data):
    """
    Ingerisce i dati dei campioni, incluse abilità e passiva, in Neo4j
    con query ottimizzate e corrette.
    """
    if not champions_data:
        logger.warning("Nessun dato campione fornito per l'ingestione.")
        return

    logger.info("Inizio ingestione Campioni (con abilità e statistiche) in Neo4j...")

    champion_params, passive_params, ability_params = [], [], []
    champion_passive_rels, champion_ability_rels = [], []

    for champion_id, champ_details in champions_data.items():
        # Prepara i parametri per il nodo Champion (proprietà principali)
        props = {
            "key": champ_details.get("key"),
            "name": champ_details.get("name"),
            "title": champ_details.get("title"),
            "blurb": clean_description(champ_details.get("blurb")),
            "partype": champ_details.get("partype"),
            "tags": champ_details.get("tags", [])
        }
        # Aggiungi le statistiche al dizionario delle proprietà
        props.update(champ_details.get("stats", {}))
        champion_params.append({"id": champion_id, "props": props})

        # Processa la passiva
        passive = champ_details.get("passive")
        if passive:
            passive_id = f"{champion_id}_passive"
            passive_params.append({
                "id": passive_id,
                "name": passive.get("name"),
                "description": clean_description(passive.get("description")),
            })
            champion_passive_rels.append({"champion_id": champion_id, "passive_id": passive_id})

        # Processa le abilità
        for i, spell in enumerate(champ_details.get("spells", [])):
            ability_id = f"{champion_id}_{['Q', 'W', 'E', 'R'][i]}"
            ability_params.append({
                "id": ability_id,
                "name": spell.get("name"),
                "description": clean_description(spell.get("description")),
                "key_bind": ["Q", "W", "E", "R"][i]
            })
            champion_ability_rels.append({"champion_id": champion_id, "ability_id": ability_id})
    try:
        # --- Query Nodi ---
        # 1. Crea/aggiorna i nodi Champion
        champion_query = """
        UNWIND $params AS champ_data
        // LA CORREZIONE È QUI: usiamo champ_data.id invece di $champion_id
        MERGE (c:Champion:Character {id: champ_data.id})
        // L'operatore += è un modo più pulito per impostare/aggiornare tutte le proprietà
        SET c += champ_data.props
        """
        db_connector.run_query(champion_query, {"params": champion_params})

        # 2. Crea/aggiorna Passives e Abilities (le tue query erano già corrette)
        passive_query = """
        UNWIND $params AS passive_data
        MERGE (p:Passive {id: passive_data.id})
        SET p.name = passive_data.name, p.description = passive_data.description
        """
        db_connector.run_query(passive_query, {"params": passive_params})

        ability_query = """
        UNWIND $params AS ability_data
        MERGE (a:Ability {id: ability_data.id})
        SET a.name = ability_data.name, a.description = ability_data.description, a.keyBind = ability_data.key_bind
        """
        db_connector.run_query(ability_query, {"params": ability_params})

        # --- Query Relazioni ---
        # 3. Crea le relazioni HAS_PASSIVE
        passive_rel_query = """
        UNWIND $rels AS rel_data
        MATCH (c:Champion {id: rel_data.champion_id})
        MATCH (p:Passive {id: rel_data.passive_id})
        MERGE (c)-[:HAS_PASSIVE]->(p)
        """
        db_connector.run_query(passive_rel_query, {"rels": champion_passive_rels})

        # 4. Crea le relazioni HAS_ABILITY
        ability_rel_query = """
        UNWIND $rels AS rel_data
        MATCH (c:Champion {id: rel_data.champion_id})
        MATCH (a:Ability {id: rel_data.ability_id})
        MERGE (c)-[:HAS_ABILITY]->(a)
        """
        db_connector.run_query(ability_rel_query, {"rels": champion_ability_rels})

        logger.info(f"Ingestione Campioni completata con successo.")

    except Exception as e:
        logger.error(f"Errore durante l'ingestione dei Campioni: {e}", exc_info=True)
        
        
        
        
def ingest_items_to_neo4j(db_connector, items_data):
    """
    Ingerisce i dati degli item e le loro relazioni di costruzione (builds from) in Neo4j.
    """
    if not items_data:
        logger.warning("Nessun dato item fornito per l'ingestione.")
        return

    logger.info("Inizio ingestione Items e delle loro ricette in Neo4j...")
    
    item_params = []
    build_relationships = []

    # Fase 1: Prepara i dati in Python
    for item_id, item_details in items_data.items():
        # Filtra solo per gli item di Summoner's Rift
        if 'maps' in item_details and item_details['maps'].get('11', False):
            params = {
                "id": item_id,
                "name": item_details.get('name'),
                "description": clean_description(item_details.get('description', '')),
                "plaintext": item_details.get('plaintext'),
                "totalGold": item_details.get('gold', {}).get('total'),
                "baseGold": item_details.get('gold', {}).get('base'),
                "sellGold": item_details.get('gold', {}).get('sell'),
                "tags": item_details.get('tags', [])
            }
            # Aggiungi le statistiche direttamente come proprietà
            params.update(item_details.get('stats', {}))
            item_params.append(params)

            # Prepara le relazioni "BUILDS_FROM"
            if 'from' in item_details:
                for component_id in item_details['from']:
                    build_relationships.append({
                        "item_id": item_id,
                        "component_id": component_id
                    })

    try:
        # Fase 2: Ingestione in blocco dei nodi Item
        item_query = """
        UNWIND $params AS item_props
        MERGE (i:Item {id: item_props.id})
        // Usa l'operatore += per impostare tutte le proprietà in una volta
        SET i += item_props 
        """
        db_connector.run_query(item_query, {"params": item_params})
        logger.info(f"Ingestiti {len(item_params)} nodi Item.")

        # Fase 3: Ingestione in blocco delle relazioni BUILDS_FROM
        if build_relationships:
            rel_query = """
            UNWIND $rels AS rel_data
            MATCH (item:Item {id: rel_data.item_id})
            MATCH (component:Item {id: rel_data.component_id})
            MERGE (item)-[:BUILDS_FROM]->(component)
            """
            db_connector.run_query(rel_query, {"rels": build_relationships})
            logger.info(f"Create {len(build_relationships)} relazioni BUILDS_FROM.")
            
        logger.info("Ingestione Items completata con successo.")
        
    except Exception as e:
        logger.error(f"Errore durante l'ingestione degli Items: {e}", exc_info=True)

def ingest_runes_to_neo4j(db_connector, runes_data):
    """
    Ingerisce i dati delle rune e dei loro percorsi in Neo4j.
    """
    if not runes_data:
        logger.warning("Nessun dato runa fornito per l'ingestione.")
        return

    logger.info("Inizio ingestione Rune e Percorsi Rune in Neo4j...")
    
    rune_paths_params = []
    runes_params = []

    # Fase 1: Prepara i dati in Python
    # runes_data è una lista di percorsi
    for path_details in runes_data:
        path_id = path_details.get('id')
        
        # Prepara i parametri per i nodi RunePath
        rune_paths_params.append({
            "id": path_id,
            "key": path_details.get('key'),
            "name": path_details.get('name')
        })

        # Itera attraverso gli slot e le rune per questo percorso
        for slot in path_details.get('slots', []):
            for rune in slot.get('runes', []):
                runes_params.append({
                    "id": rune.get('id'),
                    "key": rune.get('key'),
                    "name": rune.get('name'),
                    "shortDesc": clean_description(rune.get('shortDesc', '')),
                    "longDesc": clean_description(rune.get('longDesc', '')),
                    "path_id": path_id  # Salva l'ID del percorso per creare la relazione
                })

    try:
        # Fase 2: Ingestione in blocco dei nodi RunePath
        path_query = """
        UNWIND $params AS path_data
        MERGE (rp:RunePath {id: path_data.id})
        SET rp.key = path_data.key,
            rp.name = path_data.name
        """
        db_connector.run_query(path_query, {"params": rune_paths_params})
        logger.info(f"Ingestiti {len(rune_paths_params)} nodi RunePath.")

        # Fase 3: Ingestione in blocco dei nodi Rune e delle loro relazioni
        rune_query = """
        UNWIND $params AS rune_data
        // Crea o trova il nodo Rune
        MERGE (r:Rune {id: rune_data.id})
        SET r.key = rune_data.key,
            r.name = rune_data.name,
            r.shortDescription = rune_data.shortDesc,
            r.longDescription = rune_data.longDesc
        
        // Trova il percorso a cui appartiene e crea la relazione
        WITH r, rune_data
        MATCH (rp:RunePath {id: rune_data.path_id})
        MERGE (r)-[:BELONGS_TO_PATH]->(rp)
        """
        db_connector.run_query(rune_query, {"params": runes_params})
        logger.info(f"Ingestiti {len(runes_params)} nodi Rune e create le relative relazioni.")
        
        logger.info("Ingestione Rune completata con successo.")
        
    except Exception as e:
        logger.error(f"Errore durante l'ingestione delle Rune: {e}", exc_info=True)

def ingest_summoner_spells_to_neo4j(db_connector, summoner_spells_data):
    """
    Ingerisce i dati delle Summoner Spell in Neo4j.
    """
    if not summoner_spells_data:
        logger.warning("Nessun dato summoner spell fornito per l'ingestione.")
        return

    logger.info("Inizio ingestione Summoner Spells in Neo4j...")
    
    spell_params = []

    # Fase 1: Prepara i dati in Python
    for spell_key, spell_details in summoner_spells_data.items():
        # Filtra solo per gli incantesimi disponibili in modalità CLASSIC
        if 'modes' in spell_details and 'CLASSIC' in spell_details['modes']:
            spell_params.append({
                "id": spell_details.get('id'),      # ID testuale (es. "SummonerBarrier")
                "key": spell_details.get('key'),    # ID numerico (es. "21")
                "name": spell_details.get('name'),
                "description": clean_description(spell_details.get('description', '')),
                "cooldown": spell_details.get('cooldownBurn'),
                "summonerLevel": spell_details.get('summonerLevel')
            })

    try:
        # Fase 2: Ingestione in blocco dei nodi SummonerSpell
        query = """
        UNWIND $params AS spell_props
        // Usiamo la 'key' numerica come identificatore primario per le relazioni
        MERGE (ss:SummonerSpell {key: spell_props.key})
        SET ss.id = spell_props.id,
            ss.name = spell_props.name,
            ss.description = spell_props.description,
            ss.cooldown = spell_props.cooldown,
            ss.summonerLevel = spell_props.summonerLevel
        """
        db_connector.run_query(query, {"params": spell_params})
        logger.info(f"Ingestiti {len(spell_params)} nodi SummonerSpell.")
        
    except Exception as e:
        logger.error(f"Errore durante l'ingestione delle Summoner Spells: {e}", exc_info=True)
