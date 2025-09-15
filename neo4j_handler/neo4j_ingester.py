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