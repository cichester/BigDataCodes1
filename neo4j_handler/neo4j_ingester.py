# neo4j_handler/neo4j_ingester.py
import logging
import re
import html # Importa il modulo html per la decodifica delle entità

# Configurazione del logger specifico per questo modulo
logger = logging.getLogger(__name__)

# Lista COMPLETA di tutte le possibili statistiche degli item (non usata in questa modifica)
ALL_POSSIBLE_ITEM_STATS = [
    "FlatHPPoolMod", "rFlatHPModPerLevel", "FlatMPPoolMod", "rFlatMPModPerLevel",
    "PercentHPPoolMod", "PercentMPPoolMod", "FlatHPRegenMod", "rFlatHPRegenModPerLevel",
    "PercentHPRegenMod", "FlatMPRegenMod", "rFlatMPRegenModPerLevel", "PercentMPRegenMod",
    "FlatArmorMod", "rFlatArmorModPerLevel", "PercentArmorMod", "rFlatArmorPenetrationMod",
    "rFlatArmorPenetrationModPerLevel", "rPercentArmorPenetrationMod", "rPercentArmorPenetrationModPerLevel",
    "FlatPhysicalDamageMod", "rFlatPhysicalDamageModPerLevel", "PercentPhysicalDamageMod",
    "FlatMagicDamageMod", "rFlatMagicDamageModPerLevel", "PercentMagicDamageMod",
    "FlatMovementSpeedMod", "rFlatMovementSpeedModPerLevel", "PercentMovementSpeedMod",
    "rPercentMovementSpeedModPerLevel", "FlatAttackSpeedMod", "PercentAttackSpeedMod",
    "rPercentAttackSpeedModPerLevel", "rFlatDodgeMod", "rFlatDodgeModPerLevel",
    "PercentDodgeMod", "FlatCritChanceMod", "rFlatCritChanceModPerLevel",
    "PercentCritChanceMod", "FlatCritDamageMod", "rFlatCritDamageModPerLevel",
    "PercentCritDamageMod", "FlatBlockMod", "PercentBlockMod", "FlatSpellBlockMod",
    "rFlatSpellBlockModPerLevel", "PercentSpellBlockMod", "FlatEXPBonus",
    "PercentEXPBonus", "rPercentCooldownMod", "rPercentCooldownModPerLevel",
    "rFlatTimeDeadMod", "rFlatTimeDeadModPerLevel", "rPercentTimeDeadMod",
    "rPercentTimeDeadModPerLevel", "rFlatGoldPer10Mod", "rFlatMagicPenetrationMod",
    "rFlatMagicPenetrationModPerLevel", "rPercentMagicPenetrationMod", "rPercentMagicPenetrationModPerLevel",
    "FlatEnergyRegenMod", "rFlatEnergyRegenModPerLevel", "FlatEnergyPoolMod",
    "rFlatEnergyModPerLevel", "PercentLifeStealMod", "PercentSpellVampMod"
]

# Mappatura degli ID delle mappe ai nomi (non usata in questa modifica)
MAP_ID_TO_NAME = {
    "1": "The Twisted Treeline (Obsolete)",
    "8": "The Crystal Scar (Dominion - Obsolete)",
    "10": "The Twisted Treeline (Obsolete)", # Old ID for Treeline
    "11": "Summoner's Rift",
    "12": "Howling Abyss (ARAM)",
    "21": "Nexus Blitz",
    "22": "Teamfight Tactics", # Note: TFT is a separate game mode, but Data Dragon includes items here
    "30": "Arena", # The new 2v2v2v2 mode
    "35": "Arena (Old)" # Older Arena ID, might be a DDragon artifact
}

def clean_description(description: str) -> str:
    """
    Rimuove i tag HTML, i placeholder di Data Dragon e le entità HTML dalle stringhe di testo.
    """
    if not isinstance(description, str):
        logger.warning(f"clean_description received non-string input: {type(description)}. Converting to string.")
        description = str(description)

    # 1. Decodifica entità HTML (es. &nbsp;, &quot;, &#x27;)
    description = html.unescape(description)

    # 2. Sostituisci <br> con un newline per mantenere la formattazione di base
    cleaned_desc = description.replace('<br>', '\n')
    cleaned_desc = cleaned_desc.replace('<br/>', '\n')
    cleaned_desc = cleaned_desc.replace('<br />', '\n')

    # 3. Rimuovi i placeholder doppi graffe (es. {{ qdamage }}, {{ spellmodifierdescriptionappend }})
    cleaned_desc = re.sub(r'\{\{[^}]*\}\}', '', cleaned_desc)

    # 4. Rimuovi tag HTML specifici o simili
    cleaned_desc = re.sub(r'<lol-uikit-tooltipped-keyword[^>]*>|<\/lol-uikit-tooltipped-keyword>', '', cleaned_desc)
    cleaned_desc = re.sub(r'<font[^>]*>|</font>', '', cleaned_desc)
    cleaned_desc = re.sub(r'<[^>]*>', '', cleaned_desc) # Catch-all per altri tag

    # 5. Pulisci spazi multipli e trimma la stringa
    cleaned_desc = re.sub(r'\s+', ' ', cleaned_desc).strip()
    
    return cleaned_desc


def ingest_champions_to_neo4j(db_connector, champions_data):
    """
    Ingests champion data, including abilities and passive, into Neo4j.
    Args:
        db_connector: An instance of Neo4jConnector.
        champions_data (dict): A dictionary of champion data from Data Dragon (championFull.json 'data' field).
    """
    if not champions_data:
        logger.warning("Nessun dato campione fornito per l'ingestione.")
        return 0

    total_champions_processed = 0
    total_abilities_processed = 0
    total_passives_processed = 0
    logger.info("Inizio ingestione Campioni (con abilità e statistiche) in Neo4j...")

    champion_params = []
    passive_params = []
    ability_params = []
    
    # Per tenere traccia delle relazioni da creare dopo aver creato i nodi
    champion_passive_rels = []
    champion_ability_rels = []


    for champion_id, champ_details in champions_data.items():
        # Prepara i parametri per il nodo Champion
        # Estrarre solo le proprietà primitive per il nodo Champion
        champion_params.append({
            "id": champ_details.get("id"),
            "key": champ_details.get("key"), # key è l'ID numerico
            "name": champ_details.get("name"),
            "title": champ_details.get("title"),
            "image_full": champ_details.get("image", {}).get("full"),
            "blurb": clean_description(champ_details.get("blurb")), # Usa clean_description
            "info_attack": champ_details.get("info", {}).get("attack"),
            "info_defense": champ_details.get("info", {}).get("defense"),
            "info_magic": champ_details.get("info", {}).get("magic"),
            "info_difficulty": champ_details.get("info", {}).get("difficulty"),
            "tags": champ_details.get("tags", []), # Array di stringhe
            "partype": champ_details.get("partype"),
            # Estrai le statistiche e aggiungile direttamente come proprietà di primo livello
            "hp": champ_details.get("stats", {}).get("hp"),
            "hpperlevel": champ_details.get("stats", {}).get("hpperlevel"),
            "mp": champ_details.get("stats", {}).get("mp"),
            "mpperlevel": champ_details.get("stats", {}).get("mpperlevel"),
            "movespeed": champ_details.get("stats", {}).get("movespeed"),
            "armor": champ_details.get("stats", {}).get("armor"),
            "armorperlevel": champ_details.get("stats", {}).get("armorperlevel"),
            "spellblock": champ_details.get("stats", {}).get("spellblock"),
            "spellblockperlevel": champ_details.get("stats", {}).get("spellblockperlevel"),
            "attackrange": champ_details.get("stats", {}).get("attackrange"),
            "hpregen": champ_details.get("stats", {}).get("hpregen"),
            "hpregenperlevel": champ_details.get("stats", {}).get("hpregenperlevel"),
            "mpregen": champ_details.get("stats", {}).get("mpregen"),
            "mpregenperlevel": champ_details.get("stats", {}).get("mpregenperlevel"),
            "crit": champ_details.get("stats", {}).get("crit"),
            "critperlevel": champ_details.get("stats", {}).get("critperlevel"),
            "attackdamage": champ_details.get("stats", {}).get("attackdamage"),
            "attackdamageperlevel": champ_details.get("stats", {}).get("attackdamageperlevel"),
            "attackspeedperlevel": champ_details.get("stats", {}).get("attackspeedperlevel"),
            "attackspeed": champ_details.get("stats", {}).get("attackspeed"),
        })

        # Processa la passiva
        passive = champ_details.get("passive")
        if passive:
            passive_id = f"{champion_id}_passive" # ID unico per la passiva
            passive_params.append({
                "id": passive_id,
                "name": passive.get("name"),
                "description": clean_description(passive.get("description")), # Usa clean_description
                "image_full": passive.get("image", {}).get("full")
            })
            champion_passive_rels.append({"champion_id": champion_id, "passive_id": passive_id})

        # Processa le abilità (Q, W, E, R)
        for i, spell in enumerate(champ_details.get("spells", [])):
            # ID unico per l'abilità (es. Aatrox_Q, Aatrox_W)
            ability_id = f"{champion_id}_{spell.get('id')}" 
            ability_params.append({
                "id": ability_id,
                "name": spell.get("name"),
                "description": clean_description(spell.get("description")), # Pulisci la descrizione
                "tooltip": clean_description(spell.get("tooltip", "")), # Pulisci il tooltip
                "maxRank": spell.get("maxrank"),
                "cooldownBurn": spell.get("cooldownBurn"), # cooldwonBurn è già una stringa
                "costBurn": spell.get("costBurn"), # costBurn è già una stringa
                "costType": spell.get("costType"), # costType è una stringa
                "rangeBurn": spell.get("rangeBurn"), # rangeBurn è già una stringa
                "resource": spell.get("resource"), # resource è una stringa
                "key_bind": ["Q", "W", "E", "R"][i] # Associa la key bind
            })
            champion_ability_rels.append({"champion_id": champion_id, "ability_id": ability_id})

    try:
        # Query per creare/aggiornare i nodi Champion
        if champion_params:
            champion_query = """
            UNWIND $props AS champ_data
            MERGE (c:Champion {id: champ_data.id})
            ON CREATE SET 
                c.key = champ_data.key,
                c.name = champ_data.name, 
                c.title = champ_data.title, 
                c.imageFull = champ_data.image_full, 
                c.blurb = champ_data.blurb,
                c.infoAttack = champ_data.info_attack,
                c.infoDefense = champ_data.info_defense,
                c.infoMagic = champ_data.info_magic,
                c.infoDifficulty = champ_data.info_difficulty,
                c.tags = champ_data.tags,
                c.partype = champ_data.partype,
                c.hp = champ_data.hp,
                c.hpPerLevel = champ_data.hpperlevel,
                c.mp = champ_data.mp,
                c.mpPerLevel = champ_data.mpperlevel,
                c.moveSpeed = champ_data.movespeed,
                c.armor = champ_data.armor,
                c.armorPerLevel = champ_data.armorperlevel,
                c.spellBlock = champ_data.spellblock,
                c.spellBlockPerLevel = champ_data.spellblockperlevel,
                c.attackRange = champ_data.attackrange,
                c.hpRegen = champ_data.hpregen,
                c.hpRegenPerLevel = champ_data.hpregenperlevel,
                c.mpRegen = champ_data.mpregen,
                c.mpRegenPerLevel = champ_data.mpregenperlevel,
                c.crit = champ_data.crit,
                c.critPerLevel = champ_data.critperlevel,
                c.attackDamage = champ_data.attackdamage,
                c.attackDamagePerLevel = champ_data.attackdamageperlevel,
                c.attackSpeedPerLevel = champ_data.attackspeedperlevel,
                c.attackspeed = champ_data.attackspeed
            ON MATCH SET 
                c.key = champ_data.key,
                c.name = champ_data.name, 
                c.title = champ_data.title, 
                c.imageFull = champ_data.image_full, 
                c.blurb = champ_data.blurb,
                c.infoAttack = champ_data.info_attack,
                c.infoDefense = champ_data.info_defense,
                c.infoMagic = champ_data.info_magic,
                c.infoDifficulty = champ_data.info_difficulty,
                c.tags = champ_data.tags,
                c.partype = champ_data.partype,
                c.hp = champ_data.hp,
                c.hpPerLevel = champ_data.hpperlevel,
                c.mp = champ_data.mp,
                c.mpPerLevel = champ_data.mpperlevel,
                c.moveSpeed = champ_data.movespeed,
                c.armor = champ_data.armor,
                c.armorPerLevel = champ_data.armorperlevel,
                c.spellBlock = champ_data.spellblock,
                c.spellBlockPerLevel = champ_data.spellblockperlevel,
                c.attackRange = champ_data.attackrange,
                c.hpRegen = champ_data.hpregen,
                c.hpRegenPerLevel = champ_data.hpregenperlevel,
                c.mpRegen = champ_data.mpregen,
                c.mpRegenPerLevel = champ_data.mpregenperlevel,
                c.crit = champ_data.crit,
                c.critPerLevel = champ_data.critperlevel,
                c.attackDamage = champ_data.attackdamage,
                c.attackDamagePerLevel = champ_data.attackdamageperlevel,
                c.attackSpeedPerLevel = champ_data.attackspeedperlevel,
                c.attackspeed = champ_data.attackspeed
            RETURN count(c) AS count
            """
            result = db_connector.run_query(champion_query, {"props": champion_params}, single_record=True)
            if result:
                total_champions_processed = result['count']

        # Query per creare/aggiornare i nodi Passive
        if passive_params:
            passive_query = """
            UNWIND $props AS passive_data
            MERGE (p:Passive {id: passive_data.id})
            ON CREATE SET 
                p.name = passive_data.name, 
                p.description = passive_data.description, 
                p.imageFull = passive_data.image_full
            ON MATCH SET 
                p.name = passive_data.name, 
                p.description = passive_data.description, 
                p.imageFull = passive_data.image_full
            RETURN count(p) AS count
            """
            result = db_connector.run_query(passive_query, {"props": passive_params}, single_record=True)
            if result:
                total_passives_processed = result['count']

        # Query per creare/aggiornare i nodi Ability
        if ability_params:
            ability_query = """
            UNWIND $props AS ability_data
            MERGE (a:Ability {id: ability_data.id})
            ON CREATE SET 
                a.name = ability_data.name, 
                a.description = ability_data.description, 
                a.tooltip = ability_data.tooltip,
                a.maxRank = ability_data.maxRank,
                a.cooldownBurn = ability_data.cooldownBurn,
                a.costBurn = ability_data.costBurn,
                a.costType = ability_data.costType,
                a.rangeBurn = ability_data.rangeBurn,
                a.resource = ability_data.resource,
                a.keyBind = ability_data.key_bind
            ON MATCH SET 
                a.name = ability_data.name, 
                a.description = ability_data.description, 
                a.tooltip = ability_data.tooltip,
                a.maxRank = ability_data.maxRank,
                a.cooldownBurn = ability_data.cooldownBurn,
                a.costBurn = ability_data.costBurn,
                a.costType = ability_data.costType,
                a.rangeBurn = ability_data.rangeBurn,
                a.resource = ability_data.resource,
                a.keyBind = ability_data.key_bind
            RETURN count(a) AS count
            """
            result = db_connector.run_query(ability_query, {"props": ability_params}, single_record=True)
            if result:
                total_abilities_processed = result['count']
        
        # Creazione relazioni HAS_PASSIVE
        if champion_passive_rels:
            passive_rel_query = """
            UNWIND $props AS rel_data
            MATCH (c:Champion {id: rel_data.champion_id})
            MATCH (p:Passive {id: rel_data.passive_id})
            MERGE (c)-[:HAS_PASSIVE]->(p)
            RETURN count(c) AS count
            """
            db_connector.run_query(passive_rel_query, {"props": champion_passive_rels})

        # Creazione relazioni HAS_ABILITY
        if champion_ability_rels:
            ability_rel_query = """
            UNWIND $props AS rel_data
            MATCH (c:Champion {id: rel_data.champion_id})
            MATCH (a:Ability {id: rel_data.ability_id})
            MERGE (c)-[:HAS_ABILITY]->(a)
            RETURN count(c) AS count
            """
            db_connector.run_query(ability_rel_query, {"props": champion_ability_rels})


        logger.info(f"Ingestione Campioni completata. Nodi Champion: {total_champions_processed}, Passive: {total_passives_processed}, Abilità: {total_abilities_processed}.")
        return total_champions_processed + total_abilities_processed + total_passives_processed

    except Exception as e:
        logger.error(f"Errore durante l'ingestione dei Campioni: {e}", exc_info=True)
        return 0


def ingest_items_to_neo4j(db_connector, items_data):
    """
    Ingests item data into Neo4j.
    Args:
        db_connector: An instance of Neo4jConnector.
        items_data (dict): A dictionary where keys are item IDs and values are
                            their Data Dragon JSON objects.
    """
    if not items_data:
        logger.warning("Nessun dato item fornito per l'ingestione.")
        return 0

    item_count = 0
    logger.info("Inizio ingestione Items (solo Summoner's Rift) in Neo4j...")

    all_params = []
    for item_id, item_details in items_data.items(): # item_id è la chiave, che è l'ID
        # Filtering for Summoner's Rift (map 11) items
        if 'maps' in item_details and '11' in item_details['maps'] and item_details['maps']['11']:
            params = {
                "id": item_id,
                "name": item_details.get('name'),
                "description": clean_description(item_details.get('description', '')), # Clean description
                "plaintext": item_details.get('plaintext'),
                "gold_total": item_details.get('gold', {}).get('total'),
                "gold_base": item_details.get('gold', {}).get('base'),
                "gold_sell": item_details.get('gold', {}).get('sell'),
                "tags": item_details.get('tags'),
                "image_full": item_details.get('image', {}).get('full')
            }
            # Aggiungi le statistiche dell'item dinamicamente
            # Si assicura che solo le chiavi presenti in ALL_POSSIBLE_ITEM_STATS vengano considerate
            # e che i valori siano numerici (o None se assenti)
            for stat_key in ALL_POSSIBLE_ITEM_STATS:
                # Usa .get() con un valore di default None per evitare KeyError se la statistica non esiste
                params[stat_key] = item_details.get('stats', {}).get(stat_key)
            
            all_params.append(params)
        else:
            logger.debug(f"Item {item_id} non disponibile su Summoner's Rift o manca dati mappa. Salto.")

    if not all_params:
        logger.info("Nessun item valido per Summoner's Rift trovato per l'ingestione.")
        return 0

    try:
        # Costruisci dinamicamente la parte SET della query per includere tutte le statistiche
        # È cruciale che i nomi delle proprietà siano validi in Cypher (no caratteri speciali o spazi)
        # e che i valori siano tipi primitivi.
        set_clauses_create = []
        set_clauses_match = []
        
        # Proprietà fisse
        fixed_props = [
            "name", "description", "plaintext", "gold_total", "gold_base", 
            "gold_sell", "tags", "image_full"
        ]
        for prop in fixed_props:
            set_clauses_create.append(f"i.{prop} = p.{prop}")
            set_clauses_match.append(f"i.{prop} = p.{prop}")
        
        # Proprietà dinamiche (statistiche)
        for stat_key in ALL_POSSIBLE_ITEM_STATS:
            # Assicurati che il nome della proprietà Cypher sia valido (es. rimuovi caratteri speciali se ce ne fossero)
            cypher_stat_key = stat_key # Per questo elenco, i nomi sono già validi
            set_clauses_create.append(f"i.{cypher_stat_key} = p.{stat_key}")
            set_clauses_match.append(f"i.{cypher_stat_key} = p.{stat_key}")

        batch_query = f"""
        UNWIND $props AS p
        MERGE (i:Item {{id: p.id}})
        ON CREATE SET
            {', '.join(set_clauses_create)}
        ON MATCH SET
            {', '.join(set_clauses_match)}
        RETURN i.id
        """
        
        results = db_connector.run_query(batch_query, {"props": all_params})
        item_count = len(results) if results else 0
        logger.info(f"Ingestione Items completata. Nodi creati/aggiornati: {item_count}.")
    except Exception as e:
        logger.error(f"Errore durante l'ingestione degli Items: {e}", exc_info=True)

    return item_count


def ingest_runes_to_neo4j(db_connector, runes_data):
    """
    Ingests rune data (runesReforged) into Neo4j.
    Args:
        db_connector: An instance of Neo4jConnector.
        runes_data (list): A list of rune path JSON objects from Data Dragon.
    """
    if not runes_data:
        logger.warning("Nessun dato runa fornito per l'ingestione.")
        return 0

    total_nodes_processed = 0
    logger.info("Inizio ingestione Rune in Neo4j...")

    rune_paths_params = []
    runes_params = []

    # 'runes_data' is expected to be a list of rune paths
    for path in runes_data: # Each path is a dictionary
        path_id = path.get('id')
        path_name = path.get('name')
        path_icon = path.get('icon')
        
        # Prepara i parametri per i nodi RunePath
        rune_paths_params.append({
            "id": path_id,
            "name": path_name,
            "icon": path_icon
        })

        # Processa gli slot e le singole rune
        for slot in path.get('slots', []):
            for rune in slot.get('runes', []):
                # Prepara i parametri per i nodi Rune
                runes_params.append({
                    "id": rune.get('id'),
                    "name": rune.get('name'),
                    "icon": rune.get('icon'),
                    "shortDesc": clean_description(rune.get('shortDesc', '')),
                    "longDesc": clean_description(rune.get('longDesc', '')),
                    "path_id": path_id # Link alla sua path
                })

    try:
        # PRIMA QUERY: Inserisci o aggiorna i RunePath
        if rune_paths_params:
            path_query = """
            UNWIND $props AS path_data
            MERGE (rp:RunePath {id: path_data.id})
            ON CREATE SET rp.name = path_data.name, rp.icon = path_data.icon
            ON MATCH SET rp.name = path_data.name, rp.icon = path_data.icon
            RETURN count(rp) AS count
            """
            result_paths = db_connector.run_query(path_query, {"props": rune_paths_params}, single_record=True)
            if result_paths:
                total_nodes_processed += result_paths['count']

        # SECONDA QUERY: Inserisci o aggiorna le Rune e crea le relazioni
        if runes_params:
            rune_query = """
            UNWIND $props AS rune_data
            MERGE (r:Rune {id: rune_data.id})
            ON CREATE SET 
                r.name = rune_data.name,
                r.icon = rune_data.icon,
                r.shortDescription = rune_data.shortDesc,
                r.longDescription = rune_data.longDesc
            ON MATCH SET
                r.name = rune_data.name,
                r.icon = rune_data.icon,
                r.shortDescription = rune_data.shortDesc,
                r.longDescription = rune_data.longDesc
            WITH r, rune_data
            MATCH (rp:RunePath {id: rune_data.path_id})
            MERGE (r)-[:BELONGS_TO_PATH]->(rp)
            RETURN count(r) AS count
            """
            result_runes = db_connector.run_query(rune_query, {"props": runes_params}, single_record=True)
            if result_runes:
                total_nodes_processed += result_runes['count']

        logger.info(f"Ingestione Rune completata. Nodi creati/aggiornati: {total_nodes_processed}.")
        return total_nodes_processed

    except Exception as e:
        logger.error(f"Errore durante l'ingestione delle Rune: {e}", exc_info=True)
        return 0

def ingest_summoner_spells_to_neo4j(db_connector, summoner_spells_data):
    """
    Ingests summoner spell data into Neo4j.
    Args:
        db_connector: An instance of Neo4jConnector.
        summoner_spells_data (dict): A dictionary of summoner spell JSON objects.
    """
    if not summoner_spells_data:
        logger.warning("Nessun dato incantesimo dell'evocatore fornito per l'ingestione.")
        return 0

    spell_count = 0
    logger.info("Inizio ingestione Summoner Spells in Neo4j...")

    all_params = []
    for spell_id, spell_details in summoner_spells_data.items():
        # Consider only spells for CLASSIC map (map '1' in Data Dragon 'modes' field)
        if 'modes' in spell_details and 'CLASSIC' in spell_details['modes']:
            params = {
                "id": spell_details.get('id'),
                "name": spell_details.get('name'),
                "description": clean_description(spell_details.get('description', '')), # Clean description
                "tooltip": clean_description(spell_details.get('tooltip', '')),         # Clean tooltip
                "cooldownBurn": spell_details.get('cooldownBurn'),
                "summonerLevel": spell_details.get('summonerLevel'),
                "modes": spell_details.get('modes'), # List of modes (this is fine as it's an array of strings)
                "image_full": spell_details.get('image', {}).get('full')
            }
            all_params.append(params)
        else:
            logger.debug(f"Incantesimo {spell_id} non disponibile per la modalità CLASSIC. Salto.")

    if not all_params:
        logger.info("Nessun incantesimo dell'evocatore valido per la modalità CLASSIC trovato per l'ingestione.")
        return 0

    try:
        batch_query = """
        UNWIND $props AS p
        MERGE (ss:SummonerSpell {id: p.id})
        ON CREATE SET
            ss.name = p.name,
            ss.description = p.description,
            ss.tooltip = p.tooltip,
            ss.cooldownBurn = p.cooldownBurn,
            ss.summonerLevel = p.summonerLevel,
            ss.modes = p.modes,
            ss.image_full = p.image_full
        ON MATCH SET
            ss.name = p.name,
            ss.description = p.description,
            ss.tooltip = p.tooltip,
            ss.cooldownBurn = p.cooldownBurn,
            ss.summonerLevel = p.summonerLevel,
            ss.modes = p.modes,
            ss.image_full = p.image_full
        RETURN ss.id
        """
        results = db_connector.run_query(batch_query, {"props": all_params})
        spell_count = len(results) if results else 0
        logger.info(f"Ingestione Summoner Spells completata. Nodi creati/aggiornati: {spell_count}.")
    except Exception as e:
        logger.error(f"Errore durante l'ingestione dei Summoner Spells: {e}", exc_info=True)

    return spell_count