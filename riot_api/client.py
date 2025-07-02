# riot_api/client.py

import requests
import time
import logging
from requests.exceptions import HTTPError
import os # Assicurati che os sia importato per os.getenv

logger = logging.getLogger(__name__)

class RiotAPIClient:
    def __init__(self, api_key: str, region: str = "euw1"):
        self.api_key = api_key
        self.region = region
        
        # Mapping per routing specifico della piattaforma (es. euw1, na1)
        self.platform_routing = self._get_platform_routing(region) 
        # Mapping per routing regionale globale (es. europe, americas) per Account-V1
        self.regional_routing = self._get_regional_routing(region) 

        self.base_url_platform = f"https://{self.platform_routing}.api.riotgames.com/lol"
        self.base_url_regional = f"https://{self.regional_routing}.api.riotgames.com/riot" # Nuovo URL base per Account-V1

        # Rate limiting properties (il resto del codice rimane lo stesso)
        self.max_retries = 5 # Assicurati che sia definito
        self.requests_per_second = 20
        self.requests_per_two_minutes = 100
        self.last_request_time = 0
        self.request_count_second = 0
        self.request_count_two_minutes = 0
        self.start_of_second = time.time()
        self.start_of_two_minutes = time.time()

        logger.info(f"RiotAPIClient inizializzato per regione: {self.region}")
        logger.info(f"Rate limits impostati: {self.requests_per_second}/1s e {self.requests_per_two_minutes}/120s")

    def _get_platform_routing(self, region: str) -> str:
        # Questa mappa le regioni ai loro valori di routing della piattaforma (per Summoner-V4, Match-V5)
        return region

    def _get_regional_routing(self, region: str) -> str:
        # Questa mappa le regioni ai loro valori di routing regionali (per Account-V1)
        if region in ["euw1", "eun1", "tr1", "ru"]: # Aggiungi altre regioni europee se necessario
            return "europe"
        elif region in ["na1", "br1", "la1", "la2"]:
            return "americas"
        elif region in ["kr", "jp1"]:
            return "asia"
        elif region in ["oc1", "ph2", "sg2", "th2", "tw2", "vn2"]:
            return "sea"
        else:
            return "europe" # Default

    def _make_request(self, url: str, headers: dict = None, attempt: int = 1):
        # Il resto di questa funzione (retry, backoff, headers) rimane invariato
        # Assicurati che l'API key sia sempre inclusa negli header
        if headers is None:
            headers = {}
        headers['X-Riot-Token'] = self.api_key

        try:
            self._apply_rate_limiting() # Assicurati che questa riga sia presente e gestisca i limiti
            logger.info(f"Effettuando richiesta a: {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status() 
            return response.json()
        except HTTPError as e:
            logger.error(f"HTTPError: {e.response.status_code} per URL: {url} (Tentativo {attempt}/{self.max_retries})")
            if e.response.status_code == 429: # Too many requests
                retry_after = int(e.response.headers.get('Retry-After', 1))
                logger.warning(f"Rate limit raggiunto. Ritento dopo {retry_after} secondi.")
                time.sleep(retry_after)
                if attempt < self.max_retries:
                    return self._make_request(url, headers, attempt + 1)
            elif e.response.status_code == 403: # Forbidden
                logger.error(f"Errore non gestito 403 per URL: {url}. La chiave API potrebbe essere non valida o non avere i permessi necessari. Rilancio.")
                raise # Rilancia l'errore per fermare il processo
            elif e.response.status_code == 404: # Not Found
                logger.warning(f"Risorsa non trovata: {url}")
                return None
            
            logger.error(f"Errore HTTP imprevisto: {e.response.status_code} per URL: {url}. Rilancio.")
            raise # Rilancia l'errore se non è gestito


    def _apply_rate_limiting(self):
        # Questo è il tuo metodo per gestire i rate limit, assicurati sia presente e funzionante
        current_time = time.time()

        # Reset for the 1-second window
        if current_time - self.start_of_second >= 1:
            self.start_of_second = current_time
            self.request_count_second = 0

        # Reset for the 120-second window
        if current_time - self.start_of_two_minutes >= 120:
            self.start_of_two_minutes = current_time
            self.request_count_two_minutes = 0

        # Wait if current limit reached
        if self.request_count_second >= self.requests_per_second:
            sleep_time = 1 - (current_time - self.start_of_second)
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        if self.request_count_two_minutes >= self.requests_per_two_minutes:
            sleep_time = 120 - (current_time - self.start_of_two_minutes)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
        self.request_count_second += 1
        self.request_count_two_minutes += 1

    # --- NUOVA FUNZIONE: Ottieni Account (PUUID, GameName, TagLine) tramite Riot ID ---
    def get_account_by_riot_id(self, game_name: str, tag_line: str):
        # Questo usa il routing regionale (europe, americas, ecc.) per Account-V1
        url = f"{self.base_url_regional}/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
        logger.info(f"Richiesta Account by Riot ID: {game_name}#{tag_line}")
        try:
            return self._make_request(url)
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Riot ID '{game_name}#{tag_line}' non trovato.")
                return None
            raise # Rilancia altri errori HTTP

    # --- FUNZIONE ESISTENTE: Ottieni dati evocatore tramite PUUID ---
    def get_summoner_by_puuid(self, puuid: str):
        # Questo usa il routing della piattaforma (euw1, na1, ecc.) per Summoner-V4
        url = f"{self.base_url_platform}/summoner/v4/summoners/by-puuid/{puuid}"
        logger.info(f"Richiesta Summoner by PUUID: {puuid}")
        return self._make_request(url)

    # Nota: la vecchia funzione get_summoner_by_name probabilmente non è più necessaria.
    # Se vuoi comunque un singolo punto di ingresso per ottenere tutti i dati dell'evocatore da un Riot ID,
    # puoi creare una funzione combinata:
    def get_full_summoner_data_by_riot_id(self, game_name: str, tag_line: str):
        account_data = self.get_account_by_riot_id(game_name, tag_line)
        if account_data and 'puuid' in account_data:
            puuid = account_data['puuid']
            # Ora usa l'endpoint Summoner-V4 per ottenere i dettagli specifici dell'evocatore
            summoner_data = self.get_summoner_by_puuid(puuid)
            
            if summoner_data:
                # Combina i dati dell'account (gameName, tagLine, puuid) con i dati dell'evocatore
                # (id, accountId, name, profileIconId, summonerLevel)
                return {**account_data, **summoner_data}
            return account_data # Ritorna almeno il puuid se i dati dell'evocatore non sono disponibili
        return None
    
    def get_match_ids_by_puuid(self, puuid: str, count: int = 20, start: int = 0):
        """
        Ottiene una lista di Match IDs per un dato PUUID.
        Questa API usa il routing regionale (europe, americas, asia).
        """
        # Match-V5 usa il base_url_regional (es. europe.api.riotgames.com)
        url = f"{self.base_url_regional}/match/v5/matches/by-puuid/{puuid}/ids?start={start}&count={count}"
        logger.info(f"Richiesta Match IDs per PUUID: {puuid} (Count: {count}, Start: {start})")
        try:
            return self._make_request(url)
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Nessun Match ID trovato per PUUID: {puuid}")
                return [] # Ritorna una lista vuota se non ci sono partite
            raise # Rilancia altri errori HTTP