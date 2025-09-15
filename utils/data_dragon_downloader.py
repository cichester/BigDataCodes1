# utils/data_dragon_downloader.py
import requests
import os
import json
import logging

logger = logging.getLogger(__name__)

class DataDragonDownloader:
    BASE_URL = "https://ddragon.leagueoflegends.com/cdn"
    VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"

    def __init__(self, version=None, language="en_US", data_dir="data"):
        self.language = language
        self.data_dir = data_dir
        
        # Inizializza la cache per i dati dei campioni
        self._champion_cache = None

        if version:
            self.version = version
        else:
            self.version = self.get_latest_version() or "13.19.1"
            if self.version is None:
                logger.error("Impossibile recuperare l'ultima versione di Data Dragon. Usando la versione predefinita (13.19.1) come fallback.")
                self.version = "13.19.1"

        self.base_version_language_path = os.path.join(self.data_dir, self.version, self.language)
        os.makedirs(self.base_version_language_path, exist_ok=True)
        logger.info(f"Data Dragon Downloader inizializzato per versione: {self.version}, lingua: {self.language}")

    def get_latest_version(self):
        """Recupera l'ultima versione disponibile di Data Dragon."""
        logger.info("Recupero dell'ultima versione di Data Dragon...")
        try:
            response = requests.get(self.VERSIONS_URL)
            response.raise_for_status()
            versions = response.json()
            if versions:
                latest_version = versions[0]
                logger.info(f"Ultima versione di Data Dragon trovata: {latest_version}")
                return latest_version
            else:
                logger.warning("Nessuna versione trovata da Data Dragon.")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante il recupero delle versioni di Data Dragon da {self.VERSIONS_URL}: {e}")
            return None
        except Exception as e:
            logger.error(f"Errore imprevisto durante il recupero delle versioni: {e}")
            return None

    def _get_file_url(self, data_type, champion_id=None):
        """Costruisce l'URL completo per il download del file JSON."""
        if data_type == 'champion' and champion_id:
            return f"{self.BASE_URL}/{self.version}/data/{self.language}/champion/{champion_id}.json"
        elif data_type == 'champion':
            return f"{self.BASE_URL}/{self.version}/data/{self.language}/champion.json"
        elif data_type == 'championFull':
            return f"{self.BASE_URL}/{self.version}/data/{self.language}/championFull.json"
        elif data_type in ['item', 'summoner', 'profileicon', 'map', 'language', 'sticker', 'runesReforged']:
            return f"{self.BASE_URL}/{self.version}/data/{self.language}/{data_type}.json"
        else:
            raise ValueError(f"Tipo di dati non supportato: {data_type}")

    def _download_and_load_json(self, url, save_path):
        """Scarica un file JSON e lo carica in memoria."""
        if not os.path.exists(save_path):
            logger.info(f"Scaricando da: {url} a {save_path}")
            try:
                response = requests.get(url)
                response.raise_for_status()
                json_data = response.json()
                
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=4, ensure_ascii=False)
                logger.info(f"Download e salvataggio completato: {save_path}")
                return json_data
            except requests.exceptions.RequestException as e:
                logger.error(f"Errore durante il download da {url}: {e}")
                return None
            except json.JSONDecodeError as e:
                logger.error(f"Errore di decodifica JSON da {url}: {e}")
                return None
            except Exception as e:
                logger.error(f"Errore generico durante il download o il salvataggio di {url}: {e}")
                return None
        else:
            logger.info(f"File {save_path} già presente. Caricamento esistente.")
            try:
                with open(save_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Errore durante la lettura del file {save_path}: {e}")
                return None

    def get_data(self, data_type, force_download=False):
        """
        Scarica e carica i dati specifici da Data Dragon.
        Gestisce i diversi formati di risposta di Data Dragon.
        """
        logger.info(f"Data Dragon: Tentativo di recupero dati per tipo '{data_type}' (versione: {self.version})")
        file_name = f"{data_type}.json"
        
        file_path = os.path.join(self.base_version_language_path, file_name)
        url = self._get_file_url(data_type)

        if not force_download and os.path.exists(file_path):
            logger.info(f"File {file_path} già presente. Saltando il download.")
            json_data = self._load_json_data(file_path)
        else:
            json_data = self._download_and_load_json(url, file_path)

        if json_data:
            if data_type in ['champion', 'item', 'summoner', 'profileicon', 'map', 'language', 'sticker', 'championFull']:
                return json_data.get('data')
            elif data_type == 'runesReforged':
                return json_data
            else:
                logger.warning(f"Tipo di dati '{data_type}' non gestito esplicitamente per la chiave 'data'. Restituisco JSON completo.")
                return json_data
        return None

    def _load_json_data(self, file_path):
        """Carica un file JSON esistente."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"File non trovato: {file_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Errore di decodifica JSON dal file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Errore generico durante la lettura del file {file_path}: {e}")
            return None
        
    def get_champion_by_id(self, champion_id: int):
        """
        Recupera i dati di un campione dato il suo ID numerico.
        Usa una cache interna per evitare di ricaricare il file JSON a ogni chiamata.
        """
        if self._champion_cache is None:
            champions_data = self.get_data('champion')
            if not champions_data:
                logger.error("Impossibile recuperare i dati dei campioni da Data Dragon.")
                return None
                
            self._champion_cache = {int(champ['key']): champ for champ in champions_data.values()}

        return self._champion_cache.get(champion_id)