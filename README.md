Costruzione di un Knowledge Graph per l'Universo di League of Legends
Questo progetto implementa una pipeline di dati end-to-end, scalabile e resiliente, progettata per ingerire, processare e strutturare dati eterogenei provenienti dall'universo di League of Legends. La pipeline utilizza Apache Kafka per lo streaming dei dati, Neo4j come database a grafo e l'Intelligenza Artificiale Generativa (Google Gemini + LangChain) per arricchire i dati e fornire un'interfaccia di interrogazione in linguaggio naturale.

## 1. Prerequisiti
Prima di iniziare, assicurati di avere installato il seguente software:

Python 3.10+

Docker e Docker Compose: Per eseguire l'infrastruttura di Kafka e Neo4j.

Neo4j Desktop (Consigliato): Per visualizzare e interagire facilmente con il Knowledge Graph.

## 2. Configurazione dell'Ambiente
Segui questi passaggi per configurare il progetto in locale.

### a. Clonare il Repository
git clone [https://github.com/cichester/BigDataCodes1.git](https://github.com/cichester/BigDataCodes1.git)
cd BigDataCodes1

### b. Creare l'Ambiente Virtuale Python
È una best practice isolare le dipendenze del progetto.

python -m venv venv_lolkg
# Su Windows:
venv_lolkg\Scripts\activate.ps1


### c. Installare le Dipendenze
Installa tutti i pacchetti necessari con un singolo comando:

pip install -r requirements.txt

### d. Avviare l'Infrastruttura (Kafka & Neo4j)
Il file docker-compose.yml fornito configurerà e avvierà i container per Kafka e Zookeeper.

docker-compose up -d

Dopo aver eseguito questo comando, i servizi saranno in esecuzione in background.

Attivare Neo4j tramite l'apposita applicazione Neo4j desktop, altrimenti configurare il docker-compose in modo tale da poter avviare un'istanza di Neo4j.
Una volta creato il db ferma l'esecuzione. Copiare il file **apoc-2025.05.0-core** all'interno della cartella **"C:\Users\IlTuoUtente\ .Neo4jDesktop2\Data\dbmss\dbms-IDDelTuoDBMS\plugins"**, facilmente raggiungibile cliccando sul simbolo della cartella presente nell'interfaccia di Neo4j Desktop. Poi, posizionati nella cartella **"C:\Users\IlTuoUtente\ .Neo4jDesktop2\Data\dbmss\dbms-IDDelTuoDBMS\conf"**, apri con l'editor di testo che preferisci il file **"neo4j"** ed aggiungi dbms.security.procedures.unrestricted=apoc.meta.*   , in questo modo l'ia potrà leggere il db ed interrogarlo.
Riavviare dunque il GraphDB.

### e. Configurare le Variabili d'Ambiente
Crea un file chiamato .env nella cartella principale del progetto e incollaci il seguente contenuto, sostituendo i valori con le tue credenziali e percorsi.

# Credenziali per il database Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=LaTuaPassword

# La tua chiave API per Google Gemini
GOOGLE_API_KEY=AIzaSy...laTuaChiaveAPI...

# Il percorso ASSOLUTO alla cartella che contiene i file .txt della lore
LORE_FILES_PATH="G:/BigDataCodes/champion_lore_data"

Nota Bene: Il percorso LORE_FILES_PATH deve essere assoluto e corretto per il tuo sistema.

## 3. Esecuzione della Pipeline
L'esecuzione del sistema richiede l'avvio di tre processi distinti in terminali separati. Assicurati che il tuo ambiente virtuale (venv_lolkg) sia attivato in ogni terminale.

### Passo 0: Estrazione della Lore (Operazione Una Tantum)
Questo script va eseguito una sola volta per analizzare i file di lore tramite l'LLM e generare il file lore_data.json che verrà poi caricato nel grafo.

python -m data_ingestion.extract_lore_data

Attendi il completamento di questo script prima di procedere con gli altri passaggi.

### Passo 1: Avviare il Producer (Terminale 1)
Questo script simula il flusso di dati, leggendo le partite dai file JSON e inviandole al topic di Kafka.

**ATTENZIONE:** Prima di eseguirlo, apri il file data_ingestion/kafka_producer.py e assicurati che la variabile KAGGLE_DATA_PATH punti alla cartella corretta dove hai salvato i file delle partite.

python data_ingestion/kafka_producer.py

Lascia questo terminale in esecuzione. Inizierà a inviare messaggi a Kafka.

## Passo 2: Avviare il Consumer (Terminale 2)
Questo è il cuore della pipeline. All'avvio, ingerirà i dati statici (campioni, oggetti, etc.) e la lore pre-elaborata. Successivamente, si metterà in ascolto sul topic di Kafka per ricevere e processare le partite in streaming.

python -m main

Vedrai i log dell'ingestione e il sistema rimarrà attivo, in attesa di nuovi messaggi da Kafka.

## Passo 3: Avviare l'Agente di Interrogazione (Terminale 3)
Questo script avvia l'agente conversazionale che ti permette di interrogare il grafo in linguaggio naturale.

python query_agent.py

Attendi il messaggio "👤 La tua domanda:" e poi inizia a porre le tue domande.

## 4. Note Aggiuntive
Pulizia dell'Ambiente Docker: Per fermare e rimuovere tutti i container e i volumi (inclusi i dati di Kafka e Neo4j), usa il seguente comando:

docker-compose down --volumes

Visualizzazione del Grafo: Apri Neo4j Desktop e connettiti all'istanza locale (di solito bolt://localhost:7687 con utente neo4j e la password che hai impostato nel file .env) per esplorare visualmente il Knowledge Graph mentre viene costruito.
