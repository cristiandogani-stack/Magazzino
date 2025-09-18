Questa versione dell'applicazione introduce due funzionalità di amministrazione utili per la gestione dei dati:

1. **Export Database** – consente di esportare l'intero contenuto del database in vari formati:
   - *Excel (.xlsx)*: un foglio per ogni tabella. È necessario che le librerie `pandas` e `openpyxl` siano installate; in caso contrario viene proposto automaticamente il formato CSV.
   - *CSV (zip)*: un archivio ZIP con un file `.csv` per ciascuna tabella.
   - *JSON*: un singolo file `.json` che contiene per ogni tabella un array di oggetti.
   - *SQLite*: una copia del file del database in uso (utile per backup o analisi avanzate).
   - *All‑in‑one (zip)*: un archivio ZIP che include il database SQLite, il file JSON, tutti i CSV e – se possibile – anche l'Excel.

2. **Resetta Database** – svuota completamente tutte le tabelle (ad eccezione della tabella degli utenti) e cancella i file caricati nella cartella `uploads/`. Questa operazione è irreversibile e richiede la conferma dell'utente. Dopo il reset, il database è vuoto e i dizionari di materiali, fornitori, produttori, tipi e macchine resteranno privi di elementi finché non verranno aggiunti nuovi dati tramite l'interfaccia.

Entrambe le funzioni sono accessibili dalla scheda **Gestione Accessi** tramite i pulsanti situati accanto al titolo.

È stato aggiunto anche il flag `SEED_DEFAULTS` in `app.py`. Impostandolo a `False` si disabilita l'inserimento automatico dei valori di default nei vocabolari (ad esempio “Fornitore Generico”, “Marmitalia”, ecc.) durante la prima inizializzazione del database. Questo consente di partire con dizionari completamente vuoti.

Per eventuali ulteriori personalizzazioni (ad esempio esportare solo alcune tabelle o includere altri formati), è sufficiente modificare la funzione `export_database` in `app.py`.