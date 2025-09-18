import os
import shutil
import sqlite3
import socket
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, Response, session, jsonify

# ---------------------------------------------------------------------------
# Moduli standard aggiuntivi
#
# json:    usato per serializzare l'intero database in formato JSON.
# io:      fornisce StringIO per scrivere CSV come testo prima di convertirlo in bytes.
# zipfile: utilizzato per creare archivi ZIP sia per l'export CSV che per il pacchetto completo.
import json
import io
import zipfile
import qrcode
from io import BytesIO
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import csv
from datetime import datetime, timedelta
from urllib.parse import quote
import smtplib
from email.message import EmailMessage

# Per il caricamento di documenti associati ai materiali utilizziamo
# secure_filename per evitare problemi di path traversal e generiamo
# nomi univoci con uuid4.  Inoltre definiamo un set di estensioni
# consentite (immagini e PDF) e un percorso di salvataggio dedicato.
from werkzeug.utils import secure_filename
from uuid import uuid4

# Aggiungiamo le classi di Pillow per creare un PDF di QR code. La
# libreria Pillow è già presente nell'ambiente e consente di
# comporre immagini e testo su pagine. Servirà per generare un
# documento contenente tutti i QR generati.
from PIL import Image, ImageDraw, ImageFont

# ----------------------------------------------------------------------
# Funzione di migrazione per la tabella riordino_soglie_ext
#
# In alcune versioni precedenti l'applicazione creava la tabella
# ``riordino_soglie_ext`` senza includere tutte le colonne previste
# (ad esempio mancavano le dimensioni o il produttore) oppure definiva
# la chiave primaria solo sulla combinazione materiale/tipo/spessore.
# Questa funzione verifica la struttura della tabella ed esegue una
# migrazione schema‑on‑the‑fly qualora siano assenti colonne o la
# chiave primaria non includa anche ``dimensione_x``, ``dimensione_y`` e
# ``produttore``.  In tal caso la vecchia tabella viene rinominata,
# viene creata la nuova tabella con lo schema corretto e i dati
# esistenti vengono copiati, assegnando valori vuoti per le colonne
# mancanti.  La funzione è idempotente e può essere invocata ad ogni
# aggiornamento di soglia o quantità senza effetti collaterali.

def ensure_riordino_soglie_ext_schema(conn: sqlite3.Connection) -> None:
    """Verifica e, se necessario, migra la tabella ``riordino_soglie_ext``.

    La tabella corretta deve avere le colonne:

        - materiale (TEXT, NOT NULL)
        - tipo (TEXT, NOT NULL)
        - spessore (TEXT, NOT NULL)
        - dimensione_x (TEXT, NOT NULL)
        - dimensione_y (TEXT, NOT NULL)
        - produttore (TEXT, NOT NULL)
        - threshold (INTEGER, NOT NULL)
        - quantita_riordino (INTEGER, opzionale)

    ed una chiave primaria composta da tutte le sei colonne testuali.

    :param conn: connessione SQLite già aperta
    """
    try:
        # Controlla se la tabella esiste
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='riordino_soglie_ext'"
        ).fetchone()
        if not cur:
            # La tabella non esiste: crearla con lo schema corretto
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS riordino_soglie_ext (
                    materiale TEXT NOT NULL,
                    tipo TEXT NOT NULL,
                    spessore TEXT NOT NULL,
                    dimensione_x TEXT NOT NULL,
                    dimensione_y TEXT NOT NULL,
                    produttore TEXT NOT NULL,
                    threshold INTEGER NOT NULL,
                    quantita_riordino INTEGER,
                    PRIMARY KEY (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore)
                )
                """
            )
            return
        # Recupera le informazioni sulle colonne della tabella esistente
        col_info = conn.execute("PRAGMA table_info(riordino_soglie_ext)").fetchall()
        existing_cols = {row[1]: row for row in col_info}  # row[1] = name
        # Determina le colonne appartenenti alla chiave primaria (pk > 0)
        pk_cols = {row[1] for row in col_info if row[5] > 0}
        # Elenco delle colonne e PK desiderate
        required_cols = {
            'materiale', 'tipo', 'spessore', 'dimensione_x', 'dimensione_y', 'produttore', 'threshold', 'quantita_riordino'
        }
        required_pk = {
            'materiale', 'tipo', 'spessore', 'dimensione_x', 'dimensione_y', 'produttore'
        }
        # Se mancano colonne o la PK non corrisponde, esegui migrazione
        if not required_cols.issubset(existing_cols.keys()) or pk_cols != required_pk:
            # Rinomina la vecchia tabella
            conn.execute("ALTER TABLE riordino_soglie_ext RENAME TO riordino_soglie_ext_old")
            # Crea la nuova tabella con lo schema corretto
            conn.execute(
                """
                CREATE TABLE riordino_soglie_ext (
                    materiale TEXT NOT NULL,
                    tipo TEXT NOT NULL,
                    spessore TEXT NOT NULL,
                    dimensione_x TEXT NOT NULL,
                    dimensione_y TEXT NOT NULL,
                    produttore TEXT NOT NULL,
                    threshold INTEGER NOT NULL,
                    quantita_riordino INTEGER,
                    PRIMARY KEY (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore)
                )
                """
            )
            # Copia i dati dalla vecchia tabella nella nuova, assegnando stringa vuota alle colonne mancanti
            # Recupera i record dalla vecchia tabella
            old_rows = conn.execute("SELECT * FROM riordino_soglie_ext_old").fetchall()
            for row in old_rows:
                row_dict = {col: row[col] for col in row.keys()}
                # Prepara valori per le nuove colonne, fornendo stringa vuota se mancanti
                mat = row_dict.get('materiale', '') or ''
                tp = row_dict.get('tipo', '') or ''
                sp = row_dict.get('spessore', '') or ''
                dx = row_dict.get('dimensione_x', '') or ''
                dy = row_dict.get('dimensione_y', '') or ''
                prod = row_dict.get('produttore', '') or ''
                threshold_val = row_dict.get('threshold', DEFAULT_REORDER_THRESHOLD) or DEFAULT_REORDER_THRESHOLD
                quant = row_dict.get('quantita_riordino')
                # Inserisci nella nuova tabella
                conn.execute(
                    "INSERT OR IGNORE INTO riordino_soglie_ext (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore, threshold, quantita_riordino) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (mat, tp, sp, dx, dy, prod, threshold_val, quant),
                )
            # Elimina la vecchia tabella
            conn.execute("DROP TABLE IF EXISTS riordino_soglie_ext_old")
    except Exception:
        # In caso di errore di migrazione ignoriamo l'eccezione per non interrompere l'esecuzione
        pass

# Funzione di utilità per interpretare filtri numerici come valori esatti o range.
def _parse_range_or_exact(s: str):
    """Parsa un input filtro che può essere:
    - vuoto => None
    - "40-60" => (40.0, 60.0) inclusivo
    - "50" => (50.0, 50.0) match esatto
    Accetta virgola come separatore decimale.
    Restituisce una tupla (min_v, max_v) oppure None se non parsabile.
    """
    if not s:
        return None
    s = str(s).strip()
    # normalizza virgola come punto
    s = s.replace(',', '.')
    if '-' in s:
        parts = [p.strip() for p in s.split('-', 1)]
        try:
            lo = float(parts[0]) if parts[0] != '' else None
            hi = float(parts[1]) if parts[1] != '' else None
        except ValueError:
            return None
        # Se uno dei due valori manca, non applichiamo il filtro
        if lo is None or hi is None:
            return None
        if lo > hi:
            lo, hi = hi, lo
        return (lo, hi)
    else:
        try:
            v = float(s)
        except ValueError:
            return None
        return (v, v)

# Nuova funzione di utilità per interpretare le stringhe di ubicazione.
def _parse_location_string(loc: str):
    """
    Parsea una stringa di ubicazione come "A-12" o "A12" e ritorna una tupla (lettera, numero).
    Accetta anche formati con separatori diversi e spazi. Restituisce (None, None) se la stringa non è valida.

    La funzione normalizza la stringa rimuovendo caratteri non alfanumerici (sostituendoli con un trattino),
    converte la lettera in maiuscolo e accetta anche formati privi di trattino.  Se il formato
    non corrisponde al pattern lettera seguita da un numero, restituisce (None, None).
    """
    if not loc:
        return None, None
    # Import locale di 're' per evitare dipendenze globali
    import re as _re  # type: ignore
    s = str(loc).strip().upper()
    # Sostituisci tutti i separatori non alfanumerici con un trattino unico
    s = _re.sub(r"[^A-Z0-9]+", "-", s)
    # Accetta sia "A-12" che "A12" con un numero da 1 a 3 cifre
    m = _re.match(r"^([A-Z])[-]?([0-9]{1,3})$", s)
    if not m:
        return None, None
    return m.group(1), int(m.group(2))

# The ``MATERIALI`` constant remains for backwards compatibility but is no
# longer used directly in the application.  The list of available
# materials is now stored in a dedicated SQLite table (see
# ``VOCAB_TABLE``) and can be managed through the web interface.  If
# ``MATERIALI`` changes in future versions, the new values will only be
# used to seed an empty vocabulary.
MATERIALI = []

# ----------------------------------------------------------------------------
#  Applicazione di gestione magazzino
#
#  Questa applicazione web consente di gestire un piccolo magazzino di
#  materiale. È possibile inserire nuovi materiali con informazioni
#  dettagliate (dimensioni, spessore, quantità, ubicazione, fornitore e note),
#  consultare la lista completa con filtri e ricerca, modificare o
#  cancellare voci esistenti, aggiornare la quantità disponibile e
#  esportare l'inventario in formato CSV. Ogni materiale è identificato da
#  un codice QR generato automaticamente che ne semplifica la ricerca e
#  l'eliminazione tramite scansione (funziona anche da cellulare sulla
#  stessa rete). La grafica è curata e responsive grazie all'uso di
#  Bootstrap e di un foglio di stile personalizzato.
#
#  Per avviare l'applicazione basta eseguire `python app.py` dalla
#  directory del progetto. I file statici e i template HTML si trovano
#  rispettivamente nelle cartelle ``static`` e ``templates``.
# ----------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = 'supersegreto'  # Cambiare in produzione con un valore segreto

DATABASE = "magazzino.db"

# ----------------------------------------------------------------------
# Gestione ID univoci per le lastre
#
# Per garantire che gli ID assegnati alle lastre non vengano mai riutilizzati
# anche dopo la loro eliminazione, introduciamo un database SQLite
# separato e nascosto.  Questo file, denominato ``.slab_ids.db``,
# memorizza tutti gli identificativi che sono stati assegnati alle
# nuove lastre nel tempo.  Ogni ID viene aggiunto a questa tabella
# al momento della creazione della lastra e non viene mai rimosso.
# In questo modo, anche se il database principale ``magazzino.db``
# venisse resettato o se gli ID venissero cancellati dalla tabella
# ``materiali``, avremo comunque memoria degli ID già utilizzati.

# Percorso del database nascosto utilizzato per tracciare gli ID delle lastre.
ID_TRACKING_DB = os.path.join(os.path.dirname(__file__), '.slab_ids.db')

def get_id_db_connection():
    """Restituisce una connessione al database degli ID.

    Questo database è separato da ``magazzino.db`` e contiene
    semplicemente una tabella ``slab_ids`` con una colonna ``id`` che
    raccoglie tutti gli identificativi delle lastre creati.  La
    funzione applica lo stesso factory per restituire risultati come
    dizionari, ma non applica pragma avanzati poiché il carico è
    minimo.
    """
    conn = sqlite3.connect(ID_TRACKING_DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_id_db() -> None:
    """Inizializza il database degli ID se necessario.

    Crea la tabella ``slab_ids`` con una singola colonna ``id``
    dichiarata come ``INTEGER PRIMARY KEY``.  Se la tabella esiste
    già non viene modificata.  La chiamata di commit viene eseguita
    esplicitamente per assicurare che la struttura sia persistita.
    """
    try:
        with get_id_db_connection() as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS slab_ids (id INTEGER PRIMARY KEY)')
            conn.commit()
    except Exception:
        # In caso di errore di creazione del DB nascosto ignoriamo
        # l'eccezione per non impedire l'avvio dell'applicazione.
        pass

def record_used_ids(ids: list[int]) -> None:
    """Registra una o più ID di lastre nel database nascosto.

    Accetta un elenco di interi e, per ciascuno, esegue un
    ``INSERT OR IGNORE`` nella tabella ``slab_ids`` del database
    nascosto.  In questo modo gli ID vengono memorizzati una sola
    volta e non sono mai rimossi.  La funzione gestisce
    silenziosamente eventuali errori di inserimento e assicura il
    commit al termine.

    :param ids: lista di identificativi da registrare
    """
    if not ids:
        return
    try:
        with get_id_db_connection() as conn:
            # Assicura che la tabella esista prima dell'inserimento
            conn.execute('CREATE TABLE IF NOT EXISTS slab_ids (id INTEGER PRIMARY KEY)')
            for rid in ids:
                try:
                    conn.execute('INSERT OR IGNORE INTO slab_ids (id) VALUES (?)', (int(rid),))
                except Exception:
                    # Se l'inserimento fallisce (ad esempio se rid non è convertibile) ignora
                    pass
            try:
                conn.commit()
            except Exception:
                pass
    except Exception:
        # Se non riusciamo ad aprire il DB nascosto, non impediamo
        # l'inserimento del materiale nel DB principale.
        pass

# ----------------------------------------------------------------------
# Configurazione stampante Zebra
#
# La stampante Zebra viene utilizzata per la stampa diretta delle etichette.
# Le impostazioni di connessione (host e porta) vengono lette dal file
# ``zebra_config.txt`` presente nella directory dell'applicazione. Se il
# file non esiste o non contiene valori validi, vengono utilizzati i
# valori di default definiti qui di seguito.  La funzione
# ``load_zebra_config`` restituisce sempre una tupla (host, port).

# Percorso del file di configurazione.  Si trova accanto a questo modulo.
ZEBRA_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'zebra_config.txt')

# Valori di default utilizzati se il file non specifica host/port.
DEFAULT_ZEBRA_HOST = '192.168.100.23'
DEFAULT_ZEBRA_PORT = 9100

# ----------------------------------------------------------------------
# Configurazione email SMTP
#
# Per l'invio automatico delle email ai fornitori è necessario
# configurare un server SMTP.  Le impostazioni vengono lette da
# ``smtp_config.txt`` se presente, altrimenti dai valori di default o
# dalle variabili d'ambiente esistenti.  Il file ha formato
# ``chiave=valore`` per ciascuna opzione e supporta i seguenti campi:
#
# - host: indirizzo del server SMTP
# - port: porta del server (numerica)
# - user: username per l'autenticazione (facoltativo)
# - pass: password per l'autenticazione (facoltativa)
# - from: indirizzo mittente di default (facoltativo)
# - tls: 'true' o 'false' per abilitare STARTTLS (di default abilitato)
#
# Due funzioni di utilità sono fornite per caricare e salvare le
# impostazioni: ``load_smtp_config`` e ``save_smtp_config``.

# Percorso del file di configurazione SMTP (accanto a questo modulo).
SMTP_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'smtp_config.txt')

def load_smtp_config() -> dict[str, str | None]:
    """Carica la configurazione SMTP dal file ``smtp_config.txt``.

    Se il file è presente, ogni riga non vuota e non commentata deve
    contenere una coppia ``chiave=valore``.  Le chiavi supportate sono
    'host', 'port', 'user', 'pass', 'from' e 'tls'.  Le chiavi vengono
    restituite con la stessa denominazione utilizzata nel file.  Se il
    file non esiste o non può essere letto, viene restituito un
    dizionario vuoto.

    :return: un dizionario con le opzioni SMTP lette dal file
    """
    settings: dict[str, str | None] = {}
    try:
        with open(SMTP_CONFIG_FILE, encoding='utf-8') as cfg:
            for line in cfg:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    settings[key] = value
    except FileNotFoundError:
        # Nessuna configurazione presente
        return {}
    except Exception:
        # Se c'è un errore di lettura, torna un dizionario vuoto
        return {}
    return settings

def save_smtp_config(settings: dict[str, str | None]) -> None:
    """Salva la configurazione SMTP nel file ``smtp_config.txt``.

    Accetta un dizionario con le chiavi supportate ('host', 'port',
    'user', 'pass', 'from', 'tls') e sovrascrive completamente il
    contenuto del file.  I valori ``None`` o stringhe vuote non
    vengono scritti.  Le righe sono generate nel formato
    ``chiave=valore``.

    :param settings: dizionario con le impostazioni da salvare
    """
    try:
        lines: list[str] = []
        for key, value in settings.items():
            if value:
                lines.append(f"{key}={value}")
        with open(SMTP_CONFIG_FILE, 'w', encoding='utf-8') as cfg:
            cfg.write('\n'.join(lines))
    except Exception:
        # In caso di errore di scrittura, non facciamo nulla esplicito
        pass


def load_zebra_config() -> tuple[str, int]:
    """Carica la configurazione per la stampante Zebra.

    Legge le impostazioni dal file ``zebra_config.txt`` (host e port). Se il
    file non è presente o non contiene chiavi valide, restituisce i valori
    di default.  Le righe che iniziano con ``#`` vengono ignorate.

    :return: una tupla (host, port)
    """
    host = DEFAULT_ZEBRA_HOST
    port = DEFAULT_ZEBRA_PORT
    try:
        with open(ZEBRA_CONFIG_FILE, encoding='utf-8') as cfg:
            for line in cfg:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    if key == 'host':
                        host = value
                    elif key == 'port':
                        try:
                            port = int(value)
                        except ValueError:
                            # se non è un intero valido, manteniamo il valore corrente
                            pass
    except FileNotFoundError:
        # Se il file non esiste, usiamo i default
        pass
    except Exception:
        # In caso di altri errori di lettura, usiamo comunque i default
        pass
    return host, port

# ------------------------------------------------------------------
# Utilità generazione codici ordine
#
# Ogni ordine confermato deve essere associato a un codice univoco e
# progressivo nel formato "TM<anno>/<progressivo>" (ad esempio "TM{year}/0001").
# Questa funzione calcola il prossimo codice disponibile interrogando la
# tabella storico dei riordini.  Il prefisso include l'anno corrente
# seguito da una barra e un numero progressivo a quattro cifre con zeri
# a sinistra.  Se nessun codice per l'anno corrente è presente la
# numerazione riparte da 1.
def generate_order_code(conn: sqlite3.Connection) -> str:
    """Restituisce il prossimo codice ordine progressivo nel formato "TM<anno>/<progressivo>".

    Il progressivo è relativo all'anno in corso: vengono estratti tutti i codici
    esistenti dalla colonna ``numero_ordine`` della tabella ``riordini_effettuati``.
    Per i codici che iniziano con "TM<anno>/", viene prelevata la parte
    numerica dopo la barra e considerata per determinare il massimo progressivo
    per l'anno corrente.  Se non esistono codici dell'anno corrente, il
    progressivo riparte da 1.  I codici nel vecchio formato (ad esempio
    "ORD-000123") vengono ignorati ai fini del calcolo del nuovo progressivo.
    """
    import datetime
    year = datetime.datetime.now().year
    try:
        rows = conn.execute(
            "SELECT numero_ordine FROM riordini_effettuati WHERE numero_ordine IS NOT NULL AND numero_ordine != ''"
        ).fetchall()
    except sqlite3.Error:
        rows = []
    max_prog = 0
    for row in rows:
        code = row['numero_ordine']
        if not code:
            continue
        code = str(code).strip()
        # Considera solo i codici che seguono il formato TMYYYY/NNNN
        if code.startswith(f"TM{year}/"):
            try:
                prog_part = int(code.split('/', 1)[1])
                if prog_part > max_prog:
                    max_prog = prog_part
            except Exception:
                continue
    next_prog = max_prog + 1
    return f"TM{year}/{next_prog:04d}"

def _build_zpl_for_id(material_id: int) -> str:
    """Costruisce la stringa ZPL per stampare l'etichetta di un materiale.

    Per le lastre e gli sfridi viene stampato un QR code affiancato
    dall'ID del materiale, mentre per i bancali (pallet) viene
    stampato solo l'ID in grande formato occupando quasi tutta
    l'area disponibile dell'etichetta.  Le dimensioni e le posizioni
    degli elementi vengono calcolate dinamicamente in base alla
    dimensione dell'etichetta (38×22 mm) e alla risoluzione della
    stampante Zebra (203 dpi).

    :param material_id: l'identificativo del materiale
    :return: una stringa ZPL completa pronta per l'invio
    """
    # Verifica se il materiale è un bancale interrogando il database.  In
    # caso di errori o se la colonna non è presente, si considera
    # comunque come non bancale.
    is_pallet = False
    try:
        with get_db_connection() as conn:
            row = conn.execute("SELECT is_pallet FROM materiali WHERE id=?", (material_id,)).fetchone()
            if row and row[0] is not None:
                try:
                    is_pallet = bool(int(row[0]))
                except Exception:
                    is_pallet = False
    except Exception:
        # Ignora eventuali errori di accesso al database: trattiamo come non bancale
        is_pallet = False

    # Imposta la risoluzione della stampante (203 dpi) e calcola i punti per mm
    dpi = 203
    dots_per_mm = dpi / 25.4
    # Dimensioni fisiche dell'etichetta (38×22 mm)
    label_width_mm = 38
    label_height_mm = 22
    label_width_px = int(label_width_mm * dots_per_mm)
    label_height_px = int(label_height_mm * dots_per_mm)
    # Margine di sicurezza di ~2 mm sui bordi
    margin_px = int(2 * dots_per_mm)
    data_str = str(material_id)

    # Se il materiale è un bancale stampiamo solo il numero
    if is_pallet:
        # Calcola l'area disponibile sottraendo i margini
        available_width = label_width_px - (2 * margin_px)
        available_height = label_height_px - (2 * margin_px)
        # Determina le dimensioni del carattere: la massima altezza disponibile
        font_height = available_height
        # Larghezza per carattere calcolata dividendo lo spazio orizzontale per il numero di cifre
        # Se len(data_str) è zero (non dovrebbe accadere) usa 1 per evitare divisione per zero
        char_count = max(len(data_str), 1)
        font_width = max(int(available_width / char_count), 1)
        # Calcola la posizione X per centrare orizzontalmente il testo
        text_total_width = font_width * char_count
        x_text = margin_px + max((available_width - text_total_width) // 2, 0)
        y_text = margin_px  # posizionato in alto, allineato al margine superiore
        # Costruisce la stringa ZPL senza QR code
        zpl_lines: list[str] = []
        zpl_lines.append('^XA')
        zpl_lines.append(f'^PW{label_width_px}')
        zpl_lines.append(f'^LL{label_height_px}')
        zpl_lines.append('^LH0,0')
        zpl_lines.append(f'^FO{x_text},{y_text}^A0N,{font_height},{font_width}^FD{data_str}^FS')
        zpl_lines.append('^XZ')
        return '\n'.join(zpl_lines)

    # Per lastre e sfridi continua con la stampa standard QR+testo
    # Parametri del codice QR (moduli medi per un numero breve)
    approx_qr_modules = 21
    module_size = 6  # dimensione del modulo in punti
    qr_size_px = module_size * approx_qr_modules

    # Dimensione carattere di default per il testo
    font_height = 60
    font_width = 20
    text_width_px = font_width * len(data_str)

    # Calcola la posizione predefinita del testo accanto al QR code
    x_qr = margin_px
    y_qr = margin_px
    x_text = x_qr + qr_size_px + margin_px
    y_text = max(margin_px, (label_height_px - font_height) // 2)

    # Se il testo non entra nello spazio rimanente, posizionalo sotto il QR code
    if x_text + text_width_px > (label_width_px - margin_px):
        x_text = margin_px
        y_text = y_qr + qr_size_px + margin_px
        available_vertical_space = label_height_px - y_text - margin_px
        if available_vertical_space < font_height:
            scale_factor = max(available_vertical_space, 8) / font_height
            font_height = int(font_height * scale_factor)
            font_width = int(font_width * scale_factor)

    # Costruisci la stringa ZPL standard con QR code e testo
    zpl_lines: list[str] = []
    zpl_lines.append('^XA')
    zpl_lines.append(f'^PW{label_width_px}')
    zpl_lines.append(f'^LL{label_height_px}')
    zpl_lines.append('^LH0,0')
    zpl_lines.append(f'^FO{x_qr},{y_qr}^BQN,2,{module_size}^FDLA,{data_str}^FS')
    zpl_lines.append(f'^FO{x_text},{y_text}^A0N,{font_height},{font_width}^FD{data_str}^FS')
    zpl_lines.append('^XZ')
    return '\n'.join(zpl_lines)

def _print_label_to_zebra(material_id: int) -> bool:
    """Invia la stampa di un'etichetta per un singolo materiale alla stampante Zebra.

    Legge le impostazioni di connessione e costruisce la stringa ZPL
    utilizzando ``_build_zpl_for_id``.  In caso di errori di connessione
    la funzione restituisce False.  La stampa non viene ritentata in caso
    di fallimento.

    :param material_id: identificatore del materiale
    :return: True se l'etichetta è stata inviata con successo, False altrimenti
    """
    host, port = load_zebra_config()
    zpl = _build_zpl_for_id(material_id)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect((host, port))
            # La stampante Zebra accetta dati ASCII; codifichiamo la
            # stringa ZPL in ASCII e inviamo tutti i byte.
            sock.sendall(zpl.encode('ascii'))
        return True
    except Exception:
        # Silenziosamente fallisce in caso di problemi di rete.
        return False

# Nome delle tabelle supplementari introdotte nelle versioni recenti.
# ``RIORDINO_SOGGIE_TABLE`` memorizza per ogni combinazione di materiale e
# spessore la soglia di riordino (quantità alla quale scatta l'avviso).
# ``PRENOTAZIONI_TABLE`` mantiene l'elenco delle prenotazioni attive
# effettuate dagli utenti sulla pagina "Magazzino Live". Ogni prenotazione
# associa una lastra al tempo entro cui deve essere prelevata.
RIORDINO_SOGGIE_TABLE = "riordino_soglie"
PRENOTAZIONI_TABLE = "prenotazioni"

# Tabella per la gestione degli utenti che accedono all'applicazione.
# Ogni utente ha un nome univoco, una password (hash) e una lista di
# tab che può visualizzare.  Questa tabella viene creata durante
# l'inizializzazione del database (``init_db``).  Per motivi di
# retro‑compatibilità, se la tabella è vuota dopo la creazione viene
# inserito un utente di default (``admin``) con password ``admin`` e
# accesso a tutte le pagine dell'applicazione.  È consigliato
# modificare queste credenziali dopo il primo avvio tramite la pagina
# "Accessi".
USERS_TABLE = "users"

# ----------------------------------------------------------------------
# Configurazione soglie per avviso e riordino
#
# È possibile modificare le soglie di quantità che determinano quando
# un materiale deve essere segnalato come a scorte basse (avviso) o quando
# è consigliato procedere con il riordino (riordino).  I valori di default
# vengono definiti qui sotto e possono essere sovrascritti inserendo le
# chiavi ``avviso`` e ``riordino`` nel file ``thresholds.txt`` presente
# nella directory dell'applicazione.  Il file ha una sintassi semplice
# ``chiave=valore`` con eventuali righe di commento che iniziano con ``#``.
# Esempio di thresholds.txt:
#    # Soglie personalizzate
#    avviso=3
#    riordino=5

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'thresholds.txt')
DEFAULT_REORDER_THRESHOLD = 5
DEFAULT_ALERT_THRESHOLD = 2

def load_thresholds() -> tuple[int, int]:
    """Legge le soglie di avviso e riordino dal file di configurazione.

    Restituisce una tupla ``(soglia_riordino, soglia_avviso)``.  Se il file
    non esiste o contiene valori non validi, vengono utilizzati i valori
    di default.  Le chiavi riconosciute sono ``riordino`` e ``avviso``.
    """
    reorder = DEFAULT_REORDER_THRESHOLD
    alert = DEFAULT_ALERT_THRESHOLD
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    # rimuove eventuali spazi e commenti
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' not in line:
                        continue
                    key, value = line.split('=', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    try:
                        num = int(value)
                    except ValueError:
                        continue
                    if key == 'riordino':
                        reorder = num
                    elif key == 'avviso':
                        alert = num
    except Exception:
        # Eventuali errori vengono ignorati e si usano le soglie di default
        pass
    return reorder, alert

# ---------------------------------------------------------------------------
# Funzioni di supporto per le soglie di riordino per materiale/spessore

def get_reorder_threshold(materiale: str, tipo: str, spessore: str) -> int:
    """Restituisce la soglia di riordino per una combinazione materiale/tipo/spessore.

    A partire da questa versione la soglia viene definita sulla terna
    ``materiale``, ``tipo`` e ``spessore``. Se una soglia specifica non è
    presente nella tabella dedicata, viene restituito
    ``DEFAULT_REORDER_THRESHOLD``.  Il database può contenere righe con
    ``tipo`` o ``spessore`` NULL o stringa vuota; questi valori vengono
    normalizzati a stringa vuota per garantire confronti consistenti.
    """
    # Normalizziamo i valori: None -> stringa vuota
    sp = spessore or ''
    tp = tipo or ''
    with get_db_connection() as conn:
        try:
            row = conn.execute(
                f"SELECT threshold FROM {RIORDINO_SOGGIE_TABLE} WHERE materiale=? AND tipo=? AND spessore=?",
                (materiale, tp, sp)
            ).fetchone()
            if row and row['threshold'] is not None:
                try:
                    return int(row['threshold'])
                except (ValueError, TypeError):
                    pass
        except sqlite3.Error:
            # In caso di errore (ad esempio la colonna 'tipo' non esiste ancora)
            # ritorniamo la soglia di default
            pass
    return DEFAULT_REORDER_THRESHOLD

# Carichiamo le soglie all'avvio dell'applicazione.  Verranno rilette ad
# ogni richiesta della dashboard per consentire modifiche runtime senza
# riavviare l'app.
REORDER_THRESHOLD, ALERT_THRESHOLD = load_thresholds()

# Name of the table that stores the vocabulary of available materials.  This
# dictionary allows users to add new materials on the fly without
# modifying the source code.  The ``init_db`` function will create this
# table if it does not exist and seed it with a set of sensible default
# values defined below.
VOCAB_TABLE = "materiali_vocabolario"

# Name of the table that stores the vocabulary of suppliers (fornitori).  As
# with the materials dictionary, users can manage this list via a dedicated
# page in the web interface.  Suppliers stored here will populate the
# drop‑down menus on the dashboard and in the insertion/modification
# forms.
SUPPLIER_TABLE = "fornitori_vocabolario"

# Tabella del vocabolario dei produttori.  Ogni produttore rappresenta
# l'azienda o la cava da cui proviene il materiale.  Gli utenti
# possono gestire liberamente questa lista tramite l'apposita sezione
# nella pagina "Anagrafiche articoli".  Viene creata da ``init_db``
# se non esiste.  Sarà usata per popolare il campo "produttore" dei
# materiali nella schermata di inserimento e per filtrare la
# dashboard.
PRODUTTORE_TABLE = "produttori_vocabolario"

# Elenco di produttori predefiniti utilizzati per inizializzare il
# dizionario quando la tabella viene creata ed è vuota.  Questi
# valori vengono inseriti una sola volta e possono essere rimossi o
# modificati dall'utente tramite l'interfaccia.  L'elenco può essere
# esteso con nomi comuni di produttori.
DEFAULT_PRODUTTORI: list[str] = [
    'Produttore Generico',
]

# Tabella del vocabolario dei tipi di lavorazione/materiale.  Gli utenti
# possono inserire valori come "fresato", "laminato" ecc. attraverso la
# pagina del dizionario.  Ciascun tipo ha un nome univoco.  Questa
# tabella viene creata da ``init_db`` se non esiste.  Sarà usata per
# popolare il campo "tipo" dei materiali nella schermata di inserimento.
TIPO_TABLE = "tipi_vocabolario"

# Tabella del vocabolario delle macchine.  Ogni macchina rappresenta un
# impianto o linea dove verrà lavorato un materiale prenotato.  Le
# macchine vengono gestite tramite l'apposita pagina "Dizionario
# macchine".  Anche questa tabella viene creata da ``init_db``.
MACCHINE_TABLE = "macchine_vocabolario"

# Default suppliers used to seed the supplier vocabulary when the table is
# created or is empty.  These values are only inserted once and can be
# modified or removed later through the dictionary management page.  Feel
# free to adjust or extend this list with common supplier names used by
# your company.
DEFAULT_FORNITORI: list[str] = [
    'Fornitore Generico',
    'Marmitalia',
    'Stone Co.',
]

# Default materials used to seed the vocabulary.  These values are only
# inserted into the vocabulary when the table is first created or is empty.
DEFAULT_MATERIALI = [
    'Marmo',
    'Granito',
    'Travertino',
    'Onice',
    'Alabastro',
    'Basalto',
    'Porfido',
    'Quarzite'
]

# Flag che determina se i dizionari predefiniti devono essere popolati al
# primo avvio del database.  Impostando questo valore a False si
# evita che vengano inseriti valori come "Fornitore Generico" o
# "Marmitalia" nei menu a tendina.  Le liste DEFAULT_* rimangono
# definite affinché possano essere riutilizzate in futuro, ma non
# verranno inserite se SEED_DEFAULTS è impostato a False.
SEED_DEFAULTS: bool = False

# Cartella dove vengono salvati i documenti caricati per i materiali.
# Viene creata automaticamente se non esiste.  È posta fuori dalla
# directory dei template per evitare conflitti con il caricamento di
# file statici tramite Flask.  Per servire i file utilizziamo una
# route dedicata più avanti.
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
ALLOWED_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.pdf'}
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def get_db_connection():
    """Restituisce una connessione al database SQLite con factory su Row e applica PRAGMA per performance.

    Questa funzione configura la connessione con modalità WAL, sincronizzazione NORMAL,
    memorizzazione temporanea in memoria e abilita le chiavi esterne.  Queste impostazioni
    migliorano le prestazioni complessive dell'applicazione riducendo il tempo di scrittura
    e garantendo al contempo l'integrità dei dati.
    """
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    try:
        # Modalità WAL per scritture concorrenti e migliore throughput.
        conn.execute("PRAGMA journal_mode=WAL")
        # Sincronia NORMAL: compromesso tra durabilità e performance.
        conn.execute("PRAGMA synchronous=NORMAL")
        # Archivia temporanei in memoria per operazioni più veloci.
        conn.execute("PRAGMA temp_store=MEMORY")
        # Abilita le chiavi esterne in SQLite per integrità referenziale.
        conn.execute("PRAGMA foreign_keys=ON")
    except Exception:
        # Se un PRAGMA fallisce ignoriamo l'errore per mantenere retrocompatibilità.
        pass
    return conn

# ---------------------------------------------------------------------------
# Funzioni di supporto per lo storico lastre

def current_username_for_log() -> str:
    """
    Restituisce il nome utente attuale per la registrazione degli eventi.
    Se l'utente non è autenticato (ad esempio durante operazioni automatiche),
    viene restituito 'sistema'.
    """
    try:
        return session.get('username') or 'sistema'
    except Exception:
        return 'sistema'


def log_slab_event(
    slab_id: int,
    event_type: str,
    *,
    from_letter: str | None = None,
    from_number: int | None = None,
    to_letter: str | None = None,
    to_number: int | None = None,
    dimensione_x: str | None = None,
    dimensione_y: str | None = None,
    spessore: str | None = None,
    materiale: str | None = None,
    tipo: str | None = None,
    fornitore: str | None = None,
    produttore: str | None = None,
    note: str | None = None,
    nesting_link: str | None = None,
) -> None:
    """
    Registra un evento nello storico delle lastre.

    Ogni evento è identificato dal tipo (aggiunto, spostato, rimosso, sfrido) e
    raccoglie informazioni sull'ubicazione precedente e successiva, l'utente che
    ha eseguito l'operazione e le proprietà del materiale al momento
    dell'evento (dimensioni, spessore, materiale, tipo, fornitore, produttore).
    """
    ts = datetime.now().isoformat(sep=' ', timespec='seconds')
    user = current_username_for_log()
    try:
        with get_db_connection() as conn:
            conn.execute(
                "INSERT INTO slab_history (slab_id, event_type, timestamp, user, "
                "from_letter, from_number, to_letter, to_number, "
                "dimensione_x, dimensione_y, spessore, materiale, tipo, fornitore, produttore, note, nesting_link) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    slab_id,
                    event_type,
                    ts,
                    user,
                    from_letter,
                    from_number,
                    to_letter,
                    to_number,
                    dimensione_x,
                    dimensione_y,
                    spessore,
                    materiale,
                    tipo,
                    fornitore,
                    produttore,
                    note,
                    nesting_link,
                ),
            )
            conn.commit()
    except Exception:
        # In caso di errore nella registrazione ignoriamo l'eccezione per non interrompere il flusso principale
        pass

def log_slab_events(events: list[dict]) -> None:
    """
    Registra più eventi nello storico lastre in un'unica transazione.

    Ogni elemento di ``events`` deve essere un dizionario con le stesse chiavi previste da ``log_slab_event``:
      - ``slab_id``: int
      - ``event_type``: str
      - ``from_letter``, ``from_number``, ``to_letter``, ``to_number``
      - ``dimensione_x``, ``dimensione_y``, ``spessore``, ``materiale``, ``tipo``, ``fornitore``, ``produttore``, ``note``, ``nesting_link``

    Se una chiave è assente verrà interpretata come ``None``.  La colonna ``timestamp`` verrà
    valorizzata automaticamente con l'istante corrente per ogni evento.  L'utente viene determinato
    una volta sola per tutti gli eventi.
    """
    if not events:
        return
    user = current_username_for_log()
    now = datetime.now
    rows: list[tuple] = []
    for ev in events:
        ts = now().isoformat(sep=' ', timespec='seconds')
        rows.append((
            ev.get('slab_id'),
            ev.get('event_type'),
            ts,
            user,
            ev.get('from_letter'),
            ev.get('from_number'),
            ev.get('to_letter'),
            ev.get('to_number'),
            ev.get('dimensione_x'),
            ev.get('dimensione_y'),
            ev.get('spessore'),
            ev.get('materiale'),
            ev.get('tipo'),
            ev.get('fornitore'),
            ev.get('produttore'),
            ev.get('note'),
            ev.get('nesting_link'),
        ))
    try:
        with get_db_connection() as conn:
            conn.executemany(
                "INSERT INTO slab_history (slab_id, event_type, timestamp, user, "
                "from_letter, from_number, to_letter, to_number, "
                "dimensione_x, dimensione_y, spessore, materiale, tipo, fornitore, produttore, note, nesting_link) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
    except Exception:
        # Se c'è un errore nella registrazione non interrompiamo il flusso principale
        pass

# ---------------------------------------------------------------------------
# Prenotazioni utilities

def get_reserved_material_ids() -> set[int]:
    """
    Recupera l'insieme degli ID materiale attualmente prenotati.

    Effettua una semplice query sulla tabella delle prenotazioni per
    estrarre tutti i ``material_id`` presenti.  Poiché la tabella delle
    prenotazioni può contenere più record per lo stesso materiale
    (ad esempio in seguito ad estensioni future), la funzione restituisce
    un set per eliminare eventuali duplicati.  In caso di qualsiasi
    errore nella lettura del database l'insieme restituito sarà vuoto.
    """
    ids: set[int] = set()
    try:
        with get_db_connection() as conn:
            # Recupera sia l'ID della lastra prenotata che il flag generico.  In
            # presenza di una prenotazione generica includiamo anche tutte le
            # lastre figlie dello stesso bancale per evitare prenotazioni multiple.
            try:
                rows = conn.execute(f"SELECT material_id, is_generic FROM {PRENOTAZIONI_TABLE}").fetchall()
            except Exception:
                rows = []
            for r in rows:
                try:
                    mat_id = r['material_id'] if isinstance(r, sqlite3.Row) else r[0]
                except Exception:
                    try:
                        mat_id = r[0]
                    except Exception:
                        continue
                try:
                    is_gen = r['is_generic'] if isinstance(r, sqlite3.Row) else r[1]
                except Exception:
                    try:
                        is_gen = r[1]
                    except Exception:
                        is_gen = 0
                try:
                    mat_id_int = int(mat_id)
                except Exception:
                    continue
                # Per ciascuna prenotazione includiamo sempre e solo l'ID della lastra prenotata.
                # Se la prenotazione è generica non disabilitiamo le altre lastre del bancale,
                # quindi non includiamo le lastre sorelle nel set degli ID riservati.
                ids.add(mat_id_int)
    except Exception:
        # In caso di errore restituire set vuoto
        return set()
    return ids

# ---------------------------------------------------------------------------
# Funzioni di supporto per i documenti

def allowed_file(filename: str) -> bool:
    """Controlla se il file ha un'estensione consentita.

    Accettiamo immagini (PNG, JPG, JPEG) e documenti PDF. L'estensione
    viene confrontata in modo case-insensitive con l'insieme
    ``ALLOWED_EXTENSIONS`` definito in cima al modulo.
    """
    if not filename:
        return False
    _, ext = os.path.splitext(filename)
    return ext.lower() in ALLOWED_EXTENSIONS

def get_attachments(material_id: int) -> list[sqlite3.Row]:
    """Restituisce l'elenco dei documenti associati a un materiale.

    Ciascun elemento è una riga della tabella ``documenti`` con le
    colonne ``id``, ``filename`` e ``original_name``.  In caso di
    problemi di database viene restituita una lista vuota.
    """
    with get_db_connection() as conn:
        try:
            cur = conn.execute(
                "SELECT id, filename, original_name FROM documenti WHERE material_id=? ORDER BY id",
                (material_id,)
            )
            return cur.fetchall()
        except Exception:
            return []

# ------------------------------------------------------------
# Helper functions for saving uploaded documents

def save_upload_file(f):
    """
    Salva un file caricato tramite il pulsante DOCS nella sezione
    combinazioni articoli (upload_docs_combo).  Restituisce il
    percorso relativo (safe_name) e il nome originale.  I file
    vengono organizzati in sottocartelle per data all'interno
    della directory 'combo'.  Una volta salvato il file viene
    restituito un percorso relativo da inserire nel DB.

    :param f: l'oggetto FileStorage proveniente da Flask request
    :raises ValueError: se il file è mancante o l'estensione non è consentita
    :return: (safe_name, orig_name)
    """
    if not f or not getattr(f, 'filename', None):
        raise ValueError("File non valido")
    orig_name = f.filename
    # Utilizza secure_filename per evitare path traversal
    try:
        safe_orig = secure_filename(os.path.basename(orig_name))
    except Exception:
        safe_orig = orig_name
    # Verifica estensione consentita
    if not allowed_file(safe_orig):
        raise ValueError("Estensione non permessa")
    # Estrae l'estensione e normalizza in minuscolo
    _, ext = os.path.splitext(safe_orig)
    ext = ext.lower()
    # Crea la directory combo/<data> se non esiste
    date_str = datetime.now().strftime('%Y%m%d')
    dest_dir = os.path.join(UPLOAD_FOLDER, 'combo', date_str)
    try:
        os.makedirs(dest_dir, exist_ok=True)
    except Exception:
        # Se la creazione della directory fallisce solleva un errore
        raise
    # Genera un nome univoco
    unique_name = f"{uuid4().hex}{ext}"
    safe_name = f"combo/{date_str}/{unique_name}"
    dest_path = os.path.join(dest_dir, unique_name)
    # Legge il contenuto del file e scrive sul disco
    try:
        data = f.read()
    except Exception:
        raise
    with open(dest_path, 'wb') as out_f:
        out_f.write(data)
    return safe_name, safe_orig


def save_file_to_id(content_bytes: bytes, ext: str, target_id: int, doc_type: str = 'pallet') -> str:
    """
    Salva un file associato ad un ID di materiale creando una struttura a
    cartelle del tipo:

        uploads/<ID>/Documenti_pallet/<data>/<uuid><ext>
        uploads/<ID>/Documenti_materiale/<data>/<uuid><ext>

    L'argomento ``doc_type`` può assumere i valori ``'pallet'`` oppure
    ``'materiale'`` per determinare quale sottodirectory utilizzare.

    :param content_bytes: contenuto binario del file
    :param ext: estensione del file (incluso il punto), in minuscolo
    :param target_id: ID della lastra su cui salvare il documento
    :param doc_type: 'pallet' per documenti del bancale, 'materiale' per documenti specifici della lastra
    :return: percorso relativo da registrare nel database
    """
    date_str = datetime.now().strftime('%Y%m%d')
    subfolder = 'Documenti_pallet' if doc_type == 'pallet' else 'Documenti_materiale'
    # Directory di destinazione assoluta
    base_dir = os.path.join(UPLOAD_FOLDER, str(target_id), subfolder, date_str)
    try:
        os.makedirs(base_dir, exist_ok=True)
    except Exception:
        pass
    # Genera un nome univoco
    unique_name = f"{uuid4().hex}{ext}"
    # Costruisci il percorso relativo
    relative_path = os.path.join(str(target_id), subfolder, date_str, unique_name)
    # Normalizza per sistemi Windows sostituendo backslash con slash
    relative_path = relative_path.replace('\\', '/')
    dest_path = os.path.join(base_dir, unique_name)
    # Scrive il contenuto sul disco
    with open(dest_path, 'wb') as out_f:
        out_f.write(content_bytes)
    return relative_path


def init_db():
    """Initialise the SQLite database and evolve its schema as needed.

    Beyond creating the base ``materiali`` table, this function also checks
    whether new columns introduced in later versions of the application are
    present. If a column is missing it will be added on the fly via
    ``ALTER TABLE``. This approach preserves existing data while enabling
    incremental feature development (e.g. separate X/Y dimensions, parent
    relationships, and scrap flags).
    """
    with get_db_connection() as conn:
        # Create the table if it doesn't exist. Older versions of the app had
        # fewer columns; new columns will be added below.
        conn.execute(
            'CREATE TABLE IF NOT EXISTS materiali ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'materiale TEXT NOT NULL,'
            'dimensioni TEXT,'
            'spessore TEXT,'
            'quantita INTEGER NOT NULL,'
            'ubicazione_lettera TEXT,'
            'ubicazione_numero INTEGER,'
            'fornitore TEXT,'
            'note TEXT'
            ')'
        )
        # Check existing columns and apply schema migrations if necessary.
        cur = conn.execute("PRAGMA table_info(materiali)")
        existing_cols = {row['name'] for row in cur.fetchall()}
        # Separate X and Y dimensions for each slab (lastra). Previously there
        # was a single ``dimensioni`` field; we continue to keep it for
        # backwards compatibility but store a formatted representation of
        # ``dimensione_x`` and ``dimensione_y`` when both are provided.
        if 'dimensione_x' not in existing_cols:
            conn.execute("ALTER TABLE materiali ADD COLUMN dimensione_x TEXT")
        if 'dimensione_y' not in existing_cols:
            conn.execute("ALTER TABLE materiali ADD COLUMN dimensione_y TEXT")
        # ``parent_id`` links a slab to its pallet; NULL for pallets or
        # independent slabs. When ``parent_id`` is NULL and ``is_pallet`` is
        # 1 the row represents a pallet.
        if 'parent_id' not in existing_cols:
            conn.execute("ALTER TABLE materiali ADD COLUMN parent_id INTEGER")
        # ``is_sfrido`` flags scrap pieces (sfridi). 0 = no, 1 = yes.
        if 'is_sfrido' not in existing_cols:
            conn.execute("ALTER TABLE materiali ADD COLUMN is_sfrido INTEGER DEFAULT 0")
        # ``is_pallet`` distinguishes pallets from slabs. Pallets contain
        # multiple slabs and should not be removed via QR scanning.
        if 'is_pallet' not in existing_cols:
            conn.execute("ALTER TABLE materiali ADD COLUMN is_pallet INTEGER DEFAULT 0")
        # ``tipo`` stores the processing or material type (e.g. fresato,
        # laminato).  If it does not exist yet we add it.  Existing
        # installations prior to this update will have this column
        # appended without data loss.
        if 'tipo' not in existing_cols:
            conn.execute("ALTER TABLE materiali ADD COLUMN tipo TEXT")
        # ``produttore`` stores the producer or quarry of the material.  If it
        # does not exist we add it.  This field is optional and can be
        # filtered via the dashboard.  Older databases will be migrated
        # automatically without losing data.
        if 'produttore' not in existing_cols:
            conn.execute("ALTER TABLE materiali ADD COLUMN produttore TEXT")
        conn.commit()

        # ------------------------------------------------------------------
        # Tabella storico lastre
        #
        # Questa tabella tiene traccia di tutte le operazioni effettuate sulle
        # singole lastre (non sui bancali).  Ogni riga rappresenta un evento
        # associato ad una lastra specifica.  I campi ``from_letter`` e
        # ``from_number`` indicano l'ubicazione precedente; ``to_letter`` e
        # ``to_number`` indicano quella successiva.  I campi dimensione_x,
        # dimensione_y, spessore, materiale, tipo, fornitore e produttore
        # registrano lo stato del materiale al momento dell'evento.  ``note``
        # può contenere eventuali annotazioni aggiuntive e ``nesting_link``
        # sarà utilizzato in futuro per collegare il nesting.
        conn.execute(
            'CREATE TABLE IF NOT EXISTS slab_history ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'slab_id INTEGER NOT NULL,'
            'event_type TEXT NOT NULL,'
            'timestamp TEXT NOT NULL,'
            'user TEXT,'
            'from_letter TEXT,'
            'from_number INTEGER,'
            'to_letter TEXT,'
            'to_number INTEGER,'
            'dimensione_x TEXT,'
            'dimensione_y TEXT,'
            'spessore TEXT,'
            'materiale TEXT,'
            'tipo TEXT,'
            'fornitore TEXT,'
            'produttore TEXT,'
            'note TEXT,'
            'nesting_link TEXT'
            ')'
        )
        # Indici per performance: uno per slab_id e uno per data/ora
        conn.execute("CREATE INDEX IF NOT EXISTS idx_slab_history_slab ON slab_history (slab_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_slab_history_ts ON slab_history (timestamp)")
        # Aggiungiamo un indice sul tipo di evento per velocizzare i filtri
        conn.execute("CREATE INDEX IF NOT EXISTS idx_slab_history_event_type ON slab_history (event_type)")
        # Indici aggiuntivi per la tabella materiali per migliorare le performance delle query di ricerca e spostamento.
        # Permettono di recuperare rapidamente le lastre figlie di un bancale, filtrare per bancali
        # e cercare per ubicazione o combinazione materiale/tipo/spessore.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_materiali_parent ON materiali (parent_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_materiali_is_pallet ON materiali (is_pallet)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_materiali_location ON materiali (ubicazione_lettera, ubicazione_numero)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_materiali_type ON materiali (materiale, tipo, spessore)")
        conn.commit()

        # ------------------------------------------------------------------
        # Inizializzazione del database nascosto per gli ID univoci delle lastre
        #
        # Dopo aver configurato la struttura principale del database
        # ``magazzino.db``, inizializziamo anche il database
        # ``.slab_ids.db`` che servirà a tenere traccia di tutti gli
        # identificativi assegnati alle lastre.  La funzione
        # ``init_id_db`` crea la tabella necessaria se non esiste già.
        try:
            init_id_db()
        except Exception:
            # Se l'inizializzazione del DB nascosto fallisce non
            # interrompiamo il processo di setup del database
            pass

        # ------------------------------------------------------------------
        # Schema per la gestione dei riordini multi‑fase
        #
        # A supporto della nuova pagina "riordini" riorganizzata in tre
        # sezioni (Da gestire → RDO → Accettazione), introduciamo una
        # tabella intermedia ``riordini_rdo`` dove vengono salvati i
        # riordini preparati ma non ancora confermati.  Ogni riga
        # rappresenta una combinazione di materiale da riordinare con la
        # quantità richiesta, l’elenco dei fornitori e produttori
        # dedotti/selezionati e lo stato di blocco delle scelte.  Dopo che
        # l’utente conferma la riga in fase di RDO, i record vengono
        # trasferiti nella tabella ``riordini_accettazione`` (vedi
        # ``confirm_rdo``) e rimossi da ``riordini_rdo``.  La colonna
        # ``data_prevista`` contiene una data di consegna stimata; se
        # l’utente specifica più date, esse vengono archiviate nella
        # tabella secondaria ``rdo_dates``.
        try:
            conn.execute(
                'CREATE TABLE IF NOT EXISTS riordini_rdo ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT,'
                'data TEXT NOT NULL,'
                'materiale TEXT,'
                'tipo TEXT,'
                'spessore TEXT,'
                'dimensione_x TEXT,'
                'dimensione_y TEXT,'
                'quantita INTEGER NOT NULL,'
                'fornitori TEXT,'
                'fornitore_scelto TEXT,'
                'locked_forn INTEGER DEFAULT 0,'
                'produttori TEXT,'
                'produttore_scelto TEXT,'
                'locked_prod INTEGER DEFAULT 0,'
                'numero_ordine TEXT,'
                'data_prevista TEXT'
                ')'
            )
            conn.commit()
        except sqlite3.Error:
            pass

        # Tabella per memorizzare più date di consegna per ogni rdo.  Ogni
        # riga contiene l’identificatore del record RDO, una data e la
        # quantità associata a quella consegna.  Se non sono presenti
        # record in questa tabella per un determinato RDO, si utilizza
        # ``riordini_rdo.data_prevista`` come unica data di consegna.
        try:
            # La tabella ``rdo_dates`` contiene la lista di date previste per le consegne
            # con le relative quantità.  Aggiungiamo anche un campo ``produttore``
            # per associare un produttore specifico a ciascuna data.  Se la tabella
            # esiste già, i nuovi campi saranno aggiunti tramite ALTER TABLE.
            conn.execute(
                'CREATE TABLE IF NOT EXISTS rdo_dates ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT,'
                'rdo_id INTEGER,'
                'data_prevista TEXT,'
                'quantita INTEGER,'
                'produttore TEXT,'
                'FOREIGN KEY (rdo_id) REFERENCES riordini_rdo(id) ON DELETE CASCADE'
                ')'
            )
            conn.commit()
            # Aggiorna lo schema aggiungendo la colonna ``produttore`` se la tabella
            # esiste già senza questo campo.  Evita errore se già presente.
            try:
                rdo_cols = {row['name'] for row in conn.execute("PRAGMA table_info(rdo_dates)")}
                if 'produttore' not in rdo_cols:
                    conn.execute("ALTER TABLE rdo_dates ADD COLUMN produttore TEXT")
                    conn.commit()
            except sqlite3.Error:
                pass
        except sqlite3.Error:
            pass

        # Aggiorna lo schema della tabella ``riordini_accettazione`` per includere
        # i campi ``fornitore``, ``produttore`` e ``data_prevista``.  La
        # versione originaria della tabella non prevedeva questi campi, ma
        # la logica di riordino richiede di memorizzarli per ciascuna
        # combinazione.  Se la colonna non esiste, la aggiungiamo tramite
        # ALTER TABLE, mantenendo i dati esistenti intatti.
        try:
            acc_cols = {row['name'] for row in conn.execute("PRAGMA table_info(riordini_accettazione)")}
            # Aggiungi colonna ``fornitore`` se assente
            if 'fornitore' not in acc_cols:
                conn.execute("ALTER TABLE riordini_accettazione ADD COLUMN fornitore TEXT")
            # Aggiungi colonna ``produttore`` se assente
            if 'produttore' not in acc_cols:
                conn.execute("ALTER TABLE riordini_accettazione ADD COLUMN produttore TEXT")
            # Aggiungi colonna ``data_prevista`` se assente
            if 'data_prevista' not in acc_cols:
                conn.execute("ALTER TABLE riordini_accettazione ADD COLUMN data_prevista TEXT")
            # Aggiungi colonna ``ubicazione_lettera`` se assente per salvare la prima ubicazione di accettazione
            if 'ubicazione_lettera' not in acc_cols:
                conn.execute("ALTER TABLE riordini_accettazione ADD COLUMN ubicazione_lettera TEXT")
            # Aggiungi colonna ``ubicazione_numero`` se assente per salvare la prima ubicazione di accettazione
            if 'ubicazione_numero' not in acc_cols:
                conn.execute("ALTER TABLE riordini_accettazione ADD COLUMN ubicazione_numero INTEGER")
            conn.commit()
        except sqlite3.Error:
            pass

        # Aggiorna schema della tabella riordini_effettuati: aggiunge le
        # colonne ``data_prevista`` e ``produttore`` per memorizzare la
        # data di consegna prevista e il produttore associato all’evento.
        try:
            eff_cols = {row['name'] for row in conn.execute("PRAGMA table_info(riordini_effettuati)")}
            if 'data_prevista' not in eff_cols:
                conn.execute("ALTER TABLE riordini_effettuati ADD COLUMN data_prevista TEXT")
            if 'produttore' not in eff_cols:
                conn.execute("ALTER TABLE riordini_effettuati ADD COLUMN produttore TEXT")
            conn.commit()
        except sqlite3.Error:
            pass

        # Tabella ordine_produttori: analoga a ordine_fornitori ma per i produttori.
        # Contiene una riga per ogni ``numero_ordine`` con l’elenco dei
        # produttori coinvolti, il produttore scelto e un flag ``locked``
        # che indica se l’ordine è bloccato sulla scelta di un singolo
        # produttore.  La tabella viene creata su richiesta.
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ordine_produttori ("
                "numero_ordine TEXT PRIMARY KEY, "
                "produttori TEXT, "
                "produttore_scelto TEXT, "
                "locked INTEGER"
                ")"
            )
            conn.commit()
        except sqlite3.Error:
            pass

        # Popola lo storico con un evento iniziale per tutte le lastre già presenti.
        # In questo modo lo storico mostrerà subito le lastre esistenti anche se sono state caricate prima
        # dell'introduzione della funzionalità di storico. Viene registrato un evento 'aggiunto' per ogni lastra
        # che non è un bancale e non ha ancora eventi registrati. Se la lastra è già marcata come sfrido,
        # viene aggiunto anche un evento 'sfrido'. L'utente è impostato a 'sistema' per distinguere eventi generati
        # automaticamente.
        try:
            # Recupera tutte le lastre (inclusi eventuali sfridi) esistenti
            cur_existing = conn.execute(
                "SELECT id, ubicazione_lettera, ubicazione_numero, dimensione_x, dimensione_y, spessore, materiale, tipo, fornitore, produttore, note, is_sfrido, is_pallet FROM materiali"
            )
            existing_rows = cur_existing.fetchall()
            # Timestamp da utilizzare per tutti gli eventi di backfill; usare un timestamp unico
            now_ts = datetime.now().isoformat(sep=' ', timespec='seconds')
            for er in existing_rows:
                try:
                    is_pallet_val = int(er['is_pallet'] or 0)
                except Exception:
                    is_pallet_val = 0
                if is_pallet_val == 1:
                    # Salta i bancali, lo storico è solo per le lastre
                    continue
                slab_id = er['id']
                # Controlla se esistono già eventi per questa lastra
                try:
                    cur_cnt = conn.execute("SELECT COUNT(*) AS cnt FROM slab_history WHERE slab_id=?", (slab_id,))
                    row_cnt = cur_cnt.fetchone()
                    # row_cnt può essere un dict o una tuple; estrai correttamente il valore
                    if row_cnt is None:
                        cnt_val = 0
                    elif isinstance(row_cnt, dict) and 'cnt' in row_cnt:
                        cnt_val = row_cnt['cnt']
                    else:
                        cnt_val = row_cnt[0]
                except Exception:
                    cnt_val = 0
                if not cnt_val:
                    # Registra evento 'aggiunto'
                    try:
                        conn.execute(
                            "INSERT INTO slab_history (slab_id, event_type, timestamp, user, from_letter, from_number, to_letter, to_number, dimensione_x, dimensione_y, spessore, materiale, tipo, fornitore, produttore, note, nesting_link) "
                            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                slab_id,
                                'aggiunto',
                                now_ts,
                                'sistema',
                                None,
                                None,
                                er['ubicazione_lettera'],
                                er['ubicazione_numero'],
                                er['dimensione_x'],
                                er['dimensione_y'],
                                er['spessore'],
                                er['materiale'],
                                er['tipo'],
                                er['fornitore'],
                                er['produttore'],
                                er['note'],
                                None
                            )
                        )
                    except Exception:
                        # Se l'inserimento fallisce, ignoriamo per non interrompere l'inizializzazione
                        pass
                    # Se la lastra è marcata come sfrido, registra anche evento 'sfrido'
                    try:
                        is_sfrido_val = int(er['is_sfrido'] or 0)
                    except Exception:
                        is_sfrido_val = 0
                    if is_sfrido_val == 1:
                        try:
                            conn.execute(
                                "INSERT INTO slab_history (slab_id, event_type, timestamp, user, from_letter, from_number, to_letter, to_number, dimensione_x, dimensione_y, spessore, materiale, tipo, fornitore, produttore, note, nesting_link) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    slab_id,
                                    'sfrido',
                                    now_ts,
                                    'sistema',
                                    er['ubicazione_lettera'],
                                    er['ubicazione_numero'],
                                    er['ubicazione_lettera'],
                                    er['ubicazione_numero'],
                                    er['dimensione_x'],
                                    er['dimensione_y'],
                                    er['spessore'],
                                    er['materiale'],
                                    er['tipo'],
                                    er['fornitore'],
                                    er['produttore'],
                                    er['note'],
                                    None
                                )
                            )
                        except Exception:
                            pass
            conn.commit()
        except Exception:
            # In caso di errore durante la popolazione dello storico iniziale ignoriamo l'eccezione
            pass

        # ------------------------------------------------------------------
        # Tabella utenti per login e permessi
        #
        # Questa tabella permette di gestire gli utenti dell'applicazione
        # associando ad ognuno una password hash e una lista di tab
        # (pagine) che può visualizzare. Se la tabella viene creata per
        # la prima volta ed è vuota, inseriamo un utente di default
        # ``admin`` con password ``admin`` e accesso completo a tutte
        # le funzioni. Questo consente di effettuare il primo accesso e
        # aggiungere ulteriori utenti tramite l'interfaccia "Accessi".
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {USERS_TABLE} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'username TEXT NOT NULL UNIQUE,'
            'password TEXT NOT NULL,'
            'tabs TEXT NOT NULL'
            ')'
        )
        # Dopo la creazione della tabella utenti assicuriamo la presenza della colonna "ordine_template".
        # Questa colonna memorizza un testo predefinito per la generazione delle email di riordino.
        try:
            _usr_cols = conn.execute(f"PRAGMA table_info({USERS_TABLE})").fetchall()
            _usr_col_names = {c['name'] for c in _usr_cols}
            if 'ordine_template' not in _usr_col_names:
                conn.execute(f"ALTER TABLE {USERS_TABLE} ADD COLUMN ordine_template TEXT")
                conn.commit()
        except sqlite3.Error:
            pass
        # Se non esistono ancora utenti, creiamo l'account di default
        try:
            cur_users = conn.execute(f"SELECT COUNT(*) as cnt FROM {USERS_TABLE}")
            row_users = cur_users.fetchone()
            if row_users and row_users[0] == 0:
                # Includiamo la nuova tab "storico" nel set di tab di default per l'utente amministratore.
                default_tabs = "dashboard,add,scan,dizionario,config,riordini,live,accessi,storico"
                conn.execute(
                    f"INSERT INTO {USERS_TABLE} (username, password, tabs) VALUES (?,?,?)",
                    ('admin', generate_password_hash('admin'), default_tabs)
                )
                conn.commit()
        except Exception:
            # In caso di errore nell'inserimento lasciamo la tabella vuota
            pass

        # ------------------------------------------------------------------
        # Tabella anagrafica articoli
        #
        # A partire da questa versione supportiamo la definizione manuale delle
        # combinazioni di articolo tramite la pagina "Anagrafiche articoli".
        # L'anagrafica articoli consente all'utente di specificare una terna
        # (materiale, tipo, spessore) e le dimensioni X/Y associate senza
        # necessariamente inserire materiali reali nel magazzino.  Queste
        # combinazioni vengono utilizzate per la configurazione delle soglie
        # di riordino e per la pagina "Articoli preferiti".  La tabella
        # contiene un identificatore univoco e non implementa alcun vincolo
        # sulle colonne per consentire l'inserimento anche di valori parziali
        # (ad esempio solo materiale e spessore).  La combinazione completa
        # può essere considerata unica tramite un vincolo esplicito se
        # necessario in futuro.
        conn.execute(
            'CREATE TABLE IF NOT EXISTS articoli_catalogo ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'materiale TEXT NOT NULL,'
            'tipo TEXT,'
            'spessore TEXT,'
            'dimensione_x TEXT,'
            'dimensione_y TEXT'
            ')'
        )
        conn.commit()

        # Aggiungiamo eventuali colonne mancanti alla tabella ``articoli_catalogo``.
        # Oltre al flag ``preferito``, a partire da questa versione ogni articolo
        # include anche il produttore.  Per garantire la retro‑compatibilità con
        # database esistenti, verifichiamo la presenza delle colonne e le
        # aggiungiamo se necessario tramite ALTER TABLE.  Se la tabella non
        # esiste ancora l'operazione fallisce silenziosamente.
        try:
            cur_pref = conn.execute("PRAGMA table_info(articoli_catalogo)")
            existing_cols_art = {row['name'] for row in cur_pref.fetchall()}
            # Colonna per il flag di preferito
            if 'preferito' not in existing_cols_art:
                conn.execute("ALTER TABLE articoli_catalogo ADD COLUMN preferito INTEGER DEFAULT 0")
            # Colonna per il produttore associato alla combinazione
            if 'produttore' not in existing_cols_art:
                conn.execute("ALTER TABLE articoli_catalogo ADD COLUMN produttore TEXT")
            conn.commit()
        except sqlite3.Error:
            # Se la tabella non esiste ancora o si verifica un errore, ignoriamo.
            pass

        # ------------------------------------------------------------------
        # Dizionario materiali
        #
        # Crea una tabella separata per contenere il vocabolario dei
        # materiali. Ogni riga contiene un nome univoco. Questo
        # permette agli utenti di gestire autonomamente l'elenco dei
        # materiali senza dover modificare la variabile globale
        # ``MATERIALI``. La tabella viene popolata con i valori
        # predefiniti se è vuota.
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {VOCAB_TABLE} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'nome TEXT NOT NULL UNIQUE'
            ')'
        )
        # Dopo la creazione della tabella fornitori assicuriamoci che la colonna "email" esista.
        # Se manca (database aggiornato da versioni precedenti), la aggiungiamo per consentire
        # l'associazione di un indirizzo email a ciascun fornitore.  In caso di errore durante
        # la lettura dello schema o l'esecuzione dell'ALTER TABLE, l'eccezione viene ignorata.
        try:
            _sup_cols = conn.execute(f"PRAGMA table_info({SUPPLIER_TABLE})").fetchall()
            _sup_col_names = {c['name'] for c in _sup_cols}
            if 'email' not in _sup_col_names:
                conn.execute(f"ALTER TABLE {SUPPLIER_TABLE} ADD COLUMN email TEXT")
                conn.commit()
        except sqlite3.Error:
            pass
        # Se la tabella è vuota, inseriamo i materiali di default. Ciò
        # garantisce che all'avvio dell'applicazione l'utente abbia già
        # qualche materiale a disposizione da selezionare.
        cur = conn.execute(f"SELECT COUNT(*) as cnt FROM {VOCAB_TABLE}")
        row = cur.fetchone()
        # Se la tabella è vuota, inserisci i materiali di default solo
        # se SEED_DEFAULTS è attivo.  In ogni caso, i valori verranno
        # inseriti una sola volta durante la prima inizializzazione.
        if row and row['cnt'] == 0:
            if SEED_DEFAULTS:
                for mat in DEFAULT_MATERIALI:
                    conn.execute(f"INSERT INTO {VOCAB_TABLE} (nome) VALUES (?)", (mat,))
        conn.commit()

        # ------------------------------------------------------------------
        # Dizionario fornitori
        #
        # Crea una tabella separata per contenere il vocabolario dei
        # fornitori.  Ogni riga contiene un nome univoco.  Questo
        # permette agli utenti di gestire autonomamente l'elenco dei
        # fornitori senza dover modificare il codice.  La tabella viene
        # popolata con i fornitori di default se è vuota.  Per comodità,
        # quando la tabella viene creata per la prima volta si prova ad
        # estrarre un elenco distinti dei fornitori già presenti nella
        # tabella ``materiali`` per popolare automaticamente il vocabolario.
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {SUPPLIER_TABLE} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'nome TEXT NOT NULL UNIQUE'
            ')'
        )
        # Se la tabella è vuota, popoliamo con i valori di default e
        # importiamo eventuali fornitori esistenti dalla tabella materiali.
        cur = conn.execute(f"SELECT COUNT(*) as cnt FROM {SUPPLIER_TABLE}")
        row = cur.fetchone()
        if row and row['cnt'] == 0:
            # Inserisci fornitori di default solo se SEED_DEFAULTS è attivo
            if SEED_DEFAULTS:
                for fn in DEFAULT_FORNITORI:
                    conn.execute(f"INSERT OR IGNORE INTO {SUPPLIER_TABLE} (nome) VALUES (?)", (fn,))
            # Importa fornitori distinti dalla tabella materiali
            try:
                existing = conn.execute(
                    "SELECT DISTINCT fornitore FROM materiali WHERE fornitore IS NOT NULL AND TRIM(fornitore) != ''"
                ).fetchall()
                for r in existing:
                    conn.execute(f"INSERT OR IGNORE INTO {SUPPLIER_TABLE} (nome) VALUES (?)", (r['fornitore'],))
            except sqlite3.Error:
                pass
        conn.commit()

        # ------------------------------------------------------------------
        # Dizionario produttori
        #
        # Crea una tabella separata per contenere il vocabolario dei
        # produttori. Ogni riga contiene un nome univoco. Questo
        # permette agli utenti di gestire autonomamente l'elenco dei
        # produttori senza dover modificare il codice.  La tabella viene
        # popolata con eventuali produttori già presenti nella tabella
        # ``materiali`` se è vuota.  Come per i fornitori, se la tabella
        # viene creata per la prima volta importiamo anche un elenco di
        # produttori predefiniti.
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {PRODUTTORE_TABLE} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'nome TEXT NOT NULL UNIQUE'
            ')'
        )
        # Se la tabella è vuota, popoliamo con i valori di default e
        # importiamo eventuali produttori esistenti dalla tabella materiali.
        cur = conn.execute(f"SELECT COUNT(*) as cnt FROM {PRODUTTORE_TABLE}")
        row = cur.fetchone()
        if row and row['cnt'] == 0:
            # Inserisci produttori di default solo se SEED_DEFAULTS è attivo
            if SEED_DEFAULTS:
                for pr in DEFAULT_PRODUTTORI:
                    conn.execute(f"INSERT OR IGNORE INTO {PRODUTTORE_TABLE} (nome) VALUES (?)", (pr,))
            # Importa produttori distinti dalla tabella materiali
            try:
                existing_pr = conn.execute(
                    "SELECT DISTINCT produttore FROM materiali WHERE produttore IS NOT NULL AND TRIM(produttore) != ''"
                ).fetchall()
                for r in existing_pr:
                    conn.execute(f"INSERT OR IGNORE INTO {PRODUTTORE_TABLE} (nome) VALUES (?)", (r['produttore'],))
            except sqlite3.Error:
                pass
        conn.commit()

        # ------------------------------------------------------------------
        # Documenti associati ai materiali
        #
        # Questa tabella memorizza i file caricati (immagini o PDF) legati a
        # uno specifico materiale.  Ogni documento ha un identificatore
        # univoco e conserva il nome originale oltre al percorso locale
        # utilizzato internamente.  Definiamo la relazione di chiave
        # esterna con ``materiali`` per garantire che alla cancellazione di
        # un materiale vengano rimossi anche i relativi documenti.  Questo
        # vincolo ``ON DELETE CASCADE`` è supportato da SQLite a partire
        # dalla versione 3.6.19 se sono abilitate le foreign keys.  Nel
        # nostro caso non abilitiamo esplicitamente le foreign keys ma
        # ci occuperemo manualmente della rimozione nei percorsi di
        # eliminazione.
        conn.execute(
            'CREATE TABLE IF NOT EXISTS documenti ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'material_id INTEGER NOT NULL,'
            'filename TEXT NOT NULL,'
            'original_name TEXT NOT NULL,'
            'FOREIGN KEY(material_id) REFERENCES materiali(id)'
            ')'
        )
        conn.commit()

        # Estensione tabella 'documenti' per legare file alle anagrafiche articoli.
        # Aggiungiamo colonne per materiale, tipo, spessore, dimensioni e produttore se mancanti.
        try:
            existing_cols = {row['name'] for row in conn.execute("PRAGMA table_info(documenti)").fetchall()}
            # Definiamo l'elenco delle colonne da garantire sulla tabella documenti.
            # Le colonne vengono create come TEXT per coerenza con altri campi.
            required_cols = [
                ("materiale", "TEXT"),
                ("tipo", "TEXT"),
                ("spessore", "TEXT"),
                ("dimensione_x", "TEXT"),
                ("dimensione_y", "TEXT"),
                ("produttore", "TEXT"),
            ]
            for col, typ in required_cols:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE documenti ADD COLUMN {col} {typ}")
            conn.commit()
        except Exception:
            # In caso di errore nella verifica o alterazione della tabella documenti
            # (ad esempio se la tabella non esiste ancora) ignoriamo l'eccezione per
            # evitare di bloccare l'inizializzazione del database.
            pass


        # ------------------------------------------------------------------
        # Tabella soglie di riordino per materiale, tipo e spessore
        #
        # A partire da questa versione la tabella include anche il campo
        # ``tipo`` per permettere di definire soglie specifiche per ogni
        # combinazione di materiale, tipo e spessore.  Se il database
        # contiene una tabella preesistente con solo ``materiale`` e
        # ``spessore``, la eliminamo e la ricreiamo con il nuovo schema.
        # In questo modo le installazioni precedenti vengono migrate
        # automaticamente ma perdono eventuali soglie personalizzate che
        # potranno essere reinserite tramite l'interfaccia di configurazione.
        try:
            conn.execute(f'DROP TABLE IF EXISTS {RIORDINO_SOGGIE_TABLE}')
        except sqlite3.Error:
            pass
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {RIORDINO_SOGGIE_TABLE} ('
            'materiale TEXT NOT NULL,'
            'tipo TEXT NOT NULL,'
            'spessore TEXT NOT NULL,'
            'threshold INTEGER NOT NULL,'
            'quantita_riordino INTEGER,'
            'PRIMARY KEY (materiale, tipo, spessore)'
            ')'
        )
        conn.commit()

        # ------------------------------------------------------------------
        # Dizionario dei tipi (tipologie di lavorazione/materiali)
        #
        # Ogni tipo è un nome univoco inserito dall'utente (ad esempio
        # "fresato", "laminato").  Questa tabella non ha righe di
        # default; l'utente può popolarla tramite l'interfaccia.
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {TIPO_TABLE} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'nome TEXT NOT NULL UNIQUE'
            ')'
        )
        conn.commit()

        # ------------------------------------------------------------------
        # Dizionario delle macchine
        #
        # Analogamente al dizionario dei fornitori, questo vocabolario
        # consente di memorizzare l'elenco delle macchine disponibili per
        # l'uso nelle prenotazioni.  Le macchine verranno selezionate
        # dall'utente quando prenota una lastra.
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {MACCHINE_TABLE} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'nome TEXT NOT NULL UNIQUE'
            ')'
        )
        conn.commit()

        # ------------------------------------------------------------------
        # Tabella prenotazioni (Magazzino Live)
        #
        # Ogni prenotazione fa riferimento ad una lastra (material_id) e ad
        # un momento di scadenza (due_time). Il campo ``created_at`` viene
        # popolato con la data e ora corrente al momento della creazione.
        # Al rimorso di una lastra (tramite scansione) tutte le prenotazioni
        # corrispondenti verranno automaticamente cancellate.
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS {PRENOTAZIONI_TABLE} ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'material_id INTEGER NOT NULL,'
            'due_time TEXT NOT NULL,'
            'created_at TEXT NOT NULL,'
            'macchina_id INTEGER,'
            'is_generic INTEGER NOT NULL DEFAULT 0,'
            'FOREIGN KEY(material_id) REFERENCES materiali(id)'
            ')'
        )
        conn.commit()

        # Ensure ``macchina_id`` and ``is_generic`` columns exist on older installations.  If
        # missing, add them via ALTER TABLE.  ``PRAGMA table_info`` may
        # raise on unknown tables; errors are caught silently.
        try:
            cur_pren = conn.execute(f"PRAGMA table_info({PRENOTAZIONI_TABLE})")
            pren_cols = {row['name'] for row in cur_pren.fetchall()}
            if 'macchina_id' not in pren_cols:
                conn.execute(f"ALTER TABLE {PRENOTAZIONI_TABLE} ADD COLUMN macchina_id INTEGER")
            if 'is_generic' not in pren_cols:
                conn.execute(f"ALTER TABLE {PRENOTAZIONI_TABLE} ADD COLUMN is_generic INTEGER DEFAULT 0")
        except sqlite3.Error:
            pass

        # ------------------------------------------------------------------
        # Storico dei riordini effettuati
        #
        # Quando un operatore conferma un riordino dalla pagina "Riordini",
        # i dettagli (data, quantità, materiale, tipo, spessore e fornitore)
        # vengono registrati in questa tabella.  Questo permette di
        # visualizzare uno storico dei riordini eseguiti e di filtrare
        # dalla lista dei riordini i materiali già gestiti.  La tabella
        # viene creata se non esiste.
        conn.execute(
            'CREATE TABLE IF NOT EXISTS riordini_effettuati ('
            'id INTEGER PRIMARY KEY AUTOINCREMENT,'
            'material_id INTEGER,'
            'data TEXT NOT NULL,'
            'quantita INTEGER NOT NULL,'
            'materiale TEXT,'
            'tipo TEXT,'
            'spessore TEXT,'
            'fornitore TEXT'
            ')'
        )
        conn.commit()

        # Assicurati che le nuove colonne dimensione_x e dimensione_y esistano nella tabella storico.
        # Se mancano, aggiungile con ALTER TABLE. In caso di errore non fare nulla.
        try:
            cols = {row['name'] for row in conn.execute("PRAGMA table_info(riordini_effettuati)")}
            if 'dimensione_x' not in cols:
                conn.execute("ALTER TABLE riordini_effettuati ADD COLUMN dimensione_x TEXT")
            if 'dimensione_y' not in cols:
                conn.execute("ALTER TABLE riordini_effettuati ADD COLUMN dimensione_y TEXT")
            # Aggiungi la colonna tipo_evento per distinguere tra conferme d'ordine e accettazioni.
            if 'tipo_evento' not in cols:
                conn.execute("ALTER TABLE riordini_effettuati ADD COLUMN tipo_evento TEXT")
            # Aggiungi la colonna numero_ordine per salvare un codice univoco del riordino.
            if 'numero_ordine' not in cols:
                conn.execute("ALTER TABLE riordini_effettuati ADD COLUMN numero_ordine TEXT")
            conn.commit()
        except sqlite3.Error:
            pass

        # ------------------------------------------------------------------
        # Stato di accettazione dei riordini (ordini confermati ma non ancora
        # completamente ricevuti).  Questa tabella tiene traccia della quantità
        # totale ordinata e della quantità finora ricevuta.  Quando la quantità
        # ricevuta raggiunge il totale, la riga viene spostata nello storico e
        # rimossa da questa tabella.
        try:
            conn.execute(
                'CREATE TABLE IF NOT EXISTS riordini_accettazione ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT,'
                'data TEXT NOT NULL,'
                'materiale TEXT,'
                'tipo TEXT,'
                'spessore TEXT,'
                'dimensione_x TEXT,'
                'dimensione_y TEXT,'
                'quantita_totale INTEGER NOT NULL,'
                'quantita_ricevuta INTEGER NOT NULL DEFAULT 0'
                ')'
            )
            conn.commit()
            # Assicurati che la colonna numero_ordine esista nella tabella accettazione
            try:
                acc_cols = {row['name'] for row in conn.execute("PRAGMA table_info(riordini_accettazione)")}
                if 'numero_ordine' not in acc_cols:
                    conn.execute("ALTER TABLE riordini_accettazione ADD COLUMN numero_ordine TEXT")
                    conn.commit()
            except sqlite3.Error:
                pass
        except sqlite3.Error:
            pass

# Helper: retrieve the list of materials from the vocabulary table.
def get_materiali_vocabolario() -> list:
    """Restituisce un elenco di materiali disponibili nel vocabolario.

    Ogni materiale è rappresentato dalla colonna ``nome`` della
    tabella ``materiali_vocabolario`` (``VOCAB_TABLE``). Se la
    tabella non esiste o è vuota, restituisce un elenco vuoto. La
    funzione apre una connessione dedicata e la chiude
    automaticamente.
    """
    with get_db_connection() as conn:
        try:
            cur = conn.execute(f"SELECT nome FROM {VOCAB_TABLE} ORDER BY nome")
            return [row['nome'] for row in cur.fetchall()]
        except sqlite3.Error:
            # In caso di errore (ad esempio tabella inesistente) torna
            # l'elenco definito a livello di modulo.
            return list(DEFAULT_MATERIALI)

# Helper: retrieve the list of suppliers from the vocabulary table.
def get_fornitori_vocabolario() -> list:
    """Restituisce un elenco di fornitori disponibili nel vocabolario.

    Ogni fornitore è rappresentato dalla colonna ``nome`` della
    tabella ``fornitori_vocabolario`` (``SUPPLIER_TABLE``).  Se la
    tabella non esiste o è vuota, restituisce un elenco vuoto.
    """
    with get_db_connection() as conn:
        try:
            cur = conn.execute(f"SELECT nome FROM {SUPPLIER_TABLE} ORDER BY nome")
            return [row['nome'] for row in cur.fetchall()]
        except sqlite3.Error:
            return []

# Helper: retrieve the list of producers from the vocabulary table.
def get_produttori_vocabolario() -> list:
    """Restituisce un elenco di produttori disponibili nel vocabolario.

    Ogni produttore è rappresentato dalla colonna ``nome`` della
    tabella ``produttori_vocabolario`` (``PRODUTTORE_TABLE``).  Se la
    tabella non esiste o è vuota, restituisce un elenco vuoto.
    """
    with get_db_connection() as conn:
        try:
            cur = conn.execute(f"SELECT nome FROM {PRODUTTORE_TABLE} ORDER BY nome")
            return [row['nome'] for row in cur.fetchall()]
        except sqlite3.Error:
            return []

# Helper: retrieve the list of types from the vocabulary table.
def get_tipi_vocabolario() -> list:
    """Restituisce l'elenco dei tipi di lavorazione/materiali disponibili.

    Ciascun tipo è rappresentato dalla colonna ``nome`` della tabella
    ``tipi_vocabolario``.  Se la tabella non esiste o è vuota viene
    restituita una lista vuota.
    """
    with get_db_connection() as conn:
        try:
            cur = conn.execute(f"SELECT id, nome FROM {TIPO_TABLE} ORDER BY nome")
            return [dict(id=row['id'], nome=row['nome']) for row in cur.fetchall()]
        except sqlite3.Error:
            return []

# Helper: retrieve the list of machines from the vocabulary table.
def get_macchine_vocabolario() -> list:
    """Restituisce l'elenco delle macchine disponibili per le prenotazioni.

    Ciascuna macchina è rappresentata da un dizionario con chiavi ``id`` e
    ``nome``.  In caso di errori viene restituita una lista vuota.
    """
    with get_db_connection() as conn:
        try:
            cur = conn.execute(f"SELECT id, nome FROM {MACCHINE_TABLE} ORDER BY nome")
            return [dict(id=row['id'], nome=row['nome']) for row in cur.fetchall()]
        except sqlite3.Error:
            return []

# Helper: retrieve the list of articles in the catalog.
def get_articoli_catalogo() -> list[dict]:
    """Restituisce l'elenco completo delle combinazioni articolo definite nel catalogo.

    La tabella ``articoli_catalogo`` contiene le combinazioni di
    materiale/tipo/spessore/dimensioni create manualmente tramite la
    pagina di configurazione.  Ogni riga viene restituita come
    dizionario con le chiavi ``id``, ``materiale``, ``tipo``,
    ``spessore``, ``dimensione_x`` e ``dimensione_y``.  In caso di
    problemi di database viene restituita una lista vuota.
    """
    with get_db_connection() as conn:
        try:
            # Verifica le colonne presenti nella tabella per determinare quali
            # campi includere nella SELECT.  Sia ``preferito`` sia ``produttore``
            # possono mancare in installazioni precedenti; in tal caso
            # forniamo valori di default (0 per preferito, stringa vuota per produttore).
            cur_cols = conn.execute("PRAGMA table_info(articoli_catalogo)")
            cols = {row['name'] for row in cur_cols.fetchall()}
            preferito_col = 'preferito' in cols
            produttore_col = 'produttore' in cols
            # Costruisci dinamicamente la SELECT includendo sempre id, materiale, tipo,
            # spessore, dimensione_x, dimensione_y.  Se esiste la colonna
            # produttore includila, altrimenti usa stringa vuota.  Analogamente
            # per preferito.
            select_parts = [
                "id", "materiale", "tipo", "spessore", "dimensione_x", "dimensione_y"
            ]
            if produttore_col:
                select_parts.append("produttore")
            else:
                select_parts.append("'' AS produttore")
            if preferito_col:
                select_parts.append("preferito")
            else:
                select_parts.append("0 AS preferito")
            # Costruisci la clausola ORDER BY: includi produttore solo se presente
            order_cols = ["materiale", "tipo", "spessore", "dimensione_x", "dimensione_y"]
            if produttore_col:
                order_cols.append("produttore")
            select_stmt = "SELECT " + ", ".join(select_parts) + " FROM articoli_catalogo ORDER BY " + ", ".join(order_cols)
            cur = conn.execute(select_stmt)
            return [dict(row) for row in cur.fetchall()]
        except sqlite3.Error:
            return []


@app.route('/add_articolo_catalogo', methods=['POST'])
def add_articolo_catalogo():
    """Inserisce un nuovo articolo nel catalogo e torna alla pagina Anagrafiche
    portandoti sulla riga appena creata (o già esistente).

    La funzione raccoglie i parametri dal form, gestisce i valori vuoti come
    stringhe vuote e verifica se una combinazione identica è già presente nel
    catalogo.  In caso affermativo mostra un messaggio informativo; altrimenti
    inserisce la nuova riga.  Al termine memorizza la combinazione appena
    aggiunta nella sessione per poterla evidenziare sia nella pagina di
    configurazione sia nella pagina dei riordini e reindirizza alla pagina
    di configurazione propagando i parametri attraverso la query string.
    """
    # Estrai e normalizza i campi dal form; usa stringhe vuote per valori non forniti
    materiale = (request.form.get('materiale_catalogo') or '').strip()
    tipo      = (request.form.get('tipo_catalogo') or '').strip()
    spessore  = (request.form.get('spessore_catalogo') or '').strip()
    dx        = (request.form.get('dimensione_x_catalogo') or '').strip()
    dy        = (request.form.get('dimensione_y_catalogo') or '').strip()
    # Produttore facoltativo per la combinazione catalogo; stringa vuota se assente
    produttore = (request.form.get('produttore_catalogo') or '').strip()

    # Il materiale è obbligatorio
    if not materiale:
        flash('Il campo materiale è obbligatorio.', 'warning')
        return redirect(url_for('config'))

    with get_db_connection() as conn:
        # Verifica duplicati sulla combinazione di attributi, includendo il produttore
        try:
            # Normalizziamo il produttore per confrontare le stringhe vuote con valori NULL
            existing = conn.execute(
                """SELECT id FROM articoli_catalogo
                   WHERE materiale=? AND
                         (tipo=? OR (tipo IS NULL AND ?='')) AND
                         (spessore=? OR (spessore IS NULL AND ?='')) AND
                         (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) AND
                         (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) AND
                         (produttore=? OR (produttore IS NULL AND ?=''))""",
                (
                    materiale,
                    tipo or None, tipo,
                    spessore or None, spessore,
                    dx or None, dx,
                    dy or None, dy,
                    produttore or None, produttore,
                )
            ).fetchone()
        except sqlite3.Error:
            existing = None

        if existing:
            # Combinazione già presente, informa l'utente
            flash('Combinazione già presente in anagrafica.', 'info')
        else:
            try:
                # Inserisci la nuova combinazione nella tabella del catalogo articoli.
                conn.execute(
                    """INSERT INTO articoli_catalogo
                       (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        materiale,
                        tipo or None,
                        spessore or None,
                        dx or None,
                        dy or None,
                        produttore or None if produttore else None,
                    )
                )
                # Dopo aver aggiunto l'articolo al catalogo, inizializza anche la soglia di
                # riordino a zero per questa combinazione.  La tabella
                # ``riordino_soglie_ext`` richiede che tutti i campi testuali siano non null.
                # Pertanto normalizziamo i valori mancanti a stringa vuota.
                try:
                    ensure_riordino_soglie_ext_schema(conn)
                    # Normalizza i campi opzionali per la chiave primaria composta
                    mat_val = materiale
                    tipo_val = tipo or ''
                    sp_val = spessore or ''
                    dx_val = dx or ''
                    dy_val = dy or ''
                    prod_val = produttore or ''
                    # Inserisci una soglia pari a 0 e una quantità di riordino predefinita (1)
                    conn.execute(
                        "INSERT OR REPLACE INTO riordino_soglie_ext (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore, threshold, quantita_riordino) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        (mat_val, tipo_val, sp_val, dx_val, dy_val, prod_val, 0, 1),
                    )
                except Exception:
                    # In caso di errore nell'inserimento della soglia, non bloccare l'operazione.
                    pass
                flash('Articolo aggiunto al catalogo!', 'success')
            except sqlite3.Error as e:
                flash(f"Errore nell'inserimento dell'articolo: {e}", 'danger')

    # Memorizza la combinazione per evidenziare la riga nelle pagine di configurazione e riordini
    # Memorizza la combinazione per evidenziare la riga nelle pagine di configurazione e riordini
    session['last_added_combo'] = {
        'materiale': materiale,
        'tipo': tipo,
        'spessore': spessore,
        'dimensione_x': dx,
        'dimensione_y': dy,
        'produttore': produttore,
    }
    # Redirige con parametri per evidenziare e scrollare la riga
    return redirect(url_for('config',
                            hl_mat=materiale, hl_tipo=tipo, hl_sp=spessore, hl_dx=dx, hl_dy=dy, hl_produttore=produttore))


@app.route('/check_articolo_catalogo', methods=['POST'])
def check_articolo_catalogo():
    """Controlla se esiste già un articolo nel catalogo con la stessa combinazione di parametri.

    Riceve i campi ``materiale``, ``tipo``, ``spessore``, ``dimensione_x`` e ``dimensione_y`` via POST.
    Restituisce un JSON con la chiave ``exists`` impostata a True se la combinazione esiste, False altrimenti.
    I campi vuoti o assenti vengono trattati come NULL nel database.
    """
    materiale = request.form.get('materiale', '').strip()
    tipo = request.form.get('tipo', '').strip()
    spessore = request.form.get('spessore', '').strip()
    dx = request.form.get('dimensione_x', '').strip()
    dy = request.form.get('dimensione_y', '').strip()
    # Produttore facoltativo: se non specificato usiamo stringa vuota
    produttore = request.form.get('produttore', '').strip()
    exists = False
    with get_db_connection() as conn:
        try:
            row = conn.execute(
                "SELECT id FROM articoli_catalogo WHERE materiale=? AND "
                "(tipo = ? OR (tipo IS NULL AND ? = '')) AND "
                "(spessore = ? OR (spessore IS NULL AND ? = '')) AND "
                "(dimensione_x = ? OR (dimensione_x IS NULL AND ? = '')) AND "
                "(dimensione_y = ? OR (dimensione_y IS NULL AND ? = '')) AND "
                "(produttore = ? OR (produttore IS NULL AND ? = ''))",
                (
                    materiale,
                    tipo if tipo else None, tipo,
                    spessore if spessore else None, spessore,
                    dx if dx else None, dx,
                    dy if dy else None, dy,
                    produttore if produttore else None, produttore,
                ),
            ).fetchone()
            if row:
                exists = True
        except sqlite3.Error:
            exists = False
    return {'exists': exists}


@app.route('/delete_articolo_catalogo/<int:articolo_id>', methods=['POST'])
def delete_articolo_catalogo(articolo_id: int):
    """Elimina una combinazione di articolo dal catalogo.

    L'eliminazione avviene tramite POST per evitare cancellazioni accidentali.
    Dopo l'eliminazione viene reindirizzato l'utente alla pagina di
    configurazione.
    """
    with get_db_connection() as conn:
        try:
            conn.execute("DELETE FROM articoli_catalogo WHERE id=?", (articolo_id,))
            flash('Combinazione eliminata dal catalogo!', 'success')
        except sqlite3.Error:
            flash("Errore durante l'eliminazione della combinazione.", 'danger')
    return redirect(url_for('config'))

# ---------------------------------------------------------------------------
# Toggle preferito per articoli catalogo

@app.route('/toggle_preferito/<int:articolo_id>', methods=['POST'])
def toggle_preferito(articolo_id: int):
    """Inverti lo stato di preferito per una combinazione dell'anagrafica articoli.

    Questa route riceve l'identificatore di una riga della tabella
    ``articoli_catalogo`` e ne alterna il valore del campo ``preferito`` tra 0 e 1.
    Al termine reindirizza l'utente alla pagina di configurazione.  L'uso di
    POST riduce il rischio di modifiche accidentali tramite URL.
    """
    with get_db_connection() as conn:
        try:
            row = conn.execute(
                "SELECT preferito FROM articoli_catalogo WHERE id=?",
                (articolo_id,)
            ).fetchone()
            if row is not None:
                current = row['preferito'] or 0
                new_val = 0 if int(current) else 1
                conn.execute(
                    "UPDATE articoli_catalogo SET preferito=? WHERE id=?",
                    (new_val, articolo_id),
                )
                conn.commit()
        except sqlite3.Error:
            # In caso di errore non modifichiamo nulla
            pass
    return redirect(url_for('config'))
# Inizializza il database all'avvio del modulo
init_db()

# ---------------------------------------------------------------------------
# Gestione autenticazione e permessi
#
# L'applicazione richiede l'autenticazione per poter accedere a tutte
# le pagine ad eccezione di ``/login`` e dei file statici.  Ogni utente
# definito nella tabella ``users`` dispone di un nome utente, una
# password (hash) e di una lista di "tab" che può visualizzare.  La
# funzione ``require_login`` reindirizza automaticamente alla pagina
# di login se la sessione non contiene ``user_id``.  Le credenziali
# vengono verificate tramite ``check_password_hash`` e, in caso di
# successo, vengono memorizzate in ``session`` insieme all'elenco
# delle tab abilitate.  È prevista anche una route ``/logout`` per
# cancellare la sessione.

@app.before_request
def require_login():
    """Impedisce l'accesso alle pagine se l'utente non è autenticato.

    Tutte le route sono protette tranne ``/login`` e i file statici.
    Se l'utente non ha effettuato l'accesso, viene reindirizzato alla
    pagina di login.
    """
    # ``request.endpoint`` può essere None per richieste non risolte
    endpoint = request.endpoint
    # Endpoints che non richiedono login: la pagina di login e
    # i file statici (serviti internamente da Flask).  L'end point
    # ``static`` è utilizzato da Flask per servire il contenuto della
    # cartella ``static``.
    if endpoint in (None, 'login', 'static'):
        return
    # Se l'utente non è autenticato reindirizza a login
    if 'user_id' not in session:
        return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Gestisce la pagina di accesso dell'utente.

    In GET viene restituito il modulo di login.  In POST vengono
    verificate le credenziali e, se corrette, l'utente viene
    reindirizzato alla dashboard con le informazioni di sessione
    impostate.
    """
    if request.method == 'POST':
        # Recupera le credenziali inserite dall'utente.  Lo username
        # viene normalizzato in modo da ignorare la differenza tra
        # maiuscole e minuscole.  In questo modo, ad esempio,
        # "Admin" e "admin" verranno considerati equivalenti in fase
        # di login.
        raw_username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        # Normalizziamo lo username in minuscolo per il confronto.  Non
        # modifichiamo il valore memorizzato nel database: il campo
        # ``username`` conserva la forma originale, ma la query di
        # ricerca utilizza la funzione LOWER() per confrontare i
        # valori senza distinzione di maiuscole/minuscole.
        with get_db_connection() as conn:
            try:
                user = conn.execute(
                    f"SELECT id, username, password, tabs FROM {USERS_TABLE} WHERE lower(username)=lower(?)",
                    (raw_username,),
                ).fetchone()
            except sqlite3.Error:
                user = None
        if user and check_password_hash(user['password'], password):
            # Login ok: salva informazioni in sessione
            session['user_id'] = user['id']
            session['username'] = user['username']
            allowed = user['tabs'].split(',') if user['tabs'] else []
            # Rimuovi eventuali stringhe vuote da allowed
            allowed = [t.strip() for t in allowed if t.strip()]
            # Le tab disponibili per l'utente sono quelle definite nel campo "tabs".
            # Non aggiungiamo automaticamente la scheda "storico" qui: verrà mostrata
            # nella barra di navigazione solo se specificata tra le autorizzazioni
            # dell'utente (vedi session['allowed_tabs']).
            session['allowed_tabs'] = allowed
            return redirect(url_for('dashboard'))
        # Se le credenziali non sono corrette mostriamo un messaggio
        flash('Credenziali non valide.', 'danger')
    # In caso di GET o errore di login
    return render_template('login.html')


@app.route('/logout')
def logout():
    """Chiude la sessione dell'utente e reindirizza al login."""
    session.clear()
    return redirect(url_for('login'))


@app.route('/accessi', methods=['GET', 'POST'])
def accessi():
    """Gestione degli utenti e dei permessi di accesso.

    Questa pagina consente di elencare gli utenti esistenti e di
    aggiungerne di nuovi definendo per ognuno username, password e le
    relative pagine (tab) a cui è consentito l'accesso.  Per accedere
    a questa pagina l'utente deve disporre del permesso 'accessi'.
    """
    # Verifica che l'utente abbia i permessi per la gestione accessi
    allowed = session.get('allowed_tabs', [])
    if 'accessi' not in allowed:
        flash('Accesso non autorizzato alla gestione utenti.', 'danger')
        return redirect(url_for('dashboard'))
    # Definiamo l'elenco delle tab disponibili con etichette umane
    # Definiamo l'elenco delle tab disponibili con etichette umane.
    # La scheda "Accessi" viene mostrata come "Admin" nella
    # navigazione e nella gestione degli utenti, ma la chiave rimane
    # "accessi" per compatibilità con i permessi salvati per gli utenti.
    available_tabs = [
        ('dashboard', 'Dashboard'),
        ('add', 'Aggiungi'),
        ('scan', 'Scarico'),
        ('dizionario', 'Dizionario'),
        ('config', 'Anagrafiche articoli'),
        ('riordini', 'Riordini'),
        ('live', 'Magazzino Live'),
        ('storico', 'Storico'),
        ('accessi', 'Admin'),
    ]
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        selected_tabs = request.form.getlist('tabs')
        # Valida campi obbligatori
        if not username or not password:
            flash('Username e password sono obbligatori.', 'danger')
            return redirect(url_for('accessi'))
        # Verifica se esiste già un utente con lo stesso nome ignorando la
        # differenza tra maiuscole e minuscole.  In questo modo non è
        # possibile registrare "Admin" e "admin" come account distinti.
        with get_db_connection() as conn:
            try:
                existing = conn.execute(
                    f"SELECT id FROM {USERS_TABLE} WHERE lower(username)=lower(?)",
                    (username,),
                ).fetchone()
            except sqlite3.Error:
                existing = None
            if existing:
                flash('Esiste già un utente con questo nome.', 'warning')
                return redirect(url_for('accessi'))
            # Salva nuovo utente: la password viene memorizzata in forma
            # hashata e le tab selezionate vengono salvate come elenco
            # separato da virgole.
            hashed = generate_password_hash(password)
            try:
                conn.execute(
                    f"INSERT INTO {USERS_TABLE} (username, password, tabs) VALUES (?,?,?)",
                    (username, hashed, ','.join(selected_tabs)),
                )
                conn.commit()
                flash('Utente creato con successo.', 'success')
            except sqlite3.Error as exc:
                flash(f'Errore durante la creazione dell\'utente: {exc}', 'danger')
        return redirect(url_for('accessi'))
    # GET: elenca gli utenti
    with get_db_connection() as conn:
        try:
            # Recupera anche il campo password per mostrarlo in tabella.  Il
            # valore memorizzato è l'hash della password, che non consente
            # di ricostruire la stringa originale, ma permette di
            # verificare rapidamente se la password è stata impostata o
            # aggiornata.
            cur = conn.execute(
                f"SELECT id, username, password, tabs FROM {USERS_TABLE} ORDER BY username"
            )
            user_rows = cur.fetchall()
            users = []
            for row in user_rows:
                tabs_val = row['tabs'] or ''
                tabs_list = [t.strip() for t in tabs_val.split(',') if t.strip()]
                users.append({
                    'id': row['id'],
                    'username': row['username'],
                    'password': row['password'],
                    'tabs': tabs_list,
                })
        except sqlite3.Error:
            users = []
    # Carica la configurazione SMTP esistente per precompilare il modulo.
    smtp_settings = load_smtp_config()
    return render_template('accessi.html', users=users, available_tabs=available_tabs, smtp_settings=smtp_settings)


@app.route('/update_smtp_settings', methods=['POST'])
def update_smtp_settings():
    """Aggiorna le impostazioni SMTP per l'invio email.

    Questa vista è accessibile solo agli utenti con il permesso
    ``accessi`` e consente di salvare le impostazioni del server
    SMTP utilizzate dall'applicazione per inviare le email ai fornitori.
    I campi del form includono host, port, user, password, from e
    l'opzione TLS.  Le impostazioni vengono salvate nel file
    ``smtp_config.txt`` e sovrascrivono eventuali valori precedenti.

    :return: reindirizza alla pagina di gestione accessi.
    """
    allowed = session.get('allowed_tabs', [])
    if 'accessi' not in allowed:
        flash('Accesso non autorizzato alla gestione utenti.', 'danger')
        return redirect(url_for('dashboard'))
    # Raccogli i valori dal form; lasciamo che il browser
    # restituisca stringhe vuote per i campi non compilati.
    host = request.form.get('smtp_host', '').strip() or None
    port = request.form.get('smtp_port', '').strip() or None
    user = request.form.get('smtp_user', '').strip() or None
    pwd = request.form.get('smtp_pass', '').strip() or None
    from_addr = request.form.get('smtp_from', '').strip() or None
    tls = request.form.get('smtp_tls', '').strip() or None
    # Converte la porta in stringa se definita
    if port:
        try:
            int(port)  # verifica che sia numerica
        except ValueError:
            flash('La porta SMTP deve essere un numero.', 'danger')
            return redirect(url_for('accessi'))
    # Normalizza il flag TLS (salva solo se esplicitamente indicato)
    if tls and tls.lower() not in ('true', 'false', '1', '0', 'yes', 'no'):
        flash('Il valore TLS deve essere true/false.', 'danger')
        return redirect(url_for('accessi'))
    # Salva la configurazione
    settings: dict[str, str | None] = {
        'host': host,
        'port': port,
        'user': user,
        'pass': pwd,
        'from': from_addr,
        'tls': tls,
    }
    save_smtp_config(settings)
    flash('Impostazioni SMTP salvate con successo.', 'success')
    return redirect(url_for('accessi'))


@app.route('/accessi/edit/<int:user_id>', methods=['GET', 'POST'])
def edit_user(user_id: int):
    """Modifica le credenziali e i permessi di un utente esistente.

    Questa vista consente agli amministratori con il permesso ``accessi`` di
    modificare uno specifico account.  Viene presentato un modulo simile a
    quello di creazione con i valori correnti pre‑compilati.  Lo username
    può essere cambiato; se la password viene lasciata vuota, rimarrà
    invariata.  È inoltre possibile selezionare le tab a cui l'utente
    avrà accesso.

    :param user_id: l'identificativo dell'utente da modificare
    :return: una pagina HTML con il form o un reindirizzamento alla lista
    """
    # Verifica permesso di accesso
    allowed = session.get('allowed_tabs', [])
    if 'accessi' not in allowed:
        flash('Accesso non autorizzato alla gestione utenti.', 'danger')
        return redirect(url_for('dashboard'))
    # Stessa lista di tab definita nella pagina principale degli accessi
    # La stessa lista di tab definita nella pagina principale degli accessi.
    # Rinominare la scheda "Accessi" in "Admin" per l'interfaccia,
    # mantenendo la chiave "accessi" per compatibilità con i permessi.
    available_tabs = [
        ('dashboard', 'Dashboard'),
        ('add', 'Aggiungi'),
        ('scan', 'Scarico'),
        ('dizionario', 'Dizionario'),
        ('config', 'Anagrafiche articoli'),
        ('riordini', 'Riordini'),
        ('live', 'Magazzino Live'),
        ('storico', 'Storico'),
        ('accessi', 'Admin'),
    ]
    # Recupera informazioni sull'utente
    with get_db_connection() as conn:
        try:
            row = conn.execute(
                f"SELECT id, username, password, tabs FROM {USERS_TABLE} WHERE id=?",
                (user_id,),
            ).fetchone()
        except sqlite3.Error:
            row = None
    if not row:
        flash('Utente non trovato.', 'warning')
        return redirect(url_for('accessi'))
    # Costruisci dizionario utente per il template
    current_tabs_list = [t.strip() for t in (row['tabs'] or '').split(',') if t.strip()]
    user_data = {
        'id': row['id'],
        'username': row['username'],
        'password': row['password'],
        'tabs': current_tabs_list,
        # Include the ordine_template field if present in the database. This will be shown in the form
        'ordine_template': row['ordine_template'] if 'ordine_template' in row.keys() else None,
    }

    # Costruisci la lista ordinata delle tab per visualizzare nel modulo.
    # Le tab già assegnate all'utente vengono mostrate per prime nell'ordine salvato,
    # seguite dalle eventuali altre tab disponibili non ancora selezionate.
    value_to_label = {value: label for value, label in available_tabs}
    ordered_tabs: list[tuple[str, str]] = []
    for t in current_tabs_list:
        if t in value_to_label:
            ordered_tabs.append((t, value_to_label[t]))
    for value, label in available_tabs:
        if value not in current_tabs_list:
            ordered_tabs.append((value, label))
    if request.method == 'POST':
        new_username = request.form.get('username', '').strip()
        new_password = request.form.get('password', '').strip()
        selected_tabs = request.form.getlist('tabs')
        ordine_template_val = request.form.get('ordine_template', '').strip()
        if not new_username:
            flash('Il nome utente è obbligatorio.', 'danger')
            # In caso di errore, ripresentiamo la lista ordinata per l'utente
            return render_template('edit_user.html', user=user_data, available_tabs=available_tabs, ordered_tabs=ordered_tabs)
        with get_db_connection() as conn:
            # Verifica che non esista un altro utente con lo stesso nome
            try:
                dup = conn.execute(
                    f"SELECT id FROM {USERS_TABLE} WHERE lower(username)=lower(?) AND id<>?",
                    (new_username, user_id),
                ).fetchone()
            except sqlite3.Error:
                dup = None
            if dup:
                flash('Esiste già un utente con questo nome.', 'warning')
                # Ripresentiamo la lista ordinata
                return render_template('edit_user.html', user=user_data, available_tabs=available_tabs, ordered_tabs=ordered_tabs)
            # Determina quale hash di password salvare: se l'utente ha
            # inserito una nuova password, generiamo l'hash; altrimenti
            # manteniamo il valore corrente.
            hashed = row['password']
            if new_password:
                hashed = generate_password_hash(new_password)
            try:
                # Aggiorna anche il campo ordine_template se disponibile
                try:
                    cols = conn.execute(f"PRAGMA table_info({USERS_TABLE})").fetchall()
                    col_names = {c['name'] for c in cols}
                except sqlite3.Error:
                    col_names = set()
                if 'ordine_template' in col_names:
                    conn.execute(
                        f"UPDATE {USERS_TABLE} SET username=?, password=?, tabs=?, ordine_template=? WHERE id=?",
                        (new_username, hashed, ','.join(selected_tabs), ordine_template_val or None, user_id),
                    )
                else:
                    conn.execute(
                        f"UPDATE {USERS_TABLE} SET username=?, password=?, tabs=? WHERE id=?",
                        (new_username, hashed, ','.join(selected_tabs), user_id),
                    )
                conn.commit()
                flash('Utente aggiornato con successo.', 'success')
                # Se si sta modificando l'utente attualmente loggato, aggiorna
                # la sessione per riflettere eventuali modifiche a nome utente
                # e permessi.
                if session.get('user_id') == user_id:
                    session['username'] = new_username
                    allowed_tabs = [t.strip() for t in selected_tabs if t.strip()]
                    session['allowed_tabs'] = allowed_tabs
            except sqlite3.Error as exc:
                flash(f'Errore durante l\'aggiornamento dell\'utente: {exc}', 'danger')
            return redirect(url_for('accessi'))
    # GET: mostra il modulo precompilato
    return render_template('edit_user.html', user=user_data, available_tabs=available_tabs, ordered_tabs=ordered_tabs)


@app.route('/accessi/delete/<int:user_id>', methods=['GET', 'POST'])
def delete_user(user_id: int):
    """Elimina in maniera permanente un utente dal sistema.

    L'accesso a questa funzione è consentito soltanto agli amministratori
    con il permesso ``accessi``.  In GET viene mostrata una pagina di
    conferma che chiede se procedere con l'eliminazione.  In POST,
    l'utente viene cancellato definitivamente dal database.  Non è
    possibile eliminare l'utente attualmente loggato per evitare di
    interrompere la sessione corrente.

    :param user_id: identificativo dell'utente da rimuovere
    :return: una pagina di conferma o un redirect
    """
    allowed = session.get('allowed_tabs', [])
    if 'accessi' not in allowed:
        flash('Accesso non autorizzato alla gestione utenti.', 'danger')
        return redirect(url_for('dashboard'))
    # Recupera l'utente da eliminare per mostrare info
    with get_db_connection() as conn:
        try:
            row = conn.execute(
                f"SELECT id, username FROM {USERS_TABLE} WHERE id=?",
                (user_id,),
            ).fetchone()
        except sqlite3.Error:
            row = None
    if not row:
        flash('Utente non trovato.', 'warning')
        return redirect(url_for('accessi'))
    # Evita che l'utente cancelli sé stesso per non lasciare il sistema senza
    # un account attivo.  L'amministratore può eventualmente creare un
    # nuovo account e poi eliminare quello precedente.
    if request.method == 'POST':
        if session.get('user_id') == user_id:
            flash('Non puoi eliminare il tuo account mentre sei connesso.', 'warning')
            return redirect(url_for('accessi'))
        with get_db_connection() as conn:
            try:
                conn.execute(
                    f"DELETE FROM {USERS_TABLE} WHERE id=?",
                    (user_id,),
                )
                conn.commit()
                flash('Utente eliminato con successo.', 'success')
            except sqlite3.Error as exc:
                flash(f'Errore durante l\'eliminazione dell\'utente: {exc}', 'danger')
        return redirect(url_for('accessi'))
    # GET: mostra pagina di conferma
    return render_template('confirm_delete_user.html', user={'id': row['id'], 'username': row['username']})


@app.route('/')
def dashboard():
    """Pagina principale: elenco materiali con filtri, ricerca e ordinamento.

    Oltre alle funzionalità esistenti, questa versione estende la
    dashboard per supportare la visualizzazione di documenti associati a
    ciascun materiale, segnalare se un bancale contiene almeno uno
    sfrido e migliorare la logica dei filtri affinché i bancali vengano
    mostrati anche quando le lastre figlie soddisfano i criteri di
    ricerca.
    """
    # Parametri di filtro e ricerca
    materiale_filtro = request.args.get('materiale', '').strip()
    # New filter for ``tipo`` (type of processing/material).  Retrieve from query string
    # and strip whitespace to allow matching on exact values in the DB.  When
    # empty the filter will not be applied.  This value is passed through
    # to the template and used in the row filtering logic below.
    tipo_filtro = request.args.get('tipo', '').strip()
    search = request.args.get('search', '').strip()
    # Combined filter for location (e.g. "A-12").  The application historically
    # supported separate "ubicazione_lettera" and "ubicazione_numero" query
    # parameters.  To simplify the selection of locations on the dashboard we
    # introduce a single ``ubicazione`` parameter which can contain the letter
    # and number together (with or without a dash).  If ``ubicazione`` is
    # provided it takes precedence over the individual letter/number fields.
    filtro_ubicazione = request.args.get('ubicazione', '').strip()
    filtro_lettera = request.args.get('ubicazione_lettera', '')
    filtro_numero = request.args.get('ubicazione_numero', '')
    if filtro_ubicazione:
        # Parse combined location into letter and number parts.  Accept formats
        # like "A-12", "A12" or "a-12".  Fallback to splitting on dash.
        import re
        m = re.match(r'^\s*([A-Za-z]+)\s*-?\s*(\d+)\s*$', filtro_ubicazione)
        if m:
            filtro_lettera = m.group(1).upper()
            filtro_numero = m.group(2)
        else:
            # Try a simple split by dash if the regex didn't match
            parts = filtro_ubicazione.split('-')
            if len(parts) == 2:
                filtro_lettera = parts[0].strip().upper()
                filtro_numero = parts[1].strip()
    # If only the separate fields are provided (e.g. via sorting links) and the
    # combined field is empty, reconstruct the combined value so that the
    # combined input retains its value in the UI.  When both parts exist,
    # concatenate them with a dash.  If only one part exists, simply use
    # that part (useful for partial filters).
    if not filtro_ubicazione and (filtro_lettera or filtro_numero):
        if filtro_lettera and filtro_numero:
            filtro_ubicazione = f"{filtro_lettera}-{filtro_numero}"
        elif filtro_lettera:
            filtro_ubicazione = str(filtro_lettera)
        else:
            filtro_ubicazione = str(filtro_numero)
    sort_spessore = request.args.get('sort_spessore', '').strip().lower()
    sort_qty = request.args.get('sort_qty', '').strip().lower()
    # Field used to sort by combined location letter/number (e.g. "A12").
    sort_ubicazione = request.args.get('sort_ubicazione', '').strip().lower()
    view_filter = request.args.get('view', '').strip().lower() or 'all'
    fornitore_filtro = request.args.get('fornitore', '').strip()
    # Filtro produttore: valore proveniente dal menu a tendina sulla dashboard
    produttore_filtro = request.args.get('produttore', '').strip()
    # Filtri dimensionali e di spessore: accettano valori esatti o intervalli (es. "40-60").
    # Le stringhe originali vengono comunque salvate per la UI.
    max_x_str = request.args.get('max_x', '').strip()
    max_y_str = request.args.get('max_y', '').strip()
    spessore_str = request.args.get('spessore', '').strip()
    # Usa la funzione di parsing per convertire gli input in range o None.
    rng_x = _parse_range_or_exact(max_x_str)
    rng_y = _parse_range_or_exact(max_y_str)
    rng_sp = _parse_range_or_exact(spessore_str)

    # Filtro per ID lastra.  Accetta solo valori numerici.  Se presente,
    # limita la dashboard alla sola riga radice (bancale o lastra
    # indipendente) che contiene la lastra specificata.  Quando l'ID
    # corrisponde a una lastra figlia, il relativo bancale viene
    # visualizzato e l'ID della lastra viene salvato per poterlo
    # evidenziare successivamente nella pagina di dettaglio.  Se il
    # parametro non è numerico o non esiste alcun materiale con tale ID,
    # la lista dei materiali visualizzati sarà vuota.
    id_lastra_str = request.args.get('id_lastra', '').strip()
    highlight_child_id: int | None = None
    highlight_root_id: int | None = None

    # Ricarica le soglie ad ogni richiesta
    global REORDER_THRESHOLD, ALERT_THRESHOLD
    REORDER_THRESHOLD, ALERT_THRESHOLD = load_thresholds()

    conn = get_db_connection()
    # Carica tutti i materiali per applicare filtri complessi in Python
    rows_all = conn.execute(
        "SELECT * FROM materiali ORDER BY (parent_id IS NOT NULL), parent_id, id"
    ).fetchall()

    # Se l'utente ha specificato un ID di lastra, individua la riga
    # corrispondente e determina il bancale radice.  Popola
    # ``highlight_child_id`` con l'ID della lastra da evidenziare (se
    # appartenente ad un bancale) e ``highlight_root_id`` con l'ID del
    # bancale o della lastra indipendente da mostrare.  Se l'ID non è
    # valido o non esiste, la variabile ``highlight_root_id`` rimane None.
    if id_lastra_str:
        # Verifica che il parametro contenga solo cifre (input type=number
        # potrebbe fornire stringhe vuote o valori non numerici).  In caso
        # contrario non effettuiamo alcuna ricerca.
        if id_lastra_str.isdigit():
            try:
                id_lastra_int = int(id_lastra_str)
            except Exception:
                id_lastra_int = None
            if id_lastra_int is not None:
                # Crea una mappa ID->riga per recuperare rapidamente le
                # informazioni del materiale.  L'oggetto sqlite3.Row non è
                # direttamente indicizzabile per performance, quindi
                # converte le righe in dict per l'accesso per chiave.
                id_map = {}
                for rr in rows_all:
                    try:
                        id_map[int(rr['id'])] = rr
                    except Exception:
                        continue
                target_row = id_map.get(id_lastra_int)
                if target_row:
                    # Se la riga ha un parent_id, la lastra appartiene ad un
                    # bancale.  Ricaviamo il bancale radice risalendo la
                    # catena dei genitori.  Conserviamo anche
                    # ``highlight_child_id`` per poterla evidenziare nella
                    # pagina di dettaglio.
                    current_row = target_row
                    if current_row['parent_id']:
                        # La lastra è figlia: evidenzia questa lastra e
                        # risali al padre.
                        highlight_child_id = id_lastra_int
                        parent_id = current_row['parent_id']
                        # Continua a risalire finché esistono genitori
                        while parent_id:
                            parent_row = id_map.get(int(parent_id))
                            if not parent_row:
                                break
                            highlight_root_id = parent_row['id']
                            if parent_row['parent_id']:
                                parent_id = parent_row['parent_id']
                            else:
                                # Arrivato al bancale o lastra indipendente
                                break
                    else:
                        # L'ID corrisponde ad un bancale o a una lastra indipendente
                        highlight_root_id = current_row['id']
                        highlight_child_id = None
        # Se non è un valore numerico valido, lascia highlight_root_id a None.

    # Mappa delle lastre figlie per ciascun bancale
    children_map: dict[int, list[sqlite3.Row]] = {}
    for r in rows_all:
        if r['parent_id']:
            children_map.setdefault(r['parent_id'], []).append(r)

    # Determina gli ID dei materiali attualmente prenotati e gli ID
    # dei bancali (o lastre indipendenti) da evidenziare.  Se una lastra
    # figlia è prenotata evidenziamo il suo bancale, altrimenti,
    # se la lastra è indipendente, evidenziamo direttamente se stessa.
    try:
        reserved_ids = get_reserved_material_ids()
    except Exception:
        reserved_ids = set()
    reserved_highlight_ids: set[int] = set()
    # rows_all include tutte le lastre e i bancali; identifichiamo le righe
    # prenotate e aggiungiamo l'ID del bancale o della lastra indipendente.
    for r in rows_all:
        rid = r['id']
        try:
            if rid in reserved_ids:
                parent_id = r['parent_id']
                if parent_id:
                    reserved_highlight_ids.add(int(parent_id))
                else:
                    reserved_highlight_ids.add(int(rid))
        except Exception:
            # Eventuali errori di conversione vengono ignorati
            continue

    # Carica documenti e raggruppa per materiale e per combinazione
    # I documenti possono essere legati sia a un singolo materiale (tramite
    # la colonna ``material_id``) sia a una combinazione completa di anagrafica
    # articoli composta da (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore).
    # Per supportare entrambe le modalità, costruiamo due strutture di appoggio:
    # - ``attachments_by_material_id``: mappa l'ID del materiale a tutti i documenti
    #   caricati specificamente per quel materiale.
    # - ``attachments_by_combo``: mappa la tupla (materiale, tipo, spessore,
    #   dimensione_x, dimensione_y, produttore) a tutti i documenti caricati
    #   per la relativa combinazione.  Gli elementi ``None`` o stringhe vuote
    #   vengono normalizzati a stringhe vuote per consentire il confronto.
    docs = conn.execute(
        "SELECT id, material_id, original_name, materiale, tipo, spessore, dimensione_x, dimensione_y, produttore "
        "FROM documenti"
    ).fetchall()
    attachments_by_material_id: dict[int | None, list[sqlite3.Row]] = {}
    # La chiave di attachments_by_combo è una tupla a 6 elementi comprendente il produttore
    attachments_by_combo: dict[tuple[str, str, str, str, str, str], list[sqlite3.Row]] = {}
    for d in docs:
        # Associa documento a material_id (può essere 0 o NULL per documenti legati solo alla combinazione)
        attachments_by_material_id.setdefault(d['material_id'], []).append(d)
        # Costruisci chiave di combinazione normalizzando a stringa vuota i valori mancanti
        combo_key = (
            (d['materiale'] or ''),
            (d['tipo'] or ''),
            (d['spessore'] or ''),
            (d['dimensione_x'] or ''),
            (d['dimensione_y'] or ''),
            (d['produttore'] or '')
        )
        attachments_by_combo.setdefault(combo_key, []).append(d)

    # Carica la mappa delle soglie di riordino per materiale/spessore.  Questa
    # struttura viene utilizzata più avanti per determinare se un materiale
    # raggiunge la soglia di riordino.  Usiamo tuple (materiale, spessore) come
    # chiavi.  Se la tabella non contiene record, il dizionario sarà vuoto e
    # il valore di default verrà usato quando richiesto.
    threshold_map: dict[tuple[str, str], int] = {}
    try:
        cur_thresholds = conn.execute(
            f"SELECT materiale, spessore, threshold FROM {RIORDINO_SOGGIE_TABLE}"
        ).fetchall()
        for tr in cur_thresholds:
            key = (tr['materiale'], tr['spessore'] or '')
            try:
                threshold_map[key] = int(tr['threshold'])
            except (ValueError, TypeError):
                threshold_map[key] = DEFAULT_REORDER_THRESHOLD
    except sqlite3.Error:
        threshold_map = {}

    # ------------------------------------------------------------------
    # Combinazioni manuali (anagrafica articoli)
    #
    # Per limitare l'evidenziazione e il conteggio dei riordini alle sole
    # combinazioni definite manualmente, carichiamo le coppie
    # (materiale, spessore) dalla tabella ``articoli_catalogo``.  La
    # dashboard non distingue il campo ``tipo`` nella gestione delle soglie
    # aggregate, per cui qui ignoriamo tale attributo.  In caso di
    # eccezioni la variabile rimane vuota e nessuna combinazione
    # verrà segnalata come da riordinare.
    manual_keys: set[tuple[str, str]] = set()
    try:
        manual_rows = conn.execute(
            "SELECT materiale, spessore FROM articoli_catalogo"
        ).fetchall()
        for row in manual_rows:
            mat = row['materiale']
            sp = (row['spessore'] or '').strip()
            manual_keys.add((mat, sp))
    except sqlite3.Error:
        manual_keys = set()

    # Funzioni di parsing per ordinamento
    def parse_spessore(value: str) -> int:
        if not value:
            return 0
        num = ''
        for ch in str(value):
            if ch.isdigit():
                num += ch
            elif num:
                break
        try:
            return int(num) if num else 0
        except ValueError:
            return 0
    def parse_quantita(q: object) -> int:
        try:
            return int(q) if q is not None else 0
        except (ValueError, TypeError):
            return 0

    def parse_ubicazione(u: str):
        """
        Parse a combined location value into a sortable tuple.
        Accepts strings like 'A12', 'B-7', or 'C7'.  Returns a tuple (letter, number)
        where letter is the first alphabetic character and number is the integer at
        the end of the string.  Missing values yield ('', -1).
        """
        if not u:
            return ("", -1)
        s = str(u).strip().upper().replace(" ", "")
        letter = ""
        # First alphabetic character
        for ch in s:
            if ch.isalpha():
                letter = ch
                break
        # Collect trailing digits for the numeric part
        num_str = ""
        for ch in reversed(s):
            if ch.isdigit():
                num_str = ch + num_str
            elif num_str:
                break
        number = -1
        try:
            number = int(num_str) if num_str else -1
        except ValueError:
            number = -1
        return (letter, number)

    # Determina il campo di ordinamento e la direzione
    sort_field = None
    sort_dir = None
    if sort_qty in ('asc', 'desc'):
        sort_field = 'quantita'
        sort_dir = sort_qty
    elif sort_spessore in ('asc', 'desc'):
        sort_field = 'spessore'
        sort_dir = sort_spessore
    elif sort_ubicazione in ('asc', 'desc'):
        sort_field = 'ubicazione'
        sort_dir = sort_ubicazione

    # Costruisci elenco dei materiali da mostrare.  Scorriamo solo le righe
    # radice (bancali o lastre indipendenti) e applichiamo i filtri sia
    # sulla riga stessa che sulle sue eventuali lastre figlie.  Le
    # lastre figlie non vengono visualizzate direttamente nella
    # dashboard ma i bancali che contengono lastre corrispondenti
    # devono essere inclusi.
    display_rows: list[dict] = []
    for r in rows_all:
        # salta le lastre figlie; saranno considerate tramite i loro genitori
        if r['parent_id']:
            continue
        # Applica il filtro vista tipologia
        # Determina la tipologia della riga.  A partire da questa versione,
        # anche le lastre indipendenti (is_pallet=0 e parent_id NULL) vengono
        # trattate come bancali per coerenza con la richiesta di mostrare
        # sempre e solo bancali nella dashboard.  Pertanto, la variabile
        # ``is_pallet`` viene calcolata considerando sia il flag del
        # database che l'assenza di un ``parent_id``.
        is_pallet = bool(r['is_pallet']) or (r['parent_id'] is None)
        is_sfrido = bool(r['is_sfrido'])
        is_indipendente = (not is_pallet) and (not r['parent_id'])
        # Filtraggio per vista
        if view_filter == 'bancali' and not is_pallet:
            continue
        if view_filter == 'sfridi':
            # Mostra bancali che contengono almeno uno sfrido o lastre/sfridi indipendenti
            child_has_sfrido = any(child['is_sfrido'] for child in children_map.get(r['id'], []))
            if not is_sfrido and not child_has_sfrido:
                continue
        if view_filter == 'indipendenti' and not (not is_pallet and not r['parent_id']):
            continue
        # Vista riordinare: includi solo i bancali/lastre che hanno quantita <= soglia
        # e, per i bancali, includi quelli con quantità bassa o quelli che contengono
        # lastre con quantità bassa (anche se di norma le lastre figlie hanno sempre quantita=1).
        if view_filter in ('riordino', 'riordinare'):
            # Determina se questa riga (radice) soddisfa la condizione di riordino
            try:
                qty_val = int(r['quantita']) if r['quantita'] is not None else 0
            except (ValueError, TypeError):
                qty_val = 0
            meets = qty_val <= REORDER_THRESHOLD
            if not meets:
                # se non soddisfa la condizione, verifica se almeno un figlio soddisfa
                for child in children_map.get(r['id'], []):
                    try:
                        cqty = int(child['quantita']) if child['quantita'] is not None else 0
                    except (ValueError, TypeError):
                        cqty = 0
                    if cqty <= REORDER_THRESHOLD:
                        meets = True
                        break
            # Se la riga non soddisfa la soglia, escludila
            if not meets:
                continue
            # Limita alle combinazioni definite manualmente.  La variabile
            # ``manual_keys`` contiene le coppie (materiale, spessore) caricate
            # all'inizio della funzione.  Solo se la combinazione è
            # presente nel catalogo degli articoli mostriamo la riga in
            # questa vista "Da riordinare".
            key_manual = (r['materiale'], r['spessore'] or '')
            if key_manual not in manual_keys:
                continue
        # Filtraggio per materiale, fornitore, ubicazione e ricerca.  È
        # sufficiente che la riga stessa o una qualunque lastra figlia
        # soddisfi il filtro perché il bancale compaia.
        def row_matches(row: sqlite3.Row) -> bool:
            """
            Determina se una singola riga del database soddisfa i criteri di filtro
            selezionati dall'utente. Oltre ai filtri esistenti su materiale,
            fornitore e ubicazione, questa funzione incorpora anche i filtri sulle
            dimensioni massime X e Y. Quando vengono specificati ``max_x`` o
            ``max_y``, il record deve avere valori validi per ``dimensione_x`` e
            ``dimensione_y`` (convertibili in float) che siano inferiori o uguali
            ai limiti indicati. I bancali che non hanno dimensioni vengono
            automaticamente esclusi dai criteri dimensionali, ma possono essere
            inclusi se almeno una lastra figlia soddisfa i filtri (vedi logica
            successiva nel costruttore dell'elenco).
            """
            # Filtro per materiale (scelta dal menu a tendina)
            if materiale_filtro and row['materiale'] != materiale_filtro:
                return False
            # Filtro per tipo.  Confronta la colonna ``tipo`` (stringa o None) con il
            # valore selezionato dal menu a tendina.  Se l'utente ha
            # selezionato un tipo specifico e la riga non corrisponde, la
            # riga viene esclusa.  Normalizziamo eventuali ``None`` o
            # stringhe vuote a stringa vuota.
            if tipo_filtro and ((row['tipo'] or '').strip() != tipo_filtro):
                return False
            # Filtro ricerca parziale sul materiale (campo search)
            if search and search.lower() not in (row['materiale'] or '').lower():
                return False
            # Filtro per fornitore
            if fornitore_filtro and (row['fornitore'] or '').strip() != fornitore_filtro:
                return False
            # Filtro per produttore
            if produttore_filtro and (row['produttore'] or '').strip() != produttore_filtro:
                return False
            # Filtro ubicazione lettera/numero
            if filtro_lettera and (row['ubicazione_lettera'] or '') != filtro_lettera:
                return False
            if filtro_numero:
                try:
                    if int(row['ubicazione_numero']) != int(filtro_numero):
                        return False
                except (ValueError, TypeError):
                    return False
            # Filtro dimensioni con range o valore esatto. Convertiamo dimensione_x e dimensione_y in float se possibile.
            dx_raw = row['dimensione_x']
            dy_raw = row['dimensione_y']
            try:
                dx_val = float(str(dx_raw).replace(',', '.')) if dx_raw not in (None, '', 'None') else None
            except Exception:
                dx_val = None
            try:
                dy_val = float(str(dy_raw).replace(',', '.')) if dy_raw not in (None, '', 'None') else None
            except Exception:
                dy_val = None
            if rng_x is not None:
                if dx_val is None or not (rng_x[0] <= dx_val <= rng_x[1]):
                    return False
            if rng_y is not None:
                if dy_val is None or not (rng_y[0] <= dy_val <= rng_y[1]):
                    return False
            # Filtro spessore con range o valore esatto.
            if rng_sp is not None:
                sp_raw = row['spessore']
                try:
                    sp_val = float(str(sp_raw).replace(',', '.')) if sp_raw not in (None, '', 'None') else None
                except Exception:
                    sp_val = None
                if sp_val is None or not (rng_sp[0] <= sp_val <= rng_sp[1]):
                    return False
            return True
        # Verifica se il bancale/lastra indipendente soddisfa i filtri, oppure
        # se una qualsiasi lastra figlia soddisfa i filtri.
        match_self = row_matches(r)
        match_child = False
        for child in children_map.get(r['id'], []):
            if row_matches(child):
                match_child = True
                break
        if not (match_self or match_child):
            continue
        # Costruiamo un dizionario copiando la riga per poter aggiungere
        # attributi extra (sfrido nei figli, allegati, id padre).  Non
        # modifichiamo direttamente l'oggetto sqlite3.Row che è
        # immutabile.
        new_row = dict(r)
        # Combine location letter/number into a single field for sorting
        letter = new_row.get('ubicazione_lettera') or ''
        num = str(new_row.get('ubicazione_numero') or '')
        new_row['ubicazione'] = f"{letter}{num}"
        # Per l'interfaccia della dashboard forziamo l'attributo ``is_pallet`` a
        # True quando la riga non ha un parent_id, in modo che tutte le
        # lastre indipendenti vengano trattate come bancali (bancale con
        # singola lastra) e visualizzate coerentemente.
        if new_row.get('parent_id') is None:
            new_row['is_pallet'] = 1
        # child_has_sfrido segnala se almeno una lastra figlia è sfrido
        new_row['child_has_sfrido'] = any(child['is_sfrido'] for child in children_map.get(r['id'], []))
        # Colleghiamo gli eventuali documenti caricati.
        # I documenti possono essere legati direttamente al materiale tramite
        # ``material_id`` oppure a una specifica combinazione (materiale,
        # tipo, spessore, dimensione_x, dimensione_y).  Recuperiamo entrambi
        # gli insiemi e li uniamo evitando duplicati.  Normalizziamo
        # l'assenza di valori a stringa vuota per generare la chiave.
        try:
            mat_val = (new_row.get('materiale') or '')
            tipo_val = (new_row.get('tipo') or '')
            sp_val = (new_row.get('spessore') or '')
            dx_val = (new_row.get('dimensione_x') or '')
            dy_val = (new_row.get('dimensione_y') or '')
            # Includiamo anche il produttore nella chiave per recuperare i documenti
            prod_val = (new_row.get('produttore') or '')
            combo_key = (mat_val, tipo_val, sp_val, dx_val, dy_val, prod_val)
            # Documenti caricati specificamente per questo ID di materiale
            docs_material = list(attachments_by_material_id.get(r['id'], []))
            # Documenti caricati per la combinazione dell'anagrafica articolo
            docs_combo = attachments_by_combo.get(combo_key, [])
            # Unisci gli insiemi evitando duplicati (basati sull'ID del documento)
            seen_doc_ids = {d['id'] for d in docs_material}
            for doc in docs_combo:
                if doc['id'] not in seen_doc_ids:
                    docs_material.append(doc)
                    seen_doc_ids.add(doc['id'])
            new_row['attachments'] = docs_material
        except Exception:
            # In caso di errore, ripiega sul comportamento precedente
            new_row['attachments'] = attachments_by_material_id.get(r['id'], [])
        display_rows.append(new_row)

    # Ordina l'elenco in base alla scelta dell'utente mantenendo i gruppi
    # (bancali prima delle lastre indipendenti).  Per l'ordinamento per
    # spessore/quantità ordiniamo le lastre indipendenti.  I bancali sono
    # sempre mostrati prima e non vengono ordinati tra di loro.
    pallets_list: list[dict] = []
    independent_list: list[dict] = []
    for r in display_rows:
        if r['is_pallet']:
            pallets_list.append(r)
        else:
            independent_list.append(r)
    reverse_sort = (sort_dir == 'desc')
    if sort_field:
        if sort_field == 'spessore':
            key_func = lambda r: parse_spessore(r['spessore'])
        elif sort_field == 'ubicazione':
            # Build a combined location string for sorting; fall back gracefully
            key_func = lambda r: parse_ubicazione(r.get('ubicazione') or ((r.get('ubicazione_lettera') or '') + (str(r.get('ubicazione_numero')) if r.get('ubicazione_numero') is not None else '')))
        else:
            key_func = lambda r: parse_quantita(r['quantita'])
        independent_list.sort(key=key_func, reverse=reverse_sort)
        pallets_list.sort(key=key_func, reverse=reverse_sort)
    # Ricombina l'elenco: bancali seguiti da lastre indipendenti
    final_rows = pallets_list + independent_list

    # Se è stato specificato un filtro per ID lastra, limita la lista
    # risultante al solo bancale o lastra indipendente corrispondente.
    if id_lastra_str:
        # Se highlight_root_id è stato determinato, filtriamo per quel
        # bancale.  In caso contrario la lista risulterà vuota (nessun
        # risultato).
        if highlight_root_id is not None:
            final_rows = [r for r in final_rows if r['id'] == highlight_root_id]
        else:
            final_rows = []

    # Calcolo dei riepiloghi e degli ID da riordinare
    # Conteggio sfridi: conta tutti gli sfridi singoli, inclusi quelli
    # contenuti all'interno dei bancali.  Inoltre, per il conteggio
    # riordini, si considera la somma delle quantità per ogni coppia
    # materiale/spessore (indipendentemente dal numero di bancali o
    # lastre indipendenti) e si confronta il totale con la soglia.
    low_stock_count = 0
    total_sfridi = 0
    # Mappa per sommare le quantità totali per ciascuna combinazione
    aggregated_qty_map: dict[tuple[str, str], int] = {}
    # Primo passaggio: calcola sfridi e somma le quantità delle righe radice
    for r in final_rows:
        # Conta sfridi: se la riga è sfrido indipendente incrementa;
        # se il bancale contiene sfridi figli li conta separatamente
        if r['is_sfrido']:
            total_sfridi += 1
        if r.get('child_has_sfrido'):
            for child in children_map.get(r['id'], []):
                if child['is_sfrido']:
                    total_sfridi += 1
        # Per i bancali e le lastre indipendenti (parent_id è NULL) sommiamo la
        # quantità per la combinazione materiale/spessore.  La quantità del
        # bancale rappresenta già il numero di lastre contenute.
        if not r['parent_id']:
            # Ignora le lastre indipendenti contrassegnate come sfrido quando si
            # calcola la somma delle quantità per il riordino.  Il bancale che
            # contiene sfridi non viene considerato sfrido, quindi viene
            # comunque sommato.  Solo le lastre radice con flag ``is_sfrido``
            # vengono saltate.
            if r['is_sfrido']:
                continue
            try:
                qty_val = int(r['quantita']) if r['quantita'] is not None else 0
            except (ValueError, TypeError):
                qty_val = 0
            key = (r['materiale'], r['spessore'] or '')
            aggregated_qty_map[key] = aggregated_qty_map.get(key, 0) + qty_val
    # Secondo passaggio: calcola numero di scorte basse per ogni riga radice
    for r in final_rows:
        if not r['parent_id']:
            try:
                qty_val = int(r['quantita']) if r['quantita'] is not None else 0
            except (ValueError, TypeError):
                qty_val = 0
            if qty_val <= ALERT_THRESHOLD:
                low_stock_count += 1
    # Determina quali combinazioni richiedono riordino confrontando la
    # quantità aggregata con la soglia specifica.  A partire da questa
    # versione l'elenco dei riordini è limitato alle combinazioni
    # definite manualmente nell'anagrafica articoli (tabella
    # ``articoli_catalogo``).  In questo modo sulla dashboard e nella
    # pagina dei riordini verranno segnalate solo le combinazioni
    # inserite esplicitamente dall'utente.
    # A questo punto la variabile ``manual_keys`` è stata popolata
    # all'inizio della funzione.  Costruiamo l'insieme delle combinazioni
    # da riordinare solo se presenti nell'anagrafica articoli.  La
    # variabile ``flagged_keys`` contiene i soli elementi che superano la
    # soglia e sono anche definiti manualmente.
    flagged_keys: set[tuple[str, str]] = set()
    for key, total_q in aggregated_qty_map.items():
        threshold_val = threshold_map.get(key, DEFAULT_REORDER_THRESHOLD)
        if key in manual_keys and total_q <= threshold_val:
            flagged_keys.add(key)
    # Compila la lista degli ID dei bancali/lastre indipendenti da evidenziare.
    reorder_ids: list[int] = []
    for r in final_rows:
        if not r['parent_id']:
            key = (r['materiale'], r['spessore'] or '')
            if key in flagged_keys:
                reorder_ids.append(r['id'])
    # ------------------------------------------------------------------
    # Calcolo del contatore di riordino per la dashboard (versione estesa)
    #
    # A partire da questa revisione il contatore "Da riordinare" deve
    # corrispondere al numero di combinazioni complete (materiale, tipo,
    # spessore, dimensione_x, dimensione_y, produttore) che risultano
    # sotto la relativa soglia di riordino e non sono attualmente in
    # fase di accettazione o preparazione (riordini_rdo).  Per
    # soddisfare questa richiesta, replichiamo la logica della route
    # ``riordini`` in forma ridotta: carichiamo sia le soglie legacy
    # (per terna) sia quelle estese (per combinazione completa), poi
    # iteriamo tutte le combinazioni definite nell'anagrafica articoli
    # includendo il produttore.  Ogni combinazione con giacenza
    # totale inferiore o uguale alla soglia viene conteggiata.
    reorder_rows_count = 0
    try:
        # Carica mappe delle soglie legacy per (materiale, tipo, spessore)
        threshold_map_combo: dict[tuple[str, str, str], int] = {}
        try:
            th_rows = conn.execute(
                f"SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, threshold "
                f"FROM {RIORDINO_SOGGIE_TABLE}"
            ).fetchall()
            for tr in th_rows:
                key = (tr['materiale'], tr['tipo'] or '', tr['spessore'] or '')
                try:
                    threshold_map_combo[key] = int(tr['threshold'])
                except (ValueError, TypeError):
                    threshold_map_combo[key] = DEFAULT_REORDER_THRESHOLD
        except sqlite3.Error:
            threshold_map_combo = {}
        # Carica mappe delle soglie estese per combinazione completa
        threshold_map_ext: dict[tuple[str, str, str, str, str, str], int] = {}
        try:
            ext_rows = conn.execute(
                "SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy, COALESCE(produttore,'') AS prod, "
                "threshold FROM riordino_soglie_ext"
            ).fetchall()
            for er in ext_rows:
                k = (
                    er['materiale'],
                    er['tipo'] or '',
                    er['spessore'] or '',
                    (er['dx'] or '').strip(),
                    (er['dy'] or '').strip(),
                    (er['prod'] or '').strip(),
                )
                try:
                    threshold_map_ext[k] = int(er['threshold'])
                except (ValueError, TypeError):
                    threshold_map_ext[k] = DEFAULT_REORDER_THRESHOLD
        except sqlite3.Error:
            threshold_map_ext = {}
        # Recupera combinazioni attive (in accettazione e RDO) includendo produttore
        active_keys_ext: set[tuple[str, str, str, str, str, str]] = set()
        try:
            act_rows = conn.execute(
                "SELECT COALESCE(materiale,'') AS materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy, COALESCE(produttore,'') AS prod "
                "FROM riordini_accettazione"
            ).fetchall()
            for a in act_rows:
                active_keys_ext.add((
                    a['materiale'] or '',
                    a['tipo'] or '',
                    a['spessore'] or '',
                    (a['dx'] or '').strip(),
                    (a['dy'] or '').strip(),
                    (a['prod'] or '').strip(),
                ))
            # Recupera combinazioni in RDO; campo produttori può contenere lista separata da virgole
            rdo_rows_tmp = conn.execute(
                "SELECT COALESCE(materiale,'') AS materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy, "
                "COALESCE(produttori,'') AS prods, COALESCE(produttore_scelto,'') AS prod_sel "
                "FROM riordini_rdo"
            ).fetchall()
            for r in rdo_rows_tmp:
                mat = (r['materiale'] or '')
                tpv = (r['tipo'] or '')
                spv = (r['spessore'] or '')
                dxv = (r['dx'] or '').strip()
                dyv = (r['dy'] or '').strip()
                prod_sel = (r['prod_sel'] or '').strip()
                if prod_sel:
                    prod_list = [prod_sel]
                else:
                    prods_field = (r['prods'] or '')
                    if prods_field:
                        prod_list = [p.strip() for p in prods_field.split(',') if p and p.strip()]
                        if not prod_list:
                            prod_list = ['']
                    else:
                        prod_list = ['']
                for p in prod_list:
                    active_keys_ext.add((mat, tpv, spv, dxv, dyv, p or ''))
        except sqlite3.Error:
            active_keys_ext = set()
        # Recupera tutte le combinazioni dell'anagrafica articoli includendo produttore
        try:
            catalog_rows = conn.execute(
                "SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy, COALESCE(produttore,'') AS prod "
                "FROM articoli_catalogo"
            ).fetchall()
        except sqlite3.Error:
            catalog_rows = []
        seen_combos_ext: set[tuple[str, str, str, str, str, str]] = set()
        for row in catalog_rows:
            mat = row['materiale']
            tp = (row['tipo'] or '')
            sp = (row['spessore'] or '')
            dx = (row['dx'] or '').strip()
            dy = (row['dy'] or '').strip()
            prod = (row['prod'] or '').strip()
            combo_key = (mat, tp, sp, dx, dy, prod)
            # Evita duplicati
            if combo_key in seen_combos_ext:
                continue
            seen_combos_ext.add(combo_key)
            # Escludi combinazioni attive
            if combo_key in active_keys_ext:
                continue
            # Calcola la quantità totale per questa combinazione includendo il produttore
            total_qty = 0
            try:
                qty_res = conn.execute(
                    "SELECT SUM(quantita) AS tot FROM materiali WHERE materiale=? "
                    "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                    "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                    "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                    "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                    "AND TRIM(COALESCE(produttore,'')) = ? "
                    "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                    (
                        mat,
                        tp or None, tp,
                        sp or None, sp,
                        dx or None, dx,
                        dy or None, dy,
                        prod or '',
                    )
                ).fetchone()
                total_qty = int(qty_res['tot'] or 0) if qty_res and qty_res['tot'] is not None else 0
            except sqlite3.Error:
                total_qty = 0
            # Determina la soglia di riordino per la combinazione completa
            th_val = threshold_map_ext.get(combo_key, None)
            if th_val is None:
                th_val = threshold_map_combo.get((mat, tp, sp), DEFAULT_REORDER_THRESHOLD)
            try:
                # Escludi soglie pari a zero
                if int(th_val) == 0:
                    continue
            except Exception:
                pass
            # Se la giacenza è minore o uguale alla soglia, incrementa il contatore
            try:
                if int(total_qty) <= int(th_val):
                    reorder_rows_count += 1
            except Exception:
                # Ignora conversioni errate
                pass
    except Exception:
        # In caso di errore, usa il numero di combinazioni flagged (vecchio metodo) come fallback
        reorder_rows_count = len(flagged_keys)
    # Imposta reorder_count sulla base del numero di combinazioni effettive da riordinare
    reorder_count = reorder_rows_count
    # Calcola le lastre che rispettano le dimensioni massime e gli altri
    # filtri (materiale, fornitore, ubicazione, ecc.). Se almeno uno dei
    # filtri di dimensione è specificato, andiamo a popolare l'elenco
    # `filtered_slabs` con tutti i record che non sono bancali e che
    # soddisfano le stesse condizioni applicate nella dashboard (row_matches).
    filtered_slabs: list[dict] = []
    # Popola l'elenco delle lastre se l'utente ha specificato almeno un filtro dimensionale.
    if rng_x is not None or rng_y is not None:
        for r in rows_all:
            # Consideriamo solo le lastre (sia figlie che indipendenti). I
            # bancali stessi non vengono inclusi nell'elenco delle lastre.
            if r['is_pallet']:
                continue
            # Verifichiamo se la riga soddisfa tutti i filtri tramite
            # row_matches. Questo include i filtri dimensionali.
            try:
                matches = row_matches(r)
            except Exception:
                matches = False
            if matches:
                filtered_slabs.append(dict(r))

    conn.close()
    lettere = [chr(l) for l in range(ord('A'), ord('Z') + 1)]
    numeri = list(range(1, 100))
    materiali_list = get_materiali_vocabolario()
    suppliers = get_fornitori_vocabolario()
    produttori = get_produttori_vocabolario()
    # Ottieni l'elenco dei tipi di lavorazione/materiale dal dizionario.  Ogni
    # elemento è un dizionario con chiavi ``id`` e ``nome``; utilizziamo
    # direttamente l'elenco per popolare il menu a tendina del filtro "Tipo"
    # nella dashboard.  In caso di problemi (es. tabella assente) la
    # funzione restituisce una lista vuota.
    tipi = get_tipi_vocabolario()
    # Definisci una variabile compatibile per il template.  In precedenza
    # ``attachments_by_material`` veniva popolata con i documenti per
    # ciascun ID materiale.  Dopo l'introduzione di
    # ``attachments_by_material_id`` manteniamo la stessa variabile per
    # retrocompatibilità assegnandola direttamente.
    attachments_by_material = attachments_by_material_id

    # Genera l'elenco delle ubicazioni esistenti per il menu di spostamento.
    # Esegue una query per recuperare tutte le combinazioni distinte di lettera e numero
    # presenti nella tabella materiali.  I valori NULL vengono filtrati e la lista è
    # ordinata alfabeticamente per lettera e numero.  In caso di errore, la lista resta vuota.
    try:
        with get_db_connection() as conn_loc:
            loc_rows = conn_loc.execute(
                "SELECT DISTINCT COALESCE(ubicazione_lettera,'') AS L, COALESCE(ubicazione_numero,0) AS N "
                "FROM materiali WHERE ubicazione_lettera IS NOT NULL AND ubicazione_numero IS NOT NULL "
                "ORDER BY L, N"
            ).fetchall()
        location_options = [f"{row['L']}-{row['N']}" for row in loc_rows if row['L'] and row['N']]
    except Exception:
        location_options = []
    # Prepara una versione serializzabile dei materiali finali per la modalità
    # "semplifica".  Include i campi essenziali (materiale, tipo, spessore,
    # produttore, id, quantita, ubicazione_lettera, ubicazione_numero, is_sfrido)
    # per consentire il filtraggio lato client senza richiamare il server.
    try:
        materials_list_for_json = []
        for r in final_rows:
            # sqlite3.Row supporta accesso tipo dizionario, ma alcune chiavi
            # potrebbero essere None; normalizziamo a stringhe vuote
            materials_list_for_json.append({
                'id': int(r['id']),
                'materiale': r['materiale'] or '',
                'tipo': r['tipo'] or '',
                'spessore': r['spessore'] or '',
                'produttore': r['produttore'] or '',
                'quantita': r['quantita'] if 'quantita' in r.keys() else 0,
                'ubicazione_lettera': r['ubicazione_lettera'] or '',
                'ubicazione_numero': r['ubicazione_numero'] or '',
                'is_sfrido': bool(r['is_sfrido']) if 'is_sfrido' in r.keys() else False,
                # Indica se la riga è un bancale (pallet).  In caso di bancale la
                # chiave parent_id sarà None.  Questo ci permette di creare
                # correttamente il link "Dettagli bancale" nella ricerca rapida.
                'is_pallet': bool(r['is_pallet']) if 'is_pallet' in r.keys() else False,
                'parent_id': (int(r['parent_id']) if r['parent_id'] is not None else None) if 'parent_id' in r.keys() else None
            })
        import json
        materials_json = json.dumps(materials_list_for_json)
    except Exception:
        materials_json = '[]'

    return render_template(
        'dashboard.html',
        title='Magazzino',
        materiali=final_rows,
        attachments_by_material=attachments_by_material,
        search=search,
        filtro_lettera=filtro_lettera,
        filtro_numero=filtro_numero,
        # Combined location filter (for easier selection)
        filtro_ubicazione=filtro_ubicazione,
        sort_spessore=sort_spessore,
        sort_qty=sort_qty,
        sort_ubicazione=sort_ubicazione,
        view_filter=view_filter,
        materiale_filtro=materiale_filtro,
        lettere=lettere,
        numeri=numeri,
        low_stock_count=low_stock_count,
        reorder_count=reorder_count,
        total_sfridi=total_sfridi,
        reorder_ids=reorder_ids,
        materiali_list=materiali_list,
        suppliers=suppliers,
        produttori=produttori,
        tipi=tipi,
        fornitore_filtro=fornitore_filtro,
        produttore_filtro=produttore_filtro,
        tipo_filtro=tipo_filtro,
        alert_threshold=ALERT_THRESHOLD,
        max_x=max_x_str,
        max_y=max_y_str,
        spessore=spessore_str,
        # Lista di ubicazioni da proporre nella datalist per lo spostamento
        location_options=location_options,
        filtered_slabs=filtered_slabs,
        reserved_ids=list(reserved_ids) if 'reserved_ids' in locals() else [],
        reserved_highlight_ids=list(reserved_highlight_ids) if 'reserved_highlight_ids' in locals() else [],
        # Parametri per la ricerca per ID lastra.  highlight_child_id
        # identifica la lastra da evidenziare nella pagina di dettaglio,
        # highlight_root_id identifica il bancale o la lastra indipendente
        # contenente tale lastra.
        highlight_child_id=highlight_child_id,
        highlight_root_id=highlight_root_id,
        # JSON serializzato dei materiali per il filtro semplificato
        materials_json=materials_json
    )


@app.route('/add', methods=['GET', 'POST'])
def add():
    """Aggiunge un nuovo bancale o lastra al magazzino.

    L'interfaccia permette di:
    * creare un nuovo bancale con più lastre: lascia vuoto il campo "Bancale"
      e inserisci una quantità maggiore di 1. Verrà creato un record padre
      (bancale) e i relativi record figli (lastre).
    * aggiungere una singola lastra ad un bancale esistente: seleziona il
      bancale dal menu a tendina e lascia la quantità a 1. La lastra verrà
      contrassegnata come figlia tramite ``parent_id``.
    * creare una lastra indipendente: non selezionare alcun bancale e
      imposta la quantità a 1.

    Ogni lastra prevede due dimensioni (X e Y) e un flag "sfrido" per
    indicare gli sfridi. Se un bancale contiene delle lastre sfrido,
    queste verranno indicate sulla dashboard con un pallino rosso.
    """
    # Possibili ubicazioni per filtri
    lettere = [chr(l) for l in range(ord('A'), ord('Z') + 1)]
    numeri = list(range(1, 100))
    # Carica l'elenco dei bancali esistenti (is_pallet=1) per consentire
    # l'inserimento di lastre figlie. Sono ordinati per ID.
    with get_db_connection() as conn:
        pallets = conn.execute(
            "SELECT id, ubicazione_lettera, ubicazione_numero, materiale, quantita FROM materiali WHERE is_pallet=1 ORDER BY id"
        ).fetchall()
    # Carica l'elenco dei tipi di lavorazione per popolare il menu a tendina
    tipi_list = get_tipi_vocabolario()
    # Gestione precompilazione per accettazione: se arrivano parametri via GET
    acc_id_param = request.args.get('acc_id')
    q_parziale_param = request.args.get('q_parziale')
    q_totale_new_param = request.args.get('q_totale_new')
    prefill_materiale = request.args.get('materiale') or None
    prefill_tipo = request.args.get('tipo') or None
    prefill_spessore = request.args.get('spessore') or None
    prefill_dx = request.args.get('dimensione_x') or None
    prefill_dy = request.args.get('dimensione_y') or None
    # Precompilazione di fornitore e produttore: se presenti nel query string
    prefill_fornitore = request.args.get('fornitore') or None
    prefill_produttore = request.args.get('produttore') or None
    # Precompilazione da parent_id: se presente e non stiamo gestendo un'accettazione,
    # recupera il materiale padre e preimposta i campi del modulo (materiale, tipo, spessore, dimensioni e ubicazione).
    # Inoltre accettiamo eventuali valori di ubicazione passati tramite query string (da accettazione_update).
    # In tal caso, tali valori prevalgono sulla precompilazione derivata dal bancale padre.
    prefill_ubic_lettera = request.args.get('ubicazione_lettera') or None
    # number may come as string, attempt to parse
    _tmp_ubic_numero = request.args.get('ubicazione_numero')
    if _tmp_ubic_numero not in (None, '', 'None'):
        try:
            prefill_ubic_numero = int(_tmp_ubic_numero)
        except (TypeError, ValueError):
            prefill_ubic_numero = None
    else:
        prefill_ubic_numero = None
    prefill_parent_id = None
    parent_id_param = request.args.get('parent_id')
    # Non sovrascrivere le precompilazioni provenienti da accettazione (acc_id_param).
    if parent_id_param and not acc_id_param:
        try:
            parent_id_int = int(parent_id_param)
        except (TypeError, ValueError):
            parent_id_int = None
        if parent_id_int:
            with get_db_connection() as conn:
                try:
                    row = conn.execute("SELECT * FROM materiali WHERE id=?", (parent_id_int,)).fetchone()
                except Exception:
                    row = None
            if row:
                # Imposta i campi precompilati basandosi sul materiale padre se non forniti da query string.
                if not prefill_materiale:
                    prefill_materiale = row['materiale']
                if not prefill_tipo and row['tipo']:
                    prefill_tipo = row['tipo']
                if not prefill_spessore and row['spessore']:
                    prefill_spessore = row['spessore']
                if not prefill_dx and row['dimensione_x']:
                    prefill_dx = row['dimensione_x']
                if not prefill_dy and row['dimensione_y']:
                    prefill_dy = row['dimensione_y']
                # Ubicazione viene sempre impostata, per selezionare automaticamente la lettera/numero.
                # Non sovrascrivere eventuali valori passati via query string (da accettazione_update).
                if prefill_ubic_lettera is None:
                    prefill_ubic_lettera = row['ubicazione_lettera']
                if prefill_ubic_numero is None:
                    prefill_ubic_numero = row['ubicazione_numero']
                # Seleziona il bancale padre nel menu a tendina
                prefill_parent_id = parent_id_int
    # Normalizza i valori numerici da stringa
    try:
        prefill_quantita = int(q_parziale_param) if q_parziale_param not in (None, '', 'None') else None
    except (TypeError, ValueError):
        prefill_quantita = None
    try:
        prefill_totale = int(q_totale_new_param) if q_totale_new_param not in (None, '', 'None') else None
    except (TypeError, ValueError):
        prefill_totale = None

    if request.method == 'POST':
        materiale = request.form.get('materiale', '').strip()
        tipo_val = request.form.get('tipo', '').strip()
        # Dimensioni separate X e Y per la lastra
        dimensione_x = request.form.get('dimensione_x', '').strip()
        dimensione_y = request.form.get('dimensione_y', '').strip()
        # Costruiamo la stringa legacy "dimensioni" nel formato XxY per compatibilità
        dimensioni = f"{dimensione_x}x{dimensione_y}" if dimensione_x and dimensione_y else ''
        spessore = request.form.get('spessore', '').strip()
        try:
            quantita = int(request.form.get('quantita', '1'))
        except (ValueError, TypeError):
            quantita = 1
        ubicazione_lettera = request.form.get('ubicazione_lettera', '')
        try:
            ubicazione_numero = int(request.form.get('ubicazione_numero', '0'))
        except (ValueError, TypeError):
            ubicazione_numero = 0
        fornitore = request.form.get('fornitore', '').strip()
        # Recupera il produttore selezionato o digitato dall'utente.  Questo
        # campo è facoltativo e permette di tenere traccia dell'origine
        # del materiale.  Viene salvato anche se non appartiene al
        # dizionario dei produttori (l'utente potrà in seguito
        # aggiungerlo o correggerlo dal dizionario).
        produttore = request.form.get('produttore', '').strip()
        note = request.form.get('note', '').strip()
        is_sfrido = 1 if request.form.get('sfrido') else 0
        # ID del bancale padre se si sta inserendo una lastra figlia
        parent_id_raw = request.form.get('parent_id', '')
        parent_id = None
        if parent_id_raw:
            try:
                parent_id = int(parent_id_raw)
            except ValueError:
                parent_id = None
        # Verifica campi obbligatori: tutti i campi (eccetto flag sfrido, note e bancale) sono richiesti.
        # In particolare, è necessario fornire: materiale, tipo, dimensioni X/Y, spessore, ubicazione,
        # fornitore e produttore.  Se uno di questi manca, mostra un messaggio di errore.
        if (
            not materiale or not dimensione_x or not dimensione_y or not spessore or
            not ubicazione_lettera or not ubicazione_numero or not tipo_val or not fornitore or not produttore
        ):
            flash('Compila tutti i campi obbligatori: materiale, tipo, dimensioni X/Y, spessore, fornitore, produttore e ubicazione.', 'danger')
            return redirect(url_for('add'))

        # ------------------------------------------------------------------
        # Gestione produttore non presente nel vocabolario
        # Se l'utente ha specificato un produttore che non esiste ancora
        # nel dizionario dei produttori, lo inseriamo automaticamente.
        if produttore:
            try:
                with get_db_connection() as conn:
                    # Verifica se esiste già un produttore con lo stesso nome (case sensitive)
                    exists = conn.execute(
                        f"SELECT 1 FROM {PRODUTTORE_TABLE} WHERE nome=? LIMIT 1",
                        (produttore,)
                    ).fetchone()
                    if not exists:
                        conn.execute(
                            f"INSERT INTO {PRODUTTORE_TABLE} (nome) VALUES (?)",
                            (produttore,)
                        )
            except Exception:
                # In caso di errore nell'accesso al database ignoriamo l'inserimento
                pass

        # Se il materiale non appartiene al vocabolario e l'utente ha richiesto esplicitamente di aggiungerlo, inseriscilo nel dizionario
        if request.form.get('aggiungi_materiale_vocab'):
            with get_db_connection() as conn:
                try:
                    conn.execute(f"INSERT INTO {VOCAB_TABLE} (nome) VALUES (?)", (materiale,))
                    flash('Materiale aggiunto al dizionario!', 'success')
                except sqlite3.IntegrityError:
                    # Se esiste già non facciamo nulla
                    pass
        # Prima di procedere con l'inserimento effettivo, verifica se si tratta di un
        # nuovo ordine.  Se l'utente ha attivato il toggle "nuovo_ordine" nel
        # formulario, non vengono creati i bancali/lastre immediatamente: invece
        # viene registrata una riga nella tabella riordini_accettazione per
        # tenere traccia dell'ordine in attesa di arrivo.  In tal caso saltiamo
        # tutte le logiche successive di inserimento materiale e reindirizziamo
        # alla pagina Riordini.
        if request.form.get('nuovo_ordine'):
            # Data di arrivo prevista dal form (obbligatoria se nuovo ordine è selezionato)
            data_arrivo = request.form.get('data_arrivo', '').strip()
            if not data_arrivo:
                flash('Seleziona una data di arrivo prevista per il nuovo ordine.', 'danger')
                return redirect(url_for('add'))
            # Inserisci una riga di accettazione.  Usiamo la data corrente per
            # identificare la creazione dell'ordine e salviamo la data prevista
            # come data_arrivo.  La quantità totale corrisponde alla quantità
            # indicata nel modulo e la quantità ricevuta inizialmente è zero.  Il
            # numero_ordine viene lasciato NULL per indicare che si tratta di un
            # ordine manuale.  Salviamo anche il fornitore, il produttore e
            # l'ubicazione inserita dall'utente per precompilare la fase di
            # accettazione.
            try:
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            try:
                with get_db_connection() as conn:
                    conn.execute(
                        "INSERT INTO riordini_accettazione (data, materiale, tipo, spessore, dimensione_x, dimensione_y, quantita_totale, quantita_ricevuta, numero_ordine, fornitore, produttore, data_prevista, ubicazione_lettera, ubicazione_numero) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)",
                        (
                            now_str,
                            materiale,
                            tipo_val if tipo_val else None,
                            spessore,
                            dimensione_x,
                            dimensione_y,
                            quantita,
                            None,
                            fornitore if fornitore else None,
                            produttore if produttore else None,
                            data_arrivo,
                            ubicazione_lettera if ubicazione_lettera else None,
                            ubicazione_numero if ubicazione_numero else None,
                        ),
                    )
                flash('Nuovo ordine creato. Sarà mostrato nella sezione di accettazione.', 'success')
            except Exception as e:
                # In caso di errore nella creazione dell'ordine mostriamo un messaggio
                flash('Errore durante la creazione del nuovo ordine: {}'.format(e), 'danger')
            # Reindirizza alla pagina riordini per visualizzare l'ordine in attesa di accettazione
            return redirect(url_for('riordini'))

        # Elenco degli ID delle lastre appena create da utilizzare per mostrare i QR
        # Prima di procedere con l'inserimento effettivo verifichiamo se nella stessa
        # ubicazione è presente un altro bancale con materiale differente.  In questo
        # caso chiediamo all'utente di confermare l'override oppure di scegliere
        # un'altra ubicazione.  Il flag ``confirm_override`` viene impostato dal
        # template di conferma e ci consente di saltare questo controllo al secondo
        # invio del modulo.
        created_ids: list[int] = []

        # Verifica se l'utente sta aggiungendo un nuovo bancale/lastre senza
        # specificare un bancale padre. In tal caso controlliamo se esiste già
        # un bancale in questa ubicazione con un materiale diverso.  Se sì e
        # ``confirm_override`` non è impostato, mostriamo una pagina di
        # conferma.  In caso di conferma continuiamo normalmente creando un
        # nuovo bancale per il nuovo materiale nella stessa ubicazione.
        confirm_override = request.form.get('confirm_override')
        # Solo se non stiamo aggiungendo una lastra figlia (parent_id è None)
        if not parent_id:
            try:
                with get_db_connection() as check_conn:
                    row = check_conn.execute(
                        "SELECT materiale FROM materiali WHERE is_pallet=1 AND ubicazione_lettera=? AND ubicazione_numero=? LIMIT 1",
                        (ubicazione_lettera, ubicazione_numero)
                    ).fetchone()
                # Se trovata una ubicazione occupata da un materiale diverso e non è stato confermato l'override
                if row and row[0] != materiale and not confirm_override:
                    # Passiamo tutti i dati del form alla pagina di conferma in modo
                    # da poterli ripristinare in caso l'utente decida di proseguire.
                    form_data = request.form.to_dict(flat=True)
                    return render_template(
                        'conferma_ubicazione.html',
                        existing_materiale=row[0],
                        form_data=form_data
                    )
            except Exception:
                # Se il controllo fallisce (es. tabella non esistente) proseguiamo con l'inserimento.
                pass

        with get_db_connection() as conn:
            if parent_id:
                # Regola: un bancale non può avere spessori diversi.
                # Verifica che il bancale padre esista, sia marcato come pallet e abbia spessore coerente.
                try:
                    par_row = conn.execute("SELECT id, is_pallet, spessore FROM materiali WHERE id=?", (parent_id,)).fetchone()
                except sqlite3.Error:
                    par_row = None
                if not par_row or int(par_row['is_pallet'] or 0) != 1:
                    flash('Il bancale selezionato non è valido.', 'danger')
                    return redirect(url_for('add'))
                parent_sp = (par_row['spessore'] or '').strip()
                # Se il bancale ha già uno spessore assegnato e non coincide con quello corrente, mostra un errore.
                if parent_sp and parent_sp != spessore:
                    flash('Regola violata: un bancale non può avere spessori diversi.', 'danger')
                    return redirect(url_for('add'))
                # Se il bancale non ha ancora uno spessore assegnato, impostalo allo spessore corrente.
                if not parent_sp and spessore:
                    conn.execute("UPDATE materiali SET spessore=? WHERE id=?", (spessore, parent_id))
                # Inserimento di una lastra figlia ad un bancale o ad una lastra indipendente.
                # Creiamo il nuovo record figlio e associamo parent_id al materiale padre.
                cur = conn.execute(
                    "INSERT INTO materiali (materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                    (
                        materiale,
                        tipo_val if tipo_val else None,
                        dimensioni,
                        dimensione_x,
                        dimensione_y,
                        spessore,
                        1,
                        ubicazione_lettera,
                        ubicazione_numero,
                        fornitore,
                        produttore,
                        note,
                        parent_id,
                        is_sfrido,
                    )
                )
                new_id = cur.lastrowid
                created_ids.append(new_id)
                # Aggiorniamo la quantità del padre e contrassegniamolo come bancale.
                # Se il padre era una singola lastra (is_pallet=0), lo trasformiamo in bancale (is_pallet=1).
                conn.execute(
                    "UPDATE materiali SET quantita = quantita + 1, is_pallet = 1 WHERE id = ?",
                    (parent_id,)
                )
                flash('Lastra aggiunta al bancale con successo!', 'success')
            else:
                # Se non è stato specificato un bancale padre (parent_id), verifichiamo
                # se nella stessa ubicazione esiste già un bancale e se il suo materiale
                # corrisponde a quello che stiamo inserendo.  Solo in tal caso
                # utilizziamo il bancale esistente come contenitore per le nuove lastre;
                # altrimenti creiamo un nuovo bancale separato per il nuovo materiale.
                # Verifica se esiste già un bancale nella stessa ubicazione che corrisponde esattamente
                # alla combinazione di materiale, tipo, spessore, fornitore e produttore specificata.
                # Recupera tutti i bancali esistenti nella stessa ubicazione.
                # In precedenza veniva selezionato solo il primo bancale (LIMIT 1),
                # ma se in una singola ubicazione sono presenti più bancali con
                # materiali diversi potremmo selezionare quello errato.  Qui
                # recuperiamo tutti i bancali nell'ubicazione e valutiamo uno ad uno
                # per trovare un match esatto tra materiale, tipo, spessore, fornitore
                # e produttore.
                pallet_rows = conn.execute(
                    "SELECT id, materiale, tipo, spessore, fornitore, produttore "
                    "FROM materiali WHERE is_pallet=1 AND ubicazione_lettera=? AND ubicazione_numero=? "
                    "ORDER BY id",
                    (ubicazione_lettera, ubicazione_numero)
                ).fetchall()
                use_existing_pallet = False
                pallet_id = None
                # Funzione di normalizzazione utilizzata per confrontare stringhe e gestire None/str vuota
                def _norm(x) -> str:
                    return str(x).strip() if x is not None else ''
                # Itera su tutti i bancali in questa ubicazione
                for pallet_row in pallet_rows:
                    # Estrai i valori dal record esistente
                    row_id = pallet_row['id'] if isinstance(pallet_row, sqlite3.Row) else pallet_row[0]
                    row_materiale = pallet_row['materiale'] if isinstance(pallet_row, sqlite3.Row) else pallet_row[1]
                    row_tipo = pallet_row['tipo'] if isinstance(pallet_row, sqlite3.Row) else pallet_row[2]
                    row_spessore = pallet_row['spessore'] if isinstance(pallet_row, sqlite3.Row) else pallet_row[3]
                    row_fornitore = pallet_row['fornitore'] if isinstance(pallet_row, sqlite3.Row) else pallet_row[4]
                    row_produttore = pallet_row['produttore'] if isinstance(pallet_row, sqlite3.Row) else pallet_row[5]
                    # Confronta ogni campo con il valore che stiamo inserendo; considera stringhe vuote e None equivalenti
                    if (
                        _norm(row_materiale) == _norm(materiale)
                        and _norm(row_tipo) == _norm(tipo_val)
                        and _norm(row_spessore) == _norm(spessore)
                        and _norm(row_fornitore) == _norm(fornitore)
                        and _norm(row_produttore) == _norm(produttore)
                    ):
                        use_existing_pallet = True
                        pallet_id = row_id
                        # Interrompe l'iterazione dopo aver trovato il primo match
                        break

                if use_existing_pallet:
                    # Aggiorniamo la quantità del bancale esistente sommando il numero di lastre che stiamo aggiungendo.
                    conn.execute(
                        "UPDATE materiali SET quantita = quantita + ? WHERE id = ?",
                        (max(quantita, 1), pallet_id)
                    )
                    # Inseriamo le nuove lastre come figli del bancale esistente.
                    for _ in range(max(quantita, 1)):
                        cur_child = conn.execute(
                            "INSERT INTO materiali (materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                            " VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0)",
                            (
                                materiale,
                                tipo_val if tipo_val else None,
                                dimensioni,
                                dimensione_x,
                                dimensione_y,
                                spessore,
                                ubicazione_lettera,
                                ubicazione_numero,
                                fornitore,
                                produttore,
                                note,
                                pallet_id,
                                is_sfrido,
                            )
                        )
                        created_ids.append(cur_child.lastrowid)
                    flash('Lastre aggiunte al bancale esistente!', 'success')
                else:
                    # Non esiste un bancale nella stessa ubicazione: seguiamo la
                    # logica originaria. Se la quantità è maggiore di 1 creiamo un
                    # nuovo bancale con più lastre; altrimenti creiamo un bancale con
                    # una singola lastra.
                    if quantita > 1:
                        # Creazione di un bancale con più lastre: inseriamo il record padre
                        cur = conn.execute(
                            "INSERT INTO materiali (materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 1)",
                            (
                                materiale,
                                tipo_val if tipo_val else None,
                                dimensioni,
                                '',
                                '',
                                spessore,
                                quantita,
                                ubicazione_lettera,
                                ubicazione_numero,
                                fornitore,
                                produttore,
                                note,
                            )
                        )
                        pallet_id = cur.lastrowid
                        # Inseriamo le lastre figlie e raccogliamo i loro ID
                        for _ in range(quantita):
                            cur_child = conn.execute(
                                "INSERT INTO materiali (materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                                " VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0)",
                                (
                                    materiale,
                                    tipo_val if tipo_val else None,
                                    dimensioni,
                                    dimensione_x,
                                    dimensione_y,
                                    spessore,
                                    ubicazione_lettera,
                                    ubicazione_numero,
                                    fornitore,
                                    produttore,
                                    note,
                                    pallet_id,
                                    is_sfrido,
                                )
                            )
                            created_ids.append(cur_child.lastrowid)
                        flash('Bancale e lastre aggiunti con successo!', 'success')
                    else:
                        # Inserimento di una singola lastra indipendente
                        #
                        # Con la nuova logica tutte le lastre vengono sempre inserite all'interno di un
                        # bancale dedicato. Anche quando l'utente specifica una quantità pari a 1 e
                        # non seleziona alcun bancale padre, creiamo comunque un record di tipo
                        # "bancale" (is_pallet=1) che conterrà la singola lastra come figlia. In
                        # questo modo sulla dashboard saranno visibili solo i bancali e le lastre
                        # saranno sempre associate a un contenitore.
                        #
                        # Creiamo quindi il record del bancale (senza dimensioni X/Y) con la
                        # quantità impostata al numero di lastre da contenere. La quantità del
                        # bancale rappresenta infatti il numero di lastre associate.
                        cur = conn.execute(
                            "INSERT INTO materiali (materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                            " VALUES (?, ?, ?, '', '', ?, ?, ?, ?, ?, ?, ?, NULL, 0, 1)",
                            (
                                materiale,
                                tipo_val if tipo_val else None,
                                dimensioni,
                                spessore,
                                max(quantita, 1),
                                ubicazione_lettera,
                                ubicazione_numero,
                                fornitore,
                                produttore,
                                note,
                            )
                        )
                        pallet_id = cur.lastrowid
                        # Inseriamo le lastre figlie (una o più) con i valori di dimensione e
                        # spessore inseriti dall'utente. Ciascuna lastra avrà quantita=1 e
                        # riferimento al bancale appena creato.  Raccolti gli ID per mostrare
                        # successivamente i QR code.
                        for _ in range(max(quantita, 1)):
                            cur_child = conn.execute(
                                "INSERT INTO materiali (materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                                " VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0)",
                                (
                                    materiale,
                                    tipo_val if tipo_val else None,
                                    dimensioni,
                                    dimensione_x,
                                    dimensione_y,
                                    spessore,
                                    ubicazione_lettera,
                                    ubicazione_numero,
                                    fornitore,
                                    produttore,
                                    note,
                                    pallet_id,
                                    is_sfrido,
                                )
                            )
                            created_ids.append(cur_child.lastrowid)
                        flash('Bancale e lastra aggiunti con successo!', 'success')
            # Recuperiamo i record creati per la pagina di conferma QR
            # Prima di leggere gli ID appena inseriti e registrare gli eventi
            # eseguiamo un commit esplicito.  Senza questo commit i record
            # appena aggiunti potrebbero non essere visibili da una nuova
            # connessione utilizzata da ``log_slab_events`` e da altre query.
            try:
                conn.commit()
            except Exception:
                # Se il commit fallisce continuiamo comunque: l'uscita dal
                # context manager eseguirà un commit automatico
                pass
            if created_ids:
                placeholders = ','.join(['?'] * len(created_ids))
                q = f"SELECT * FROM materiali WHERE id IN ({placeholders})"
                rows = conn.execute(q, created_ids).fetchall()
                # Registra gli eventi di aggiunta per ciascuna lastra creata (non bancale) in batch
                events_to_add = []
                for r in rows:
                    try:
                        is_pallet_val = int(r['is_pallet'] or 0)
                    except Exception:
                        is_pallet_val = 0
                    if is_pallet_val == 1:
                        continue
                    events_to_add.append({
                        'slab_id': r['id'],
                        'event_type': 'aggiunto',
                        'from_letter': None,
                        'from_number': None,
                        'to_letter': r['ubicazione_lettera'],
                        'to_number': r['ubicazione_numero'],
                        'dimensione_x': r['dimensione_x'],
                        'dimensione_y': r['dimensione_y'],
                        'spessore': r['spessore'],
                        'materiale': r['materiale'],
                        'tipo': r['tipo'],
                        'fornitore': r['fornitore'],
                        'produttore': r['produttore'],
                        'note': r['note'],
                        'nesting_link': None,
                    })
                if events_to_add:
                    log_slab_events(events_to_add)
                # Una volta registrati gli eventi, memorizziamo anche gli ID delle
                # lastre create nel database nascosto dedicato.  In questo modo
                # l'identificativo di una lastra non verrà mai riutilizzato
                # successivamente.  Eventuali errori vengono ignorati per non
                # interrompere l'elaborazione dell'inserimento.
                try:
                    record_used_ids(created_ids)
                except Exception:
                    pass
                # ------------------------------------------------------------------
                # Copia i documenti associati alla combinazione (material_id=0) sul bancale e su tutte le
                # lastre figlie (sia nuove che già esistenti).  In questo modo i documenti caricati tramite
                # il pulsante DOCS nell'anagrafica articoli vengono salvati come documenti del bancale e
                # vengono ereditati da tutte le lastre.  Gli eventuali errori vengono ignorati per non
                # interrompere l'inserimento.
                try:
                    # Normalizza la chiave della combinazione a stringhe
                    combo_materiale = (materiale or '').strip()
                    combo_tipo = (tipo_val or '').strip()
                    combo_spessore = (spessore or '').strip()
                    combo_dx = (dimensione_x or '').strip()
                    combo_dy = (dimensione_y or '').strip()
                    combo_produttore = (produttore or '').strip()
                    # Determina l'ID del bancale di destinazione: se esistente (parent_id) o appena creato (pallet_id)
                    dest_pallet_id = None
                    try:
                        if 'parent_id' in locals() and parent_id:
                            dest_pallet_id = parent_id
                    except Exception:
                        pass
                    try:
                        if (dest_pallet_id is None) and 'pallet_id' in locals() and pallet_id:
                            dest_pallet_id = pallet_id
                    except Exception:
                        pass
                    if dest_pallet_id:
                        with get_db_connection() as doc_conn:
                            # Recupera tutti i documenti caricati per la combinazione
                            docs_combo = doc_conn.execute(
                                "SELECT id, filename, original_name FROM documenti WHERE material_id=0 AND COALESCE(materiale,'')=? AND COALESCE(tipo,'')=? AND COALESCE(spessore,'')=? AND COALESCE(dimensione_x,'')=? AND COALESCE(dimensione_y,'')=? AND COALESCE(produttore,'')=?",
                                (combo_materiale, combo_tipo, combo_spessore, combo_dx, combo_dy, combo_produttore)
                            ).fetchall()
                            # Costruisci la lista di tutte le lastre figlie da aggiornare (nuove + esistenti)
                            child_ids_to_update: list[int] = []
                            # aggiungi le lastre appena create
                            for new_id in created_ids:
                                if new_id not in child_ids_to_update:
                                    child_ids_to_update.append(new_id)
                            # aggiungi le lastre già esistenti collegate al bancale
                            try:
                                existing_children = doc_conn.execute("SELECT id FROM materiali WHERE parent_id=?", (dest_pallet_id,)).fetchall()
                            except Exception:
                                existing_children = []
                            for row in existing_children:
                                try:
                                    cid = row['id'] if isinstance(row, sqlite3.Row) else row[0]
                                except Exception:
                                    continue
                                if cid not in child_ids_to_update:
                                    child_ids_to_update.append(cid)
                            # Prepara l'insieme dei nomi originali dei documenti della combinazione
                            combo_orig_names: set[str] = set()
                            for doc_row in docs_combo:
                                try:
                                    oname = doc_row['original_name'] if isinstance(doc_row, sqlite3.Row) else doc_row[2]
                                except Exception:
                                    oname = None
                                if oname:
                                    combo_orig_names.add(oname)

                            # Replica ciascun documento della combinazione sul bancale e su ciascuna lastra
                            for doc_row in docs_combo:
                                src_rel = doc_row['filename'] if isinstance(doc_row, sqlite3.Row) else doc_row[1]
                                original_name = doc_row['original_name'] if isinstance(doc_row, sqlite3.Row) else doc_row[2]
                                src_path = os.path.join(UPLOAD_FOLDER, src_rel)
                                try:
                                    with open(src_path, 'rb') as sf:
                                        file_bytes = sf.read()
                                except Exception:
                                    continue
                                _, ext = os.path.splitext(src_rel)
                                ext = ext.lower()
                                # Salva sul bancale come documento 'materiale'
                                try:
                                    rel_dest_p = save_file_to_id(file_bytes, ext, dest_pallet_id, doc_type='materiale')
                                    doc_conn.execute(
                                        "INSERT INTO documenti (material_id, filename, original_name) VALUES (?, ?, ?)",
                                        (dest_pallet_id, rel_dest_p, original_name)
                                    )
                                except Exception:
                                    pass
                                # Salva su tutte le lastre figlie come documento 'pallet'
                                for cid in child_ids_to_update:
                                    try:
                                        rel_dest_c = save_file_to_id(file_bytes, ext, cid, doc_type='pallet')
                                        doc_conn.execute(
                                            "INSERT INTO documenti (material_id, filename, original_name) VALUES (?, ?, ?)",
                                            (cid, rel_dest_c, original_name)
                                        )
                                    except Exception:
                                        pass
                            # Replica i documenti già presenti sul bancale su ogni nuova lastra appena creata, saltando quelli appena inseriti per la combinazione
                            if created_ids:
                                docs_parent = doc_conn.execute(
                                    "SELECT id, filename, original_name FROM documenti WHERE material_id=?",
                                    (dest_pallet_id,)
                                ).fetchall()
                                for doc_row in docs_parent:
                                    src_rel = doc_row['filename'] if isinstance(doc_row, sqlite3.Row) else doc_row[1]
                                    original_name = doc_row['original_name'] if isinstance(doc_row, sqlite3.Row) else doc_row[2]
                                    # Evita di duplicare i documenti appena replicati dalla combinazione
                                    try:
                                        if original_name in combo_orig_names:
                                            continue
                                    except Exception:
                                        pass
                                    src_path = os.path.join(UPLOAD_FOLDER, src_rel)
                                    try:
                                        with open(src_path, 'rb') as sf:
                                            file_bytes = sf.read()
                                    except Exception:
                                        continue
                                    _, ext = os.path.splitext(src_rel)
                                    ext = ext.lower()
                                    for cid in created_ids:
                                        try:
                                            rel_dest = save_file_to_id(file_bytes, ext, cid, doc_type='pallet')
                                            doc_conn.execute(
                                                "INSERT INTO documenti (material_id, filename, original_name) VALUES (?, ?, ?)",
                                                (cid, rel_dest, original_name)
                                            )
                                        except Exception:
                                            pass
                            try:
                                doc_conn.commit()
                            except Exception:
                                pass
                except Exception:
                    pass
            else:
                rows = []
        # Aggiorna la riga di accettazione se l'inserimento deriva da una conferma
        acc_id_form = request.form.get('acc_id')
        if acc_id_form:
            try:
                acc_id_int = int(acc_id_form)
            except (TypeError, ValueError):
                acc_id_int = None
            # Quantità totale da aggiornare, se fornita (può essere stringa)
            acc_q_totale_new_raw = request.form.get('acc_q_totale_new')
            new_total_val = None
            if acc_q_totale_new_raw not in (None, '', 'None'):
                try:
                    nt = int(acc_q_totale_new_raw)
                    if nt > 0:
                        new_total_val = nt
                except (ValueError, TypeError):
                    new_total_val = None
            # Quantità effettivamente inserita con il form
            try:
                accepted_qty = int(request.form.get('quantita', '0'))
            except (TypeError, ValueError):
                accepted_qty = 0
            with get_db_connection() as conn2:
                # Assicurati che la tabella dei blocchi esista
                try:
                    conn2.execute(
                        "CREATE TABLE IF NOT EXISTS riordini_bloccati (\n"
                        "  materiale TEXT NOT NULL,\n"
                        "  tipo TEXT NOT NULL,\n"
                        "  spessore TEXT NOT NULL,\n"
                        "  dimensione_x TEXT NOT NULL,\n"
                        "  dimensione_y TEXT NOT NULL,\n"
                        "  blocked INTEGER DEFAULT 1,\n"
                        "  PRIMARY KEY(materiale, tipo, spessore, dimensione_x, dimensione_y)\n"
                        ")"
                    )
                except sqlite3.Error:
                    pass
                row = conn2.execute(
                    "SELECT * FROM riordini_accettazione WHERE id=?",
                    (acc_id_int,)
                ).fetchone()
                if row:
                    q_totale = int(row['quantita_totale'] or 0)
                    q_ricevuta = int(row['quantita_ricevuta'] or 0)
                    # Aggiorna totale se fornito
                    if new_total_val is not None:
                        q_totale = new_total_val
                        conn2.execute(
                            "UPDATE riordini_accettazione SET quantita_totale=? WHERE id=?",
                            (q_totale, acc_id_int)
                        )

                    # Aggiorna ricevuto con la quantità inserita
                    if accepted_qty > 0:
                        q_ricevuta += accepted_qty
                        conn2.execute(
                            "UPDATE riordini_accettazione SET quantita_ricevuta=? WHERE id=?",
                            (q_ricevuta, acc_id_int)
                        )
                        # Aggiorna fornitore, produttore e ubicazione per la riga di accettazione.
                        # Se fornitore e produttore sono stati specificati nel form, sovrascrivono i
                        # valori eventualmente presenti nella riga di accettazione.  L'ubicazione
                        # viene impostata solo se non è ancora stata registrata, al fine di
                        # memorizzare la ubicazione della prima accettazione.
                        # Determina i nuovi valori per fornitore e produttore: se non specificati
                        # nel form utilizza i valori esistenti della riga di accettazione.
                        new_fornitore_val = fornitore if fornitore else (row['fornitore'] if row['fornitore'] else None)
                        new_produttore_val = produttore if produttore else (row['produttore'] if row['produttore'] else None)
                        # Aggiorna la riga di accettazione con i nuovi valori e salva l'ubicazione solo se non già definita
                        try:
                            conn2.execute(
                                "UPDATE riordini_accettazione SET fornitore=?, produttore=?, "
                                "ubicazione_lettera = COALESCE(ubicazione_lettera, ?), "
                                "ubicazione_numero = COALESCE(ubicazione_numero, ?) WHERE id=?",
                                (
                                    new_fornitore_val,
                                    new_produttore_val,
                                    ubicazione_lettera,
                                    ubicazione_numero,
                                    acc_id_int
                                )
                            )
                        except sqlite3.Error:
                            pass
                        # Registra ogni accettazione (anche parziale) nello storico degli ordini
                        order_code = row['numero_ordine'] if 'numero_ordine' in row.keys() else None
                        conn2.execute(
                            "INSERT INTO riordini_effettuati (material_id, data, quantita, materiale, tipo, spessore, fornitore, produttore, dimensione_x, dimensione_y, tipo_evento, numero_ordine) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                None,
                                datetime.now().isoformat(sep=' ', timespec='seconds'),
                                accepted_qty,
                                row['materiale'],
                                row['tipo'],
                                row['spessore'],
                                new_fornitore_val,
                                new_produttore_val,
                                row['dimensione_x'],
                                row['dimensione_y'],
                                'accettazione',
                                order_code
                            )
                        )
                    conn2.commit()
                    # Se la ricezione è completa, rimuovi la riga di accettazione senza registrare un evento aggiuntivo.
                    # Gli eventi di accettazione parziale sono già stati registrati singolarmente.
                    if q_totale > 0 and q_ricevuta >= q_totale:
                        # Elimina la riga di accettazione; la combinazione potrà riapparire per un nuovo riordino se necessario
                        conn2.execute(
                            "DELETE FROM riordini_accettazione WHERE id=?",
                            (acc_id_int,)
                        )
                        conn2.commit()
                        flash('Ordine accettato completamente.', 'success')
                    else:
                        flash('Stato di accettazione aggiornato.', 'success')

        # Se ci sono nuovi record, mostriamo la pagina con i QR; altrimenti torniamo alla dashboard
        if created_ids:
            # Costruiamo l'elenco degli ID come stringa separata da virgole per l'esportazione in PDF
            pdf_ids = ','.join(str(i) for i in created_ids)
            # Costruiamo una mappa degli allegati per ciascuna lastra appena creata.
            # Questo consente di mostrare nella pagina "QR generati" un pulsante per la
            # gestione dei documenti con il conteggio degli allegati esistenti.
            attachments_map: dict[int, list] = {}
            # Raccolta degli ID dei bancali a cui appartengono le lastre appena create
            pallet_ids_set: set[int] = set()
            for r in rows:
                try:
                    attachments_map[r['id']] = get_attachments(r['id'])
                except Exception:
                    attachments_map[r['id']] = []
                # Se la lastra ha un parent_id, aggiungilo alla lista dei bancali
                try:
                    pid = r['parent_id']
                    if pid:
                        pallet_ids_set.add(pid)
                except Exception:
                    pass
            # Costruisci la mappa degli allegati per ciascun bancale
            attachments_pallet_map: dict[int, list] = {}
            for pid in pallet_ids_set:
                try:
                    attachments_pallet_map[pid] = get_attachments(pid)
                except Exception:
                    attachments_pallet_map[pid] = []
            # Converti il set di bancali in una lista ordinata per l'iterazione nel template
            pallet_ids = sorted(pallet_ids_set)
            return render_template(
                'added.html',
                title='QR generati',
                created=rows,
                pdf_ids=pdf_ids,
                attachments_map=attachments_map,
                attachments_pallet_map=attachments_pallet_map,
                pallet_ids=pallet_ids
            )
        return redirect(url_for('dashboard'))
    # Render della pagina di inserimento: passiamo la lista materiali, i fornitori, le tipologie e
    # l'elenco dei bancali esistenti per consentire l'aggancio di lastre.  Recuperiamo
    # l'elenco aggiornato dei materiali, dei fornitori e dei tipi dal vocabolario.
    materiali_list = get_materiali_vocabolario()
    fornitori_list = get_fornitori_vocabolario()
    produttori_list = get_produttori_vocabolario()
    tipi_list = get_tipi_vocabolario()
    # Recupera i valori distinti per dimensione X, Y e spessore per popolare i datalist.
    with get_db_connection() as conn:
        try:
            dim_x_rows = conn.execute("SELECT DISTINCT dimensione_x FROM materiali WHERE dimensione_x IS NOT NULL AND TRIM(dimensione_x) != '' ORDER BY CAST(dimensione_x AS INT)").fetchall()
            dimensione_x_list = [row['dimensione_x'] for row in dim_x_rows if row['dimensione_x'] is not None]
        except sqlite3.Error:
            dimensione_x_list = []
        try:
            dim_y_rows = conn.execute("SELECT DISTINCT dimensione_y FROM materiali WHERE dimensione_y IS NOT NULL AND TRIM(dimensione_y) != '' ORDER BY CAST(dimensione_y AS INT)").fetchall()
            dimensione_y_list = [row['dimensione_y'] for row in dim_y_rows if row['dimensione_y'] is not None]
        except sqlite3.Error:
            dimensione_y_list = []
        try:
            sp_rows = conn.execute("SELECT DISTINCT spessore FROM materiali WHERE spessore IS NOT NULL AND TRIM(spessore) != '' ORDER BY CAST(spessore AS INT)").fetchall()
            spessori_list = [row['spessore'] for row in sp_rows if row['spessore'] is not None]
        except sqlite3.Error:
            spessori_list = []
    return render_template(
        'add.html',
        title='Aggiungi materiale',
        lettere=lettere,
        numeri=numeri,
        materiali_list=materiali_list,
        pallets=pallets,
        fornitori_list=fornitori_list,
        produttori_list=produttori_list,
        tipi_list=tipi_list,
        dimensione_x_list=dimensione_x_list,
        dimensione_y_list=dimensione_y_list,
        spessori_list=spessori_list,
        # Variabili di precompilazione per accettazione
        prefill_materiale=prefill_materiale,
        prefill_tipo=prefill_tipo,
        prefill_spessore=prefill_spessore,
        prefill_dx=prefill_dx,
        prefill_dy=prefill_dy,
        prefill_quantita=prefill_quantita,
        prefill_totale=prefill_totale,
        acc_id=acc_id_param,
        # Variabili di precompilazione per fornitore e produttore
        prefill_fornitore=prefill_fornitore,
        prefill_produttore=prefill_produttore,
        # Variabili di precompilazione per ubicazione e bancale da un materiale padre
        prefill_ubic_lettera=prefill_ubic_lettera,
        prefill_ubic_numero=prefill_ubic_numero,
        prefill_parent_id=prefill_parent_id
    )


@app.route('/materiale/<int:material_id>')
def dettaglio(material_id: int):
    """Mostra il dettaglio di un singolo materiale.

    Accetta opzionalmente il parametro ``highlight_id`` nella query string
    per indicare la lastra figlia che deve essere evidenziata nella
    tabella.  Questo viene utilizzato quando l'utente filtra per ID
    lastra nella dashboard e quindi naviga ai dettagli del bancale.
    """
    # Leggi l'ID della lastra da evidenziare (se presente) dalla
    # query string.  Usiamo ``type=int`` per convertire automaticamente
    # la stringa in intero o ``None`` se il parametro non esiste.
    highlight_id = request.args.get('highlight_id', type=int)

    conn = get_db_connection()
    materiale = conn.execute("SELECT * FROM materiali WHERE id=?", (material_id,)).fetchone()
    if not materiale:
        conn.close()
        flash('Materiale non trovato!', 'danger')
        return redirect(url_for('dashboard'))
    # Se il materiale è un bancale (is_pallet=1) recupera tutte le sue lastre figlie
    children = []
    try:
        if materiale['is_pallet']:
            children = conn.execute(
                "SELECT * FROM materiali WHERE parent_id=? ORDER BY id", (material_id,)
            ).fetchall()
    except Exception:
        # In caso di errore (ad esempio se la colonna non esiste) ignora i figli
        children = []
    # Convertiamo children in una lista di dict per un confronto più
    # semplice in Jinja.  Alcuni motori di template hanno problemi
    # nell'accesso ai campi numerici via sqlite3.Row quando si usa
    # l'operatore ``==``.
    try:
        children_list = [dict(c) for c in children]
    except Exception:
        children_list = children
    conn.close()
    # Costruiamo una stringa di ID dei figli per la stampa multipla.  Se
    # ``children`` è vuoto la stringa risulterà vuota.
    child_ids = [child['id'] for child in children] if children else []
    child_ids_str = ','.join(str(c) for c in child_ids)
    # Determina l'insieme delle lastre attualmente prenotate per disabilitare
    # il pulsante di prenotazione sulle lastre figlie.  Se si verifica
    # un errore nella lettura del DB verrà passato un elenco vuoto al template.
    try:
        reserved_ids_det = get_reserved_material_ids()
    except Exception:
        reserved_ids_det = set()
    # Recupera gli allegati per il materiale da visualizzare nel dettaglio
    try:
        attachments = get_attachments(material_id)
    except Exception:
        attachments = []
    return render_template(
        'dettaglio.html',
        title='Dettaglio',
        materiale=materiale,
        children=children_list,
        child_ids_str=child_ids_str,
        reserved_ids=list(reserved_ids_det),
        attachments=attachments,
        highlight_id=highlight_id
    )


@app.route('/edit/<int:material_id>', methods=['GET', 'POST'])
def edit(material_id: int):
    """Modifica i dettagli di un materiale."""
    conn = get_db_connection()
    materiale = conn.execute("SELECT * FROM materiali WHERE id=?", (material_id,)).fetchone()
    if not materiale:
        conn.close()
        flash('Materiale non trovato!', 'danger')
        return redirect(url_for('dashboard'))
    lettere = [chr(l) for l in range(ord('A'), ord('Z') + 1)]
    numeri = list(range(1, 100))
    if request.method == 'POST':
        materiale_val = request.form.get('materiale', '').strip()
        tipo_val = request.form.get('tipo', '').strip()
        # Raccogli dimensioni X/Y e costruisci la stringa legacy ``dimensioni``
        dim_x = request.form.get('dimensione_x', '').strip()
        dim_y = request.form.get('dimensione_y', '').strip()
        if dim_x and dim_y:
            dimensioni = f"{dim_x}x{dim_y}"
        else:
            dimensioni = ''
        spessore = request.form.get('spessore', '').strip()
        try:
            quantita = int(request.form.get('quantita', materiale['quantita']))
        except (ValueError, TypeError):
            quantita = materiale['quantita']
        ubicazione_lettera = request.form.get('ubicazione_lettera', materiale['ubicazione_lettera'])
        try:
            ubicazione_numero = int(request.form.get('ubicazione_numero', materiale['ubicazione_numero']))
        except (ValueError, TypeError):
            ubicazione_numero = materiale['ubicazione_numero']
        fornitore = request.form.get('fornitore', '').strip()
        # Campo produttore: consente di aggiornare l'origine del materiale.  Può essere
        # vuoto o contenere un valore non presente nel dizionario dei produttori.
        produttore = request.form.get('produttore', '').strip()
        note = request.form.get('note', '').strip()
        is_sfrido = 1 if request.form.get('sfrido') else 0
        # Verifica se stiamo trasformando la lastra in sfrido
        prev_is_sfrido = materiale['is_sfrido'] if 'is_sfrido' in materiale.keys() else 0
        # Aggiornamento del record. Non modifichiamo ``parent_id`` né
        # ``is_pallet`` per evitare incoerenze. Aggiorniamo il tipo
        # utilizzando NULL quando il campo è vuoto.
        conn.execute(
            "UPDATE materiali SET materiale=?, tipo=?, dimensioni=?, dimensione_x=?, dimensione_y=?, spessore=?, quantita=?, ubicazione_lettera=?, ubicazione_numero=?, fornitore=?, produttore=?, note=?, is_sfrido=? WHERE id=?",
            (materiale_val, tipo_val if tipo_val else None, dimensioni, dim_x, dim_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, is_sfrido, material_id)
        )
        conn.commit()
        conn.close()
        # Se la lastra era normale e ora è stata marcata come sfrido, registriamo l'evento
        try:
            if int(prev_is_sfrido or 0) == 0 and int(is_sfrido or 0) == 1:
                log_slab_event(
                    slab_id=material_id,
                    event_type='sfrido',
                    from_letter=materiale['ubicazione_lettera'],
                    from_number=materiale['ubicazione_numero'],
                    to_letter=materiale['ubicazione_lettera'],
                    to_number=materiale['ubicazione_numero'],
                    dimensione_x=dim_x or materiale['dimensione_x'],
                    dimensione_y=dim_y or materiale['dimensione_y'],
                    spessore=spessore or materiale['spessore'],
                    materiale=materiale_val or materiale['materiale'],
                    tipo=tipo_val or materiale['tipo'],
                    fornitore=fornitore or materiale['fornitore'],
                    produttore=produttore or materiale['produttore'],
                    note=note
                )
        except Exception:
            pass
        # Registra sempre un evento 'modificato' dopo l'aggiornamento (anche se sfrido).  Questo consente
        # di tracciare le modifiche di dati come materiale, tipo, dimensioni, spessore, note,
        # ubicazione o fornitori/produttori.  Usiamo come posizione di partenza le coordinate
        # precedenti e come destinazione le nuove coordinate.
        try:
            # Convert the sqlite3.Row to a plain dict so we can safely use `.get()`.
            try:
                matd = dict(materiale)
            except Exception:
                # Fallback: build a dict using key access if direct conversion fails
                matd = {k: materiale[k] for k in materiale.keys()} if materiale else {}
            log_slab_events([
                {
                    'slab_id': material_id,
                    'event_type': 'modificato',
                    # Pre-move location comes from the previous values stored in the DB
                    'from_letter': matd.get('ubicazione_lettera'),
                    'from_number': matd.get('ubicazione_numero'),
                    # Destination location uses the new values submitted via the form
                    'to_letter': ubicazione_lettera,
                    'to_number': ubicazione_numero,
                    # For each attribute, prefer the new value if provided, falling back to the old one
                    'dimensione_x': dim_x or matd.get('dimensione_x'),
                    'dimensione_y': dim_y or matd.get('dimensione_y'),
                    'spessore': spessore or matd.get('spessore'),
                    'materiale': materiale_val or matd.get('materiale'),
                    'tipo': tipo_val or matd.get('tipo'),
                    'fornitore': fornitore or matd.get('fornitore'),
                    'produttore': produttore or matd.get('produttore'),
                    'note': note or matd.get('note'),
                    'nesting_link': None,
                }
            ])
        except Exception:
            # If something goes wrong during history logging, swallow the exception to avoid interrupting user flow
            pass
        flash('Materiale modificato con successo!', 'success')
        return redirect(url_for('dettaglio', material_id=material_id))
    conn.close()
    # Passiamo l'elenco dei materiali, dei fornitori e dei tipi disponibili al template per le select/datalist
    materiali_list = get_materiali_vocabolario()
    fornitori_list = get_fornitori_vocabolario()
    produttori_list = get_produttori_vocabolario()
    tipi_list = get_tipi_vocabolario()
    return render_template(
        'edit.html',
        title='Modifica materiale',
        materiale=materiale,
        lettere=lettere,
        numeri=numeri,
        materiali_list=materiali_list,
        fornitori_list=fornitori_list,
        produttori_list=produttori_list,
        tipi_list=tipi_list
    )


@app.route('/aggiorna_quantita/<int:material_id>', methods=['POST'])
def aggiorna_quantita(material_id: int):
    """Aggiorna la quantità di un materiale."""
    try:
        nuova_quantita = int(request.form['nuova_quantita'])
    except (ValueError, TypeError):
        nuova_quantita = 0
    # Aggiorna la quantità del materiale e registra l'evento di modifica.
    with get_db_connection() as conn:
        conn.execute("UPDATE materiali SET quantita=? WHERE id=?", (nuova_quantita, material_id))
        # Commit esplicito per assicurare visibilità dell'update nella query successiva
        try:
            conn.commit()
        except Exception:
            pass
    # Recupera i dettagli aggiornati del materiale per registrare lo storico
    try:
        with get_db_connection() as conn2:
            mat_row = conn2.execute("SELECT * FROM materiali WHERE id=?", (material_id,)).fetchone()
    except Exception:
        mat_row = None
    # Registra l'evento di modifica solo se l'ID corrisponde a una lastra (non bancale)
    if mat_row:
        # Convert the returned sqlite3.Row to a dict for safe .get usage
        try:
            matd = dict(mat_row)
        except Exception:
            matd = {k: mat_row[k] for k in mat_row.keys()} if mat_row else {}
        try:
            is_p = int(matd.get('is_pallet') or 0)
        except Exception:
            is_p = 0
        if is_p == 0:
            try:
                log_slab_events([
                    {
                        'slab_id': material_id,
                        'event_type': 'modificato',
                        'from_letter': matd.get('ubicazione_lettera'),
                        'from_number': matd.get('ubicazione_numero'),
                        'to_letter': matd.get('ubicazione_lettera'),
                        'to_number': matd.get('ubicazione_numero'),
                        'dimensione_x': matd.get('dimensione_x'),
                        'dimensione_y': matd.get('dimensione_y'),
                        'spessore': matd.get('spessore'),
                        'materiale': matd.get('materiale'),
                        'tipo': matd.get('tipo'),
                        'fornitore': matd.get('fornitore'),
                        'produttore': matd.get('produttore'),
                        'note': matd.get('note'),
                        'nesting_link': None,
                    }
                ])
            except Exception:
                pass
    flash('Quantità aggiornata!', 'success')
    return redirect(url_for('dettaglio', material_id=material_id))

# ---------------------------------------------------------------------------
# Aggiornamento note direttamente dal dettaglio
@app.route('/update_note/<int:material_id>', methods=['POST'])
def update_note(material_id: int):
    """
    Aggiorna il campo note di un materiale (bancale o lastra) direttamente dalla pagina di dettaglio.
    L'utente può inserire il testo e premere Invio per confermare.
    """
    # Recupera la nota inviata dal form
    note = request.form.get('note', '').strip()
    # Aggiorna il record del materiale con la nuova nota
    # e recupera il record aggiornato per eventuale log
    with get_db_connection() as conn:
        conn.execute("UPDATE materiali SET note=? WHERE id=?", (note, material_id))
        # Commit esplicito per assicurare che l'update sia visibile subito
        try:
            conn.commit()
        except Exception:
            pass
        # Recupera il record aggiornato per registrare lo storico
        try:
            mat_row = conn.execute("SELECT * FROM materiali WHERE id=?", (material_id,)).fetchone()
        except Exception:
            mat_row = None
    # Registra un evento di modifica nello storico lastre
    try:
        if mat_row:
            # Converti la Row in un dizionario per uso comodo con .get()
            try:
                matd = dict(mat_row)
            except Exception:
                matd = {k: mat_row[k] for k in mat_row.keys()} if mat_row else {}
            log_slab_events([
                {
                    'slab_id': material_id,
                    'event_type': 'modificato',
                    'from_letter': matd.get('ubicazione_lettera'),
                    'from_number': matd.get('ubicazione_numero'),
                    'to_letter': matd.get('ubicazione_lettera'),
                    'to_number': matd.get('ubicazione_numero'),
                    'dimensione_x': matd.get('dimensione_x'),
                    'dimensione_y': matd.get('dimensione_y'),
                    'spessore': matd.get('spessore'),
                    'materiale': matd.get('materiale'),
                    'tipo': matd.get('tipo'),
                    'fornitore': matd.get('fornitore'),
                    'produttore': matd.get('produttore'),
                    'note': note or matd.get('note'),
                    'nesting_link': None,
                }
            ])
    except Exception:
        # Ignora eventuali errori nel logging per non interrompere il flusso principale
        pass
    flash('Note aggiornate con successo!', 'success')
    return redirect(url_for('dettaglio', material_id=material_id))


@app.route('/remove_material', methods=['POST'])
def remove_material():
    """Elimina un materiale dal magazzino."""
    material_id_raw = request.form.get('material_id')
    if not material_id_raw:
        flash('Errore nella rimozione.', 'danger')
        return redirect(url_for('dashboard'))
    try:
        material_id = int(material_id_raw)
    except ValueError:
        flash('Errore nella rimozione.', 'danger')
        return redirect(url_for('dashboard'))
    with get_db_connection() as conn:
        materiale = conn.execute("SELECT * FROM materiali WHERE id=?", (material_id,)).fetchone()
        if not materiale:
            flash('Materiale non trovato.', 'danger')
            return redirect(url_for('dashboard'))

        # Flag che indica se, al termine, dovranno essere cancellate le prenotazioni specifiche
        # per l'ID scansionato.  Per le prenotazioni generiche viene impostato a False poiché
        # la cancellazione avviene separatamente.
        remove_specific_reservations = True

        # Se la lastra non è un bancale, verifica se può essere rimossa solo in presenza
        # di una prenotazione (specifica o generica).  Se non esiste alcuna
        # prenotazione corrispondente viene impedita la rimozione.
        try:
            is_pallet_flag = int(materiale['is_pallet'] or 0)
        except Exception:
            is_pallet_flag = 0
        if is_pallet_flag == 0:
            parent_id = materiale['parent_id']
            allowed_generic = False
            allowed_specific = False
            gen_rows = []
            # Verifica prenotazioni specifiche per l'ID corrente
            try:
                # Recupera solo le prenotazioni specifiche (is_generic=0) per la lastra corrente.  Le prenotazioni generiche
                # vengono gestite separatamente più avanti.
                spec_rows = conn.execute(
                    f"SELECT id FROM {PRENOTAZIONI_TABLE} WHERE material_id=? AND is_generic=0",
                    (material_id,)
                ).fetchall()
            except Exception:
                spec_rows = []
            if spec_rows:
                allowed_specific = True
            # Se non c'è una prenotazione specifica, verifica se c'è una prenotazione generica per il bancale
            if not allowed_specific and parent_id:
                try:
                    gen_rows = conn.execute(
                        f"""SELECT p.id, p.material_id FROM {PRENOTAZIONI_TABLE} p
                            JOIN materiali m ON p.material_id = m.id
                            WHERE p.is_generic=1 AND m.parent_id=?""",
                        (parent_id,)
                    ).fetchall()
                except Exception:
                    gen_rows = []
                if gen_rows:
                    allowed_generic = True
            # Se nessuna prenotazione corrisponde, blocca la rimozione
            if not allowed_specific and not allowed_generic:
                flash('La lastra non è prenotata e non può essere rimossa tramite scansione.', 'warning')
                return redirect(url_for('dashboard'))
            # Se c'è una prenotazione generica valida, elimina la relativa riga in prenotazioni
            # e non cancellare le prenotazioni specifiche dell'ID scansionato più avanti.
            if allowed_generic:
                # Se tra le prenotazioni generiche c'è quella relativa alla lastra attualmente
                # scansionata, rimuoviamo quella. Altrimenti eliminiamo la prima trovata.
                target_pren_id = None
                for row in gen_rows:
                    try:
                        if row['material_id'] == material_id:
                            target_pren_id = row['id']
                            break
                    except Exception:
                        continue
                if target_pren_id is None:
                    try:
                        target_pren_id = gen_rows[0]['id']
                    except Exception:
                        target_pren_id = None
                if target_pren_id is not None:
                    try:
                        conn.execute(
                            f"DELETE FROM {PRENOTAZIONI_TABLE} WHERE id=?",
                            (target_pren_id,)
                        )
                        conn.commit()
                    except Exception:
                        pass
                # Non rimuovere le prenotazioni specifiche legate a questo ID quando si esegue la rimozione
                remove_specific_reservations = False
        # Se è un bancale eliminiamo anche tutte le lastre figlie
        if materiale['is_pallet']:
            # Per i bancali rimuoviamo anche tutte le lastre figlie e i loro documenti
            # Oltre a cancellare i record dei materiali, eliminiamo i file fisici e
            # i record nella tabella documenti.
            # Recupera gli ID delle lastre figlie
            cur_children = conn.execute("SELECT id FROM materiali WHERE parent_id=?", (material_id,)).fetchall()
            child_ids = [row['id'] for row in cur_children]
            # Costruisci una lista di tutti i materiali da rimuovere (pallet + figli)
            to_remove = child_ids + [material_id]
            # Recupera i documenti associati a questi materiali
            if to_remove:
                placeholders = ','.join(['?'] * len(to_remove))
                docs = conn.execute(
                    f"SELECT id, filename FROM documenti WHERE material_id IN ({placeholders})",
                    to_remove
                ).fetchall()
                # Elimina i file fisici
                for d in docs:
                    file_path = os.path.join(UPLOAD_FOLDER, d['filename'])
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                    except Exception:
                        pass
                # Elimina i record dei documenti
                conn.execute(f"DELETE FROM documenti WHERE material_id IN ({placeholders})", to_remove)
            # Prima di eliminare lastre e bancale registriamo gli eventi di rimozione per ogni lastra figlia
            # Recuperiamo anche i dettagli delle lastre figlie per lo storico
            detailed_children = []
            if child_ids:
                placeholders_det = ','.join(['?'] * len(child_ids))
                det_rows = conn.execute(
                    f"SELECT * FROM materiali WHERE id IN ({placeholders_det})",
                    child_ids
                ).fetchall()
                for dr in det_rows:
                    detailed_children.append(dict(dr))
            # Crea un batch di eventi di rimozione per le lastre figlie (esclude i bancali)
            events_rm_children: list[dict] = []
            for info in detailed_children:
                try:
                    is_p = int(info.get('is_pallet') or 0)
                except Exception:
                    is_p = 0
                if is_p == 1:
                    continue
                events_rm_children.append({
                    'slab_id': info['id'],
                    'event_type': 'rimosso',
                    'from_letter': info.get('ubicazione_lettera'),
                    'from_number': info.get('ubicazione_numero'),
                    'to_letter': None,
                    'to_number': None,
                    'dimensione_x': info.get('dimensione_x'),
                    'dimensione_y': info.get('dimensione_y'),
                    'spessore': info.get('spessore'),
                    'materiale': info.get('materiale'),
                    'tipo': info.get('tipo'),
                    'fornitore': info.get('fornitore'),
                    'produttore': info.get('produttore'),
                    'note': info.get('note'),
                    'nesting_link': None,
                })
            if events_rm_children:
                # Effettuiamo un commit prima di registrare gli eventi di rimozione. In questo modo
                # eventuali modifiche pendenti sul database (come l'aggiornamento delle quantità del
                # bancale) vengono rese visibili alla connessione utilizzata da log_slab_events.
                try:
                    conn.commit()
                except Exception:
                    pass
                log_slab_events(events_rm_children)
            # Rimuovi lastre figlie e il bancale
            conn.execute("DELETE FROM materiali WHERE parent_id=?", (material_id,))
            conn.execute("DELETE FROM materiali WHERE id=?", (material_id,))
            # Cancella eventuali prenotazioni collegate al bancale e alle sue lastre
            if to_remove:
                placeholders_rm = ','.join(['?'] * len(to_remove))
                conn.execute(
                    f"DELETE FROM {PRENOTAZIONI_TABLE} WHERE material_id IN ({placeholders_rm})",
                    to_remove
                )
            flash('Bancale e relative lastre rimossi con successo!', 'success')
        else:
            parent_id = materiale['parent_id']
            # Rimuoviamo la singola lastra
            # Se stiamo rimuovendo la lastra a fronte di una prenotazione specifica,
            # cancelliamo prima le relative prenotazioni per evitare errori di foreign key.
            if remove_specific_reservations:
                try:
                    conn.execute(
                        f"DELETE FROM {PRENOTAZIONI_TABLE} WHERE material_id=?",
                        (material_id,)
                    )
                except Exception:
                    pass
            # Prima eliminiamo eventuali documenti associati
            docs = conn.execute("SELECT id, filename FROM documenti WHERE material_id=?", (material_id,)).fetchall()
            for d in docs:
                file_path = os.path.join(UPLOAD_FOLDER, d['filename'])
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception:
                    pass
            conn.execute("DELETE FROM documenti WHERE material_id=?", (material_id,))
            # Registriamo l'evento di rimozione per la lastra tramite batch (singolo elemento)
            try:
                # Commit prima di loggare per assicurare visibilità dei dati rimossi
                try:
                    conn.commit()
                except Exception:
                    pass
                # Convert the sqlite3.Row to a plain dict for safe .get usage
                try:
                    matd = dict(materiale)
                except Exception:
                    matd = {k: materiale[k] for k in materiale.keys()} if materiale else {}
                log_slab_events([
                    {
                        'slab_id': matd.get('id') or materiale['id'],
                        'event_type': 'rimosso',
                        'from_letter': matd.get('ubicazione_lettera'),
                        'from_number': matd.get('ubicazione_numero'),
                        'to_letter': None,
                        'to_number': None,
                        'dimensione_x': matd.get('dimensione_x'),
                        'dimensione_y': matd.get('dimensione_y'),
                        'spessore': matd.get('spessore'),
                        'materiale': matd.get('materiale'),
                        'tipo': matd.get('tipo'),
                        'fornitore': matd.get('fornitore'),
                        'produttore': matd.get('produttore'),
                        'note': matd.get('note'),
                        'nesting_link': None,
                    }
                ])
            except Exception:
                pass
            conn.execute("DELETE FROM materiali WHERE id=?", (material_id,))
            # Se esiste un bancale padre decrementiamo il conteggio delle lastre
            if parent_id:
                conn.execute("UPDATE materiali SET quantita = CASE WHEN quantita > 0 THEN quantita - 1 ELSE 0 END WHERE id=?", (parent_id,))
            flash('Lastra rimossa con successo!', 'success')
        # NOTA: non azzeriamo più la sequenza AUTOINCREMENT di SQLite.
        # In precedenza, dopo l'eliminazione di un bancale o di una lastra
        # veniva rimossa l'entry corrispondente nella tabella interna
        # ``sqlite_sequence`` per la tabella ``materiali``.  Questo faceva sì
        # che, alla successiva inserzione, SQLite potesse riutilizzare
        # l'identificativo appena liberato (in particolare se l'ultima
        # riga della tabella veniva cancellata).  Per garantire che ogni
        # lastra o bancale mantenga un ID unico e che non venga mai
        # riutilizzato, non manipoliamo più la tabella ``sqlite_sequence``.
        # Lasciando intatta la sequenza, l'opzione AUTOINCREMENT assicura
        # che gli ID continuino a crescere anche dopo eliminazioni,
        # preservando l'univocità nel tempo.
    return redirect(url_for('dashboard'))

# ---------------------------------------------------------------------------
# Endpoint per fornire informazioni sul materiale in formato JSON
#
# Questo endpoint viene utilizzato dalla pagina di scarico per recuperare
# le informazioni essenziali relative a un materiale (ID, nome del materiale,
# dimensioni e spessore) dato il suo identificativo.  Tali informazioni
# vengono poi utilizzate per comporre un messaggio di conferma prima di
# procedere con la rimozione tramite scansione del QR code.
@app.route('/material_info/<int:material_id>')
def material_info(material_id: int):
    """
    Restituisce informazioni di base per un materiale in formato JSON.

    La risposta contiene l'ID, il materiale, la dimensione X, la dimensione Y
    e lo spessore del materiale.  Se il record non viene trovato, viene
    restituito un codice HTTP 404 con un messaggio di errore.

    :param material_id: identificativo del materiale
    :return: risposta JSON con chiave ``success`` e sottochiave ``data``
    """
    # Recupera i dati richiesti dal database.  Il contesto ``with`` si
    # occupa di chiudere la connessione al termine.
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT id, materiale, dimensione_x, dimensione_y, spessore FROM materiali WHERE id=?",
            (material_id,)
        ).fetchone()
    # Se il materiale non esiste restituiamo 404 con successo=False
    if not row:
        return jsonify({'success': False, 'error': 'Materiale non trovato.'}), 404
    # Costruisci il dizionario dati sostituendo valori None con stringhe vuote
    data = {
        'id': row['id'],
        'materiale': row['materiale'] or '',
        'dimensione_x': row['dimensione_x'] or '',
        'dimensione_y': row['dimensione_y'] or '',
        'spessore': row['spessore'] or ''
    }
    return jsonify({'success': True, 'data': data})


@app.route('/qr/<int:material_id>')
def qr_code(material_id: int):
    """Genera e restituisce l'immagine PNG del solo codice QR per un materiale.

    Questa versione genera esclusivamente il QR code senza includere il codice
    univoco (ID) nell'immagine. L'anteprima sul sito mostrerà solo il QR,
    mentre le funzioni di stampa gestiranno l'aggiunta dell'ID.  In questo
    modo l'utente visualizza un'etichetta pulita e compatta, ma quando
    stampa ottiene un'etichetta completa con il codice univoco integrato.
    """
    qr = qrcode.QRCode(box_size=10, border=2)
    qr.add_data(str(material_id))
    qr_img = qr.make_image(fill_color='black', back_color='white').convert('RGB')
    buf = BytesIO()
    qr_img.save(buf, format='PNG')
    buf.seek(0)
    return send_file(buf, mimetype='image/png')


@app.route('/scan')
def scan():
    """Pagina per la scansione del QR code e la rimozione del materiale."""
    return render_template('scan.html', title='Scansione')


@app.route('/sfrido/<int:slab_id>', methods=['GET', 'POST'])
def sfrido_view(slab_id: int):
    """
    Gestisce la creazione di uno sfrido a partire da un ID di lastra rimossa.

    Questa vista viene invocata quando, durante la scansione, l'utente
    seleziona l'opzione "Aggiungi sfrido" e conferma la volontà di
    registrare uno sfrido per la lastra scansionata.  L'ID deve
    corrispondere a una lastra precedentemente rimossa (non più presente
    nella tabella ``materiali``).  Verrà chiesto all'utente di
    specificare le nuove dimensioni (X/Y) e di selezionare un bancale
    compatibile oppure di indicare l'ubicazione per un nuovo bancale.

    Al termine, la lastra verrà reinserita nel magazzino con lo stesso
    identificativo, marcata come sfrido e assegnata al bancale
    selezionato (o creato).  Verrà inoltre registrato un evento
    ``sfrido`` nello storico.
    """
    # Se l'ID è ancora presente nel magazzino non possiamo creare uno sfrido
    conn = get_db_connection()
    existing = conn.execute("SELECT id FROM materiali WHERE id=?", (slab_id,)).fetchone()
    if existing:
        conn.close()
        flash('La lastra con ID {} è ancora presente in magazzino. Rimuoverla prima di creare uno sfrido.'.format(slab_id), 'danger')
        return redirect(url_for('scan'))
    # Recupera l'ultimo evento noto per questo ID per ottenere le proprietà base
    info_row = conn.execute(
        "SELECT * FROM slab_history WHERE slab_id=? ORDER BY id DESC LIMIT 1",
        (slab_id,)
    ).fetchone()
    if not info_row:
        conn.close()
        flash('Non sono disponibili informazioni per la lastra {}. Impossibile generare uno sfrido.'.format(slab_id), 'danger')
        return redirect(url_for('scan'))
    # Converte in dizionario per uso più agevole
    try:
        info = dict(info_row)
    except Exception:
        info = {k: info_row[k] for k in info_row.keys()}
    # Normalizzazione helper: gestisce None e stringhe vuote
    def _norm(x) -> str:
        return str(x).strip() if x is not None else ''
    # Estrai i campi di base dal log: dimensioni originali non sono usate ma
    # potrebbero essere visualizzate all'utente
    base_materiale = info.get('materiale')
    base_tipo = info.get('tipo')
    base_spessore = info.get('spessore')
    base_fornitore = info.get('fornitore')
    base_produttore = info.get('produttore')
    # Ubicazione originale: verrà registrata come "from_*" nell'evento sfrido
    from_letter = info.get('from_letter')
    from_number = info.get('from_number')
    if request.method == 'POST':
        # Nuove dimensioni della lastra
        dim_x = request.form.get('dimensione_x', '').strip()
        dim_y = request.form.get('dimensione_y', '').strip()
        # Verifica dimensioni
        if not dim_x or not dim_y:
            flash('Specificare le nuove dimensioni X e Y per lo sfrido.', 'danger')
            return redirect(request.url)
        # Selezione bancale esistente o nuovo
        pallet_id_raw = request.form.get('pallet_id', '')
        ubicazione_lettera = request.form.get('ubicazione_lettera', '').strip().upper() if request.form.get('ubicazione_lettera') else ''
        # Ubicazione numero potrebbe essere vuota; gestiamolo con int sicuro
        _tmp_num = request.form.get('ubicazione_numero', '')
        try:
            ubicazione_numero = int(_tmp_num) if _tmp_num else None
        except (TypeError, ValueError):
            ubicazione_numero = None
        # Prepara finali
        parent_id = None
        final_letter = None
        final_number = None
        # Usare transazione per inserimenti/aggiornamenti
        try:
            if pallet_id_raw and pallet_id_raw != 'new':
                # L'utente ha selezionato un bancale esistente.  Verifica che sia valido e compatibile.
                try:
                    sel_id = int(pallet_id_raw)
                except (TypeError, ValueError):
                    flash('Bancale selezionato non valido.', 'danger')
                    return redirect(request.url)
                pal = conn.execute(
                    "SELECT * FROM materiali WHERE id=? AND is_pallet=1",
                    (sel_id,)
                ).fetchone()
                if not pal:
                    flash('Il bancale selezionato non esiste.', 'danger')
                    return redirect(request.url)
                # Confronta la combinazione di attributi
                if not (
                    _norm(pal['materiale']) == _norm(base_materiale)
                    and _norm(pal['tipo']) == _norm(base_tipo)
                    and _norm(pal['spessore']) == _norm(base_spessore)
                    and _norm(pal['fornitore']) == _norm(base_fornitore)
                    and _norm(pal['produttore']) == _norm(base_produttore)
                ):
                    flash('Il bancale selezionato non è compatibile con il materiale di questo sfrido.', 'danger')
                    return redirect(request.url)
                parent_id = sel_id
                final_letter = pal['ubicazione_lettera']
                final_number = pal['ubicazione_numero']
                # Aggiorna la quantità del bancale esistente (aggiunge una lastra)
                try:
                    conn.execute(
                        "UPDATE materiali SET quantita = quantita + 1 WHERE id=?",
                        (parent_id,)
                    )
                except Exception:
                    pass
            else:
                # L'utente ha scelto di creare un nuovo bancale.  Occorre avere ubicazione lettera e numero.
                if not ubicazione_lettera or ubicazione_numero is None:
                    flash('Indicare l\'ubicazione (lettera e numero) per il nuovo bancale.', 'danger')
                    return redirect(request.url)
                # Verifica se nella stessa ubicazione esiste già un bancale compatibile
                pallet_rows = conn.execute(
                    "SELECT id, materiale, tipo, spessore, fornitore, produttore, ubicazione_lettera, ubicazione_numero FROM materiali WHERE is_pallet=1 AND ubicazione_lettera=? AND ubicazione_numero=? ORDER BY id",
                    (ubicazione_lettera, ubicazione_numero)
                ).fetchall()
                found_match = False
                for pr in pallet_rows:
                    if (
                        _norm(pr['materiale']) == _norm(base_materiale)
                        and _norm(pr['tipo']) == _norm(base_tipo)
                        and _norm(pr['spessore']) == _norm(base_spessore)
                        and _norm(pr['fornitore']) == _norm(base_fornitore)
                        and _norm(pr['produttore']) == _norm(base_produttore)
                    ):
                        parent_id = pr['id']
                        final_letter = pr['ubicazione_lettera']
                        final_number = pr['ubicazione_numero']
                        # Aggiorna la quantità del bancale compatibile
                        try:
                            conn.execute(
                                "UPDATE materiali SET quantita = quantita + 1 WHERE id=?",
                                (parent_id,)
                            )
                        except Exception:
                            pass
                        found_match = True
                        break
                if not found_match:
                    # Crea un nuovo bancale con le stesse caratteristiche.  La quantità iniziale è 1 poiché
                    # conterrà subito lo sfrido che stiamo aggiungendo.  Le dimensioni del bancale
                    # vengono impostate uguali a quelle dello sfrido per semplicità.
                    cur = conn.execute(
                        "INSERT INTO materiali (materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 1)",
                        (
                            base_materiale,
                            base_tipo if base_tipo else None,
                            f"{dim_x}x{dim_y}",
                            dim_x,
                            dim_y,
                            base_spessore,
                            1,
                            ubicazione_lettera,
                            ubicazione_numero,
                            base_fornitore,
                            base_produttore,
                            None,
                        )
                    )
                    parent_id = cur.lastrowid
                    final_letter = ubicazione_lettera
                    final_number = ubicazione_numero
            # A questo punto parent_id, final_letter e final_number sono definiti
            # Inserisci la lastra sfrido con l'ID originale.  Nota: dimensioni e spessore vengono sostituiti
            # con i valori forniti dall'utente.  ``is_sfrido`` viene settato a 1 e ``is_pallet`` a 0.
            conn.execute(
                "INSERT INTO materiali (id, materiale, tipo, dimensioni, dimensione_x, dimensione_y, spessore, quantita, ubicazione_lettera, ubicazione_numero, fornitore, produttore, note, parent_id, is_sfrido, is_pallet)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)",
                (
                    slab_id,
                    base_materiale,
                    base_tipo if base_tipo else None,
                    f"{dim_x}x{dim_y}",
                    dim_x,
                    dim_y,
                    base_spessore,
                    1,
                    final_letter,
                    final_number,
                    base_fornitore,
                    base_produttore,
                    None,
                    parent_id,
                )
            )
            # Commit tutte le modifiche finora eseguite (aggiornamento/creazione
            # del bancale e inserimento della nuova lastra sfrido) prima di
            # registrare gli eventi.  In caso contrario, aprire una seconda
            # connessione per il log potrebbe fallire con "database is locked"
            # perché la transazione corrente tiene un lock di scrittura sul DB.
            try:
                conn.commit()
            except Exception:
                # anche se la commit fallisce continuiamo; eventuali eccezioni verranno gestite a valle
                pass
            # Chiudi la connessione principale prima di registrare gli eventi per rilasciare
            # completamente il lock sul database.  Una volta chiusa la connessione, si utilizzerà
            # una nuova connessione all'interno di ``log_slab_events``.
            try:
                conn.close()
            except Exception:
                pass
            # Registra gli eventi nello storico.  Registriamo sia l'aggiunta
            # (evento "aggiunto") sia l'evento specifico "sfrido" per
            # documentare completamente il cambiamento dello stato della lastra.
            try:
                log_slab_events([
                    {
                        'slab_id': slab_id,
                        'event_type': 'aggiunto',
                        'from_letter': None,
                        'from_number': None,
                        'to_letter': final_letter,
                        'to_number': final_number,
                        'dimensione_x': dim_x,
                        'dimensione_y': dim_y,
                        'spessore': base_spessore,
                        'materiale': base_materiale,
                        'tipo': base_tipo,
                        'fornitore': base_fornitore,
                        'produttore': base_produttore,
                        'note': None,
                        'nesting_link': None,
                    },
                    {
                        'slab_id': slab_id,
                        'event_type': 'sfrido',
                        'from_letter': from_letter,
                        'from_number': from_number,
                        'to_letter': final_letter,
                        'to_number': final_number,
                        'dimensione_x': dim_x,
                        'dimensione_y': dim_y,
                        'spessore': base_spessore,
                        'materiale': base_materiale,
                        'tipo': base_tipo,
                        'fornitore': base_fornitore,
                        'produttore': base_produttore,
                        'note': None,
                        'nesting_link': None,
                    }
                ])
            except Exception:
                # Se il logging fallisce non interrompiamo il flusso; l'inserimento dello
                # sfrido rimarrà comunque valido.
                pass
            flash('Sfrido creato con successo!', 'success')
            # Reindirizza alla pagina di dettaglio della nuova lastra sfrido
            return redirect(url_for('dettaglio', material_id=slab_id))
        except Exception as e:
            # In caso di eccezione, chiudi la connessione e segnala l'errore
            try:
                conn.rollback()
            except Exception:
                pass
            conn.close()
            flash('Si è verificato un errore durante la creazione dello sfrido: {}'.format(e), 'danger')
            return redirect(url_for('scan'))
    # Metodo GET: prepara i dati per il form
    # Recupera tutti i bancali esistenti per mostrarli nel menu a tendina.  Ordiniamo per ID per stabilità.
    pallets = conn.execute(
        "SELECT id, ubicazione_lettera, ubicazione_numero, materiale, tipo, spessore, fornitore, produttore FROM materiali WHERE is_pallet=1 ORDER BY id"
    ).fetchall()
    # Determina se esiste un bancale precedentemente utilizzato per questa lastra.  Utilizziamo la
    # combinazione di ubicazione (from_letter/from_number) e caratteristiche del materiale per
    # identificare un bancale ancora presente.  Se trovato verrà suggerito come default nel form.
    previous_pallet_id = None
    previous_pallet_letter = None
    previous_pallet_number = None
    for p in pallets:
        try:
            if (
                p['ubicazione_lettera'] == from_letter
                and p['ubicazione_numero'] == from_number
                and _norm(p['materiale']) == _norm(base_materiale)
                and _norm(p['tipo']) == _norm(base_tipo)
                and _norm(p['spessore']) == _norm(base_spessore)
                and _norm(p['fornitore']) == _norm(base_fornitore)
                and _norm(p['produttore']) == _norm(base_produttore)
            ):
                previous_pallet_id = p['id']
                previous_pallet_letter = p['ubicazione_lettera']
                previous_pallet_number = p['ubicazione_numero']
                break
        except Exception:
            pass
    conn.close()
    # Passa alla template le informazioni della lastra rimossa, l'elenco dei bancali e l'eventuale bancale precedente
    return render_template(
        'sfrido_form.html',
        slab_id=slab_id,
        base_materiale=base_materiale,
        base_tipo=base_tipo,
        base_spessore=base_spessore,
        base_fornitore=base_fornitore,
        base_produttore=base_produttore,
        base_dim_x=info.get('dimensione_x'),
        base_dim_y=info.get('dimensione_y'),
        pallets=pallets,
        previous_pallet_id=previous_pallet_id,
        previous_pallet_letter=previous_pallet_letter,
        previous_pallet_number=previous_pallet_number,
    )


@app.route('/export_csv')
def export_csv():
    """Esporta l'inventario in formato CSV scaricabile."""
    conn = get_db_connection()
    materiali = conn.execute("SELECT * FROM materiali").fetchall()
    conn.close()

    def generate():
        data = BytesIO()
        writer = csv.writer(data)
        # intestazioni
        writer.writerow(['ID', 'Materiale', 'Dimensioni', 'Spessore', 'Quantità', 'Ubicazione', 'Fornitore', 'Produttore', 'Note'])
        yield data.getvalue()
        data.seek(0)
        data.truncate(0)
        for m in materiali:
            writer.writerow([
                m['id'],
                m['materiale'],
                m['dimensioni'],
                m['spessore'],
                m['quantita'],
                f"{m['ubicazione_lettera']}-{m['ubicazione_numero']}",
                m['fornitore'],
                m['produttore'],
                m['note']
            ])
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
    headers = {'Content-Disposition': 'attachment; filename="magazzino.csv"'}
    return Response(generate(), mimetype='text/csv', headers=headers)

# ---------------------------------------------------------------------------
# Gestione riordini

@app.route('/sfridi')
def sfridi():
    """
    Pagina che elenca tutte le lastre marcate come sfrido (scarti).

    Vengono mostrati sia gli sfridi indipendenti che quelli contenuti all'interno dei bancali.
    Ogni riga consente di modificare o prenotare la singola lastra.
    """
    conn = get_db_connection()
    # Imposta row factory per avere risultati come dict
    conn.row_factory = sqlite3.Row
    try:
        # Recupera tutte le lastre marcate come sfrido, ordinandole per ubicazione e caratteristiche
        rows = conn.execute(
            """
            SELECT id, ubicazione_lettera, ubicazione_numero, dimensione_x, dimensione_y,
                   spessore, materiale, tipo, fornitore, produttore, note
            FROM materiali
            WHERE is_sfrido = 1
            ORDER BY ubicazione_lettera, CAST(ubicazione_numero AS INTEGER), materiale, tipo, spessore
            """
        ).fetchall()
    finally:
        conn.close()
    # Recupera gli ID dei materiali prenotati per disabilitare il tasto Prenota sugli sfridi
    try:
        reserved_ids_sfridi = get_reserved_material_ids()
    except Exception:
        reserved_ids_sfridi = set()
    return render_template('sfridi.html', lastre=rows, reserved_ids=list(reserved_ids_sfridi))

@app.route('/riordini')
def riordini():
    """Pagina che elenca i materiali che hanno raggiunto la soglia di riordino.

    Questa vista scorre tutti i materiali radice (bancali o lastre indipendenti) e
    verifica se la loro quantità aggregata (per combinazione materiale/tipo/spessore)
    è inferiore o uguale alla soglia definita per quella terna.  Se almeno
    una lastra figlia soddisfa la soglia, anche il bancale viene segnalato.  I
    risultati sono presentati in una tabella con le principali informazioni.
    """
    # Verifica permessi: solo gli utenti con il tab "riordini" possono accedere.
    allowed = session.get('allowed_tabs', [])
    if 'riordini' not in allowed:
        flash('Accesso non autorizzato ai riordini.', 'danger')
        return redirect(url_for('dashboard'))
    conn = get_db_connection()
    # La tabella ``riordini_bloccati`` non viene più utilizzata per bloccare le combinazioni
    # dopo l'accettazione. I riordini sono ora bloccati solo mentre la combinazione
    # è presente nella tabella ``riordini_accettazione``. Dopo che l'accettazione è
    # stata completata e la combinazione è stata spostata nello storico, la
    # combinazione può ricomparire nei riordini se la giacenza scende di nuovo
    # sotto la soglia.  Pertanto non creiamo né consultiamo più questa tabella.
    # Recupera tutte le righe radice (parent_id IS NULL)
    rows = conn.execute(
        "SELECT * FROM materiali WHERE parent_id IS NULL ORDER BY id"
    ).fetchall()
    # Carica soglie e quantità di riordino da tabella legacy e dalla tabella estesa
    threshold_map: dict[tuple[str, str, str], int] = {}
    reorder_qty_map: dict[tuple[str, str, str], int | None] = {}
    threshold_map_ext: dict[tuple[str, str, str, str, str, str], int] = {}
    reorder_qty_map_ext: dict[tuple[str, str, str, str, str, str], int | None] = {}
    try:
        thresholds = conn.execute(
            f"SELECT materiale, tipo, spessore, threshold, quantita_riordino FROM {RIORDINO_SOGGIE_TABLE}"
        ).fetchall()
        for tr in thresholds:
            key = (tr['materiale'], tr['tipo'] or '', tr['spessore'] or '')
            # threshold legacy
            try:
                threshold_map[key] = int(tr['threshold'])
            except (ValueError, TypeError):
                threshold_map[key] = DEFAULT_REORDER_THRESHOLD
            # quantita di riordino legacy
            try:
                qraw = tr['quantita_riordino']
                qval = int(qraw) if qraw not in (None, '') else None
            except (ValueError, TypeError):
                qval = None
            if qval is not None and qval <= 0:
                qval = None
            reorder_qty_map[key] = qval
        # Carica valori dalla tabella estesa se esiste
        try:
            ext_rows = conn.execute(
                "SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy, COALESCE(produttore,'') AS prod, "
                "threshold, quantita_riordino FROM riordino_soglie_ext"
            ).fetchall()
            for er in ext_rows:
                k = (
                    er['materiale'],
                    er['tipo'] or '',
                    er['spessore'] or '',
                    (er['dx'] or '').strip(),
                    (er['dy'] or '').strip(),
                    (er['prod'] or '').strip(),
                )
                try:
                    threshold_map_ext[k] = int(er['threshold'])
                except (ValueError, TypeError):
                    threshold_map_ext[k] = DEFAULT_REORDER_THRESHOLD
                try:
                    qraw2 = er['quantita_riordino']
                    qv2 = int(qraw2) if qraw2 not in (None, '') else None
                except (ValueError, TypeError):
                    qv2 = None
                if qv2 is not None and qv2 <= 0:
                    qv2 = None
                reorder_qty_map_ext[k] = qv2
        except sqlite3.Error:
            threshold_map_ext = {}
            reorder_qty_map_ext = {}
    except sqlite3.Error:
        threshold_map = {}
        reorder_qty_map = {}
        threshold_map_ext = {}
        reorder_qty_map_ext = {}
    # Costruisci l'elenco delle combinazioni da riordinare partendo solo dalle anagrafiche articoli.
    # Recupera l'insieme delle combinazioni attualmente in attesa di accettazione.
    # Solo le combinazioni in questa tabella vengono escluse dalla lista dei riordini,
    # così che dopo l'accettazione la combinazione possa riapparire se scende di nuovo sotto soglia.
    # L'insieme delle combinazioni attive tiene conto anche del produttore.
    # Ogni tupla è composta da (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore).
    active_keys: set[tuple[str, str, str, str, str, str]] = set()
    try:
        # Recupera combinazioni attualmente in fase di accettazione includendo il produttore.
        act_rows = conn.execute(
            "SELECT COALESCE(materiale,'') AS materiale, "
            "COALESCE(tipo,'') AS tipo, "
            "COALESCE(spessore,'') AS spessore, "
            "COALESCE(dimensione_x,'') AS dx, "
            "COALESCE(dimensione_y,'') AS dy, "
            "COALESCE(produttore,'') AS prod "
            "FROM riordini_accettazione"
        ).fetchall()
        for a in act_rows:
            # Ogni combinazione può essere associata a più produttori? Nel contesto di accettazione
            # la colonna "produttore" rappresenta un singolo valore, quindi aggiungiamo direttamente.
            active_keys.add((a['materiale'] or '', a['tipo'] or '', a['spessore'] or '', (a['dx'] or ''), (a['dy'] or ''), (a['prod'] or '')))
        # Recupera combinazioni che sono già state preparate in RDO ma non ancora confermate.
        # La tabella riordini_rdo conserva l'elenco dei produttori in una singola stringa (separati da virgola)
        # e il produttore scelto se bloccato.  Consideriamo tutte le combinazioni per ciascun produttore
        # presente nella lista o il produttore scelto.
        rdo_rows_excl = conn.execute(
            "SELECT COALESCE(materiale,'') AS materiale, "
            "COALESCE(tipo,'') AS tipo, "
            "COALESCE(spessore,'') AS spessore, "
            "COALESCE(dimensione_x,'') AS dx, "
            "COALESCE(dimensione_y,'') AS dy, "
            "COALESCE(produttori,'') AS prods, "
            "COALESCE(produttore_scelto,'') AS prod_sel "
            "FROM riordini_rdo"
        ).fetchall()
        for r in rdo_rows_excl:
            mat = (r['materiale'] or '')
            tpv = (r['tipo'] or '')
            spv = (r['spessore'] or '')
            dxv = (r['dx'] or '')
            dyv = (r['dy'] or '')
            prod_sel = (r['prod_sel'] or '').strip()
            # Determina l'elenco dei produttori: se è stato selezionato un singolo produttore, usa quello;
            # altrimenti considera tutti i produttori nella stringa separata da virgola;
            # se non ci sono produttori, utilizza una stringa vuota per rappresentare "senza produttore".
            if prod_sel:
                prod_list = [prod_sel]
            else:
                prods_field = (r['prods'] or '')
                if prods_field:
                    prod_list = [p.strip() for p in prods_field.split(',') if p and p.strip()]
                    if not prod_list:
                        prod_list = ['']
                else:
                    prod_list = ['']
            for p in prod_list:
                active_keys.add((mat, tpv, spv, dxv, dyv, p or ''))
    except sqlite3.Error:
        # In caso di errore su una delle query, azzera l'insieme per evitare blocchi
        active_keys = set()

    reorder_rows: list[dict] = []
    # Itera tutte le combinazioni manualmente definite nel catalogo articoli
    try:
        catalog_rows = conn.execute(
            "SELECT materiale, "
            "COALESCE(tipo,'') AS tipo, "
            "COALESCE(spessore,'') AS spessore, "
            "COALESCE(dimensione_x,'') AS dx, "
            "COALESCE(dimensione_y,'') AS dy, "
            "COALESCE(produttore,'') AS prod "
            "FROM articoli_catalogo"
        ).fetchall()
    except sqlite3.Error:
        catalog_rows = []
    # Utilizza un set per evitare di elaborare più volte la stessa combinazione dal catalogo
    seen_combos: set[tuple[str, str, str, str, str, str]] = set()
    for row in catalog_rows:
        mat = row['materiale']
        tp = (row['tipo'] or '')
        sp = (row['spessore'] or '')
        dx = (row['dx'] or '').strip()
        dy = (row['dy'] or '').strip()
        prod = (row['prod'] or '').strip()
        combo_key = (mat, tp, sp, dx, dy, prod)
        # Evita duplicati: se la combinazione è già stata processata, salta
        if combo_key in seen_combos:
            continue
        seen_combos.add(combo_key)
        # Se la combinazione è attualmente in accettazione o in RDO, salta (verrà mostrata nella sezione dedicata)
        if combo_key in active_keys:
            continue
        # Calcola la quantità totale per questa combinazione esatta dal magazzino.
        # Sommiamo la quantità di tutte le righe (pallet radice o lastre figlie) con dimensioni
        # esattamente uguali.  Non filtriamo per parent_id in modo da replicare la
        # logica dell'anagrafica articoli, dove ogni combinazione (dimensione X/Y) è
        # trattata separatamente e non vi è sovrapposizione tra pallet radice e lastre figlie.
        try:
            # Calcola la quantità totale della combinazione includendo il produttore.
            qty_res = conn.execute(
                "SELECT SUM(quantita) AS tot FROM materiali WHERE materiale=? "
                "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                "AND TRIM(COALESCE(produttore,'')) = ? "
                "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                (
                    mat,
                    tp or None, tp,
                    sp or None, sp,
                    dx or None, dx,
                    dy or None, dy,
                    prod or '',
                )
            ).fetchone()
            total_qty = int(qty_res['tot'] or 0) if qty_res and qty_res['tot'] is not None else 0
        except sqlite3.Error:
            total_qty = 0
        # Recupera la soglia per la combinazione.  Prima prova la mappa estesa,
        # altrimenti usa la mappa legacy.
        th_val = threshold_map_ext.get((mat, tp, sp, dx, dy, prod), None)
        if th_val is None:
            th_val = threshold_map.get((mat, tp, sp), DEFAULT_REORDER_THRESHOLD)
        # SOGGLIA=0 => la combinazione NON va calcolata nei riordini
        try:
            if int(th_val) == 0:
                continue
        except Exception:
            pass
        # Quantità di riordino impostata manualmente (può essere None o <=0)
        rq_manual_raw = None
        # Prova la mappa estesa
        if (mat, tp, sp, dx, dy, prod) in reorder_qty_map_ext:
            rq_manual_raw = reorder_qty_map_ext.get((mat, tp, sp, dx, dy, prod))
        else:
            rq_manual_raw = reorder_qty_map.get((mat, tp, sp))
        try:
            rq_manual = int(rq_manual_raw) if rq_manual_raw not in (None, '') else None
            if rq_manual is not None and rq_manual <= 0:
                rq_manual = None
        except Exception:
            rq_manual = None
        # NUOVA LOGICA "Q.tà da ordinare":
        # se (Q + R) <= (S + 1) => ordina (S + 1) - Q; altrimenti ordina R
        # dove Q=quantità totale, R=quantità di riordino manuale, S=soglia
        rq = None
        if int(total_qty) <= int(th_val):
            if rq_manual is None:
                # Se non c'è R, usa direttamente (S + 1) - Q (>=1 perché Q <= S)
                rq = (int(th_val) + 1) - int(total_qty)
            else:
                if (int(total_qty) + rq_manual) <= (int(th_val) + 1):
                    rq = (int(th_val) + 1) - int(total_qty)
                else:
                    rq = rq_manual
        else:
            # Sopra soglia: usa eventuale quantità manuale
            if rq_manual is not None:
                rq = rq_manual
            else:
                rq = None
        # Mostra la combinazione solo se la quantità totale è inferiore o uguale alla soglia
        if total_qty <= th_val:
            # Raccogli elenco dei bancali radice interessati per questa combinazione
            bancali_list: list[dict] = []
            try:
                pallet_rows = conn.execute(
                    "SELECT id, COALESCE(ubicazione_lettera,'') AS lettera, COALESCE(ubicazione_numero,0) AS numero, COALESCE(quantita,0) AS quantita "
                    "FROM materiali WHERE parent_id IS NULL AND materiale=? "
                    "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                    "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                    "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                    "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                    "AND (is_sfrido IS NULL OR is_sfrido != 1) "
                    "ORDER BY lettera, numero",
                    (mat, tp or None, tp, sp or None, sp, dx or None, dx, dy or None, dy)
                ).fetchall()
                for pr in pallet_rows:
                    num = '' if pr['numero'] is None else int(pr['numero'])
                    bancali_list.append({
                        'id': pr['id'],
                        'ubicazione': f"{pr['lettera']}{num}",
                        'quantita': int(pr['quantita'] or 0)
                    })
            except sqlite3.Error:
                bancali_list = []
            reorder_rows.append({
                'materiale': mat,
                'tipo': tp,
                'spessore': sp,
                'dimensione_x': dx,
                'dimensione_y': dy,
                'produttore': prod,
                'quantita_totale': total_qty,
                'riordino_qty': rq,
                # Lista di bancali interessati è conservata per eventuali estensioni future.
                'bancali_interessati': bancali_list,
                'key_id': abs(hash((mat, tp, sp, dx, dy, prod))) % 100000000
            })

    # Costruisci lo storico dei riordini effettuati per visualizzarlo sotto la tabella.
    # Verranno applicati filtri e paginazione in base ai parametri di query.
    history: list[dict] = []
    # In parallelo, raccogli i valori distinti per i filtri a tendina (materiale, tipo, spessore, dimensioni X/Y, evento).
    distinct_materiali: set[str] = set()
    distinct_tipi: set[str] = set()
    distinct_spessori: set[str] = set()
    distinct_dxs: set[str] = set()
    distinct_dys: set[str] = set()
    distinct_eventi: set[str] = set()
    # Raccogli anche i produttori distinti per filtrare lo storico ordini
    distinct_produttori: set[str] = set()
    try:
        hist_rows = conn.execute("SELECT * FROM riordini_effettuati ORDER BY datetime(data) DESC").fetchall()
        for hr in hist_rows:
            row_dict = dict(hr)
            # Calcola flag confermato/accettato in base al tipo_evento
            tipo_evento_val = (row_dict.get('tipo_evento') or '')
            row_dict['confermato'] = True if tipo_evento_val == 'ordine' else False
            row_dict['accettato'] = True if tipo_evento_val == 'accettazione' else False
            # Popola set distinti
            mval = (row_dict.get('materiale') or '').strip()
            if mval:
                distinct_materiali.add(mval)
            tval = (row_dict.get('tipo') or '').strip()
            if tval:
                distinct_tipi.add(tval)
            sval = (row_dict.get('spessore') or '').strip()
            if sval:
                distinct_spessori.add(sval)
            dxv = (row_dict.get('dimensione_x') or '').strip()
            if dxv:
                distinct_dxs.add(dxv)
            dyv = (row_dict.get('dimensione_y') or '').strip()
            if dyv:
                distinct_dys.add(dyv)
            evv = tipo_evento_val.strip()
            if evv:
                distinct_eventi.add(evv)
            # Raccogli valori distinti di produttore
            pval = (row_dict.get('produttore') or '').strip()
            if pval:
                distinct_produttori.add(pval)
            history.append(row_dict)
    except sqlite3.Error:
        history = []

    # Raccogli le righe ancora in accettazione per mostrarle prima dello storico.
    # Se mancano dimensioni X o Y, tenta di recuperarle dall'anagrafica articoli.
    accettazioni: list[dict] = []
    try:
        acc_rows = conn.execute("SELECT * FROM riordini_accettazione ORDER BY datetime(data) DESC").fetchall()
        # Precarica una mappa dell'anagrafica articoli per ricostruire dimensioni
        catalog_map: dict[tuple[str, str, str], tuple[str, str]] = {}
        try:
            cat_rows = conn.execute(
                "SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy FROM articoli_catalogo"
            ).fetchall()
            for cr in cat_rows:
                key = (cr['materiale'], cr['tipo'] or '', cr['spessore'] or '')
                # solo imposta se non già presente (prendi la prima combinazione disponibile)
                if key not in catalog_map:
                    catalog_map[key] = ((cr['dx'] or '').strip(), (cr['dy'] or '').strip())
        except sqlite3.Error:
            catalog_map = {}
        # Precarica mappa fornitori per ciascun ordine in accettazione (tabella ordine_fornitori)
        forn_map: dict[str, dict] = {}
        try:
            forrows = conn.execute(
                "SELECT numero_ordine, fornitori, fornitore_scelto, locked FROM ordine_fornitori"
            ).fetchall()
            for fr in forrows:
                num = fr['numero_ordine']
                if num:
                    forn_map[str(num)] = dict(fr)
        except sqlite3.Error:
            # Tabella ordine_fornitori potrebbe non esistere
            forn_map = {}
        # Precarica mappa produttori per ciascun ordine (tabella ordine_produttori)
        prod_map: dict[str, dict] = {}
        try:
            prodrows = conn.execute(
                "SELECT numero_ordine, produttori, produttore_scelto, locked FROM ordine_produttori"
            ).fetchall()
            for pr in prodrows:
                nump = pr['numero_ordine']
                if nump:
                    prod_map[str(nump)] = dict(pr)
        except sqlite3.Error:
            # Tabella ordine_produttori potrebbe non esistere
            prod_map = {}
        for ar in acc_rows:
            row_dict = dict(ar)
            # Se dimensioni mancanti o vuote, prova a recuperarle dal catalogo per la terna
            dx = (row_dict.get('dimensione_x') or '').strip()
            dy = (row_dict.get('dimensione_y') or '').strip()
            if (not dx or not dy) and row_dict.get('materiale'):
                key = (
                    row_dict.get('materiale'),
                    (row_dict.get('tipo') or ''),
                    (row_dict.get('spessore') or '')
                )
                if key in catalog_map:
                    cdx, cdy = catalog_map[key]
                    # mantieni eventuali valori già presenti
                    if not dx:
                        dx = cdx
                    if not dy:
                        dy = cdy
                    row_dict['dimensione_x'] = dx
                    row_dict['dimensione_y'] = dy
            # Calcola il residuo (quantità ancora da ricevere) e lo stato di avanzamento
            try:
                qt = int(row_dict.get('quantita_totale') or 0)
            except (ValueError, TypeError):
                qt = 0
            try:
                qr = int(row_dict.get('quantita_ricevuta') or 0)
            except (ValueError, TypeError):
                qr = 0
            residuo = qt - qr
            if residuo < 0:
                residuo = 0
            # Stato: Parziale se non tutto ricevuto, Completo se quantità ricevuta >= totale
            stato = 'Completo' if residuo == 0 else 'Parziale'
            row_dict['residuo'] = residuo
            row_dict['stato'] = stato
            # Calcola progress_pct (0-100) per la barra di avanzamento
            progress_pct = 0
            try:
                if qt and qt > 0:
                    progress_pct = int((qr * 100) / qt)
            except Exception:
                progress_pct = 0
            # clamp 0-100
            if progress_pct < 0:
                progress_pct = 0
            if progress_pct > 100:
                progress_pct = 100
            row_dict['progress_pct'] = progress_pct
            # Associa fornitori e stato di scelta del fornitore a questo ordine (se presenti)
            numero_ordine_val = str(row_dict.get('numero_ordine') or '')
            if numero_ordine_val and numero_ordine_val in forn_map:
                forn_entry = forn_map[numero_ordine_val]
                # Lista di fornitori (può essere stringa separata da virgole)
                fornitori_str = (forn_entry.get('fornitori') or '')
                if fornitori_str:
                    forn_list = [fn.strip() for fn in fornitori_str.split(',') if fn.strip()]
                else:
                    forn_list = []
                row_dict['fornitori'] = forn_list
                row_dict['fornitore_scelto'] = forn_entry.get('fornitore_scelto')
                try:
                    row_dict['forn_locked'] = bool(int(forn_entry.get('locked', 0)))
                except Exception:
                    row_dict['forn_locked'] = False
            else:
                row_dict['fornitori'] = []
                row_dict['fornitore_scelto'] = None
                row_dict['forn_locked'] = False
            # Associa produttori e stato di scelta del produttore a questo ordine (se presenti)
            if numero_ordine_val and numero_ordine_val in prod_map:
                prod_entry = prod_map[numero_ordine_val]
                prod_str = (prod_entry.get('produttori') or '')
                if prod_str:
                    prod_list = [pd.strip() for pd in prod_str.split(',') if pd.strip()]
                else:
                    prod_list = []
                row_dict['produttori'] = prod_list
                row_dict['produttore_scelto'] = prod_entry.get('produttore_scelto')
                try:
                    row_dict['prod_locked'] = bool(int(prod_entry.get('locked', 0)))
                except Exception:
                    row_dict['prod_locked'] = False
            else:
                row_dict['produttori'] = []
                row_dict['produttore_scelto'] = None
                row_dict['prod_locked'] = False
            accettazioni.append(row_dict)
    except sqlite3.Error:
        accettazioni = []

    # Filtri per lo storico: date e attributi.
    # Parametri di query: page, start_date, end_date, materiale_filter, tipo_filter, spessore_filter, dx_filter, dy_filter.
    page_param = request.args.get('page', default='1')
    try:
        current_page = int(page_param)
    except (TypeError, ValueError):
        current_page = 1
    # Limite per pagina
    per_page = 15
    # Ricava filtri dalle query string
    start_date_str = request.args.get('start_date', '').strip()
    end_date_str = request.args.get('end_date', '').strip()
    materiale_filter = request.args.get('materiale_filter', '').strip()
    tipo_filter = request.args.get('tipo_filter', '').strip()
    spessore_filter = request.args.get('spessore_filter', '').strip()
    dx_filter = request.args.get('dx_filter', '').strip()
    dy_filter = request.args.get('dy_filter', '').strip()
    # Filtro per il tipo di evento nello storico ('ordine' o 'accettazione')
    evento_filter = request.args.get('evento_filter', '').strip()
    # Filtro per produttore nello storico
    produttore_filter = request.args.get('produttore_filter', '').strip()
    # Filtra lo storico in base ai parametri
    filtered_history: list[dict] = []
    # Funzione ausiliaria per confronto case-insensitive contenuto
    def matches(value: str | None, pattern: str) -> bool:
        if not pattern:
            return True
        if value is None:
            return False
        return pattern.lower() in str(value).lower()
    # Converti date di filtro (YYYY-MM-DD) in oggetti date
    start_date_obj = None
    end_date_obj = None
    try:
        if start_date_str:
            start_date_obj = datetime.fromisoformat(start_date_str)
    except Exception:
        start_date_obj = None
    try:
        if end_date_str:
            # se l'utente fornisce solo data, consideriamo la fine della giornata
            end_date_obj = datetime.fromisoformat(end_date_str) + timedelta(days=1)
    except Exception:
        end_date_obj = None
    for row in history:
        # Filtra per date
        # La colonna data è stringa ISO (YYYY-MM-DD HH:MM:SS), converti in datetime
        include = True
        if start_date_obj or end_date_obj:
            try:
                row_dt = datetime.fromisoformat(str(row.get('data')))
            except Exception:
                row_dt = None
            if row_dt:
                if start_date_obj and row_dt < start_date_obj:
                    include = False
                if end_date_obj and row_dt >= end_date_obj:
                    include = False
        # Filtra per attributi testo
        if include and not matches(row.get('materiale'), materiale_filter):
            include = False
        if include and not matches(row.get('tipo'), tipo_filter):
            include = False
        if include and not matches(row.get('spessore'), spessore_filter):
            include = False
        # Filtra per dimensioni esatte (usa contains per semplicità)
        if include and not matches(row.get('dimensione_x'), dx_filter):
            include = False
        if include and not matches(row.get('dimensione_y'), dy_filter):
            include = False
        # Filtra per produttore se specificato
        if include and not matches(row.get('produttore'), produttore_filter):
            include = False
        # Filtra per tipo_evento se specificato
        if include and evento_filter:
            tev = (row.get('tipo_evento') or '')
            if tev != evento_filter:
                include = False
        if include:
            filtered_history.append(row)
    # Calcola pagine
    total_results = len(filtered_history)
    total_pages = (total_results + per_page - 1) // per_page if total_results > 0 else 1
    # Normalizza pagina richiesta
    if current_page < 1:
        current_page = 1
    if current_page > total_pages:
        current_page = total_pages
    # Determina range di elementi da mostrare
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    history_paginated = filtered_history[start_idx:end_idx]

    # Costruisci una struttura gerarchica padre-figlio per lo storico.
    # Ogni elemento dell'elenco risultante è un dizionario con le chiavi:
    #  - 'parent': la riga dell'ordine confermato (tipo_evento="ordine") o None.
    #  - 'children': un elenco di righe di accettazione (tipo_evento="accettazione")
    #    che appartengono allo stesso ordine. La chiave di raggruppamento è la
    #    combinazione (materiale, tipo, spessore, dimensione_x, dimensione_y).
    # L'elenco è costruito invertendo l'ordine cronologico (dato che
    # ``history_paginated`` è già ordinato in ordine decrescente).  In
    # questo modo le accettazioni più recenti appariranno subito sotto
    # l'ordine più recente.
    history_tree: list[dict] = []
    try:
        # Invertiamo l'elenco per processare dalla più vecchia alla più recente
        history_asc = list(reversed(history_paginated))
        # Raggruppa gli eventi per ordine e combinazione, ignorando il produttore.
        # La chiave include numero_ordine, materiale, tipo, spessore e dimensioni, così che
        # tutte le accettazioni con lo stesso ordine vengano associate al relativo "ordine" padre
        # anche se hanno produttori differenti.  Questo consente di etichettare il produttore
        # della riga padre come "Misto" quando necessario.
        last_order_for_combo: dict[tuple[str, str, str, str, str, str], dict] = {}
        nodes_temp: list[dict] = []
        for row in history_asc:
            tev = (row.get('tipo_evento') or '').strip()
            # Costruiamo la chiave di raggruppamento senza includere il produttore
            combo = (
                (row.get('numero_ordine') or ''),
                (row.get('materiale') or ''),
                (row.get('tipo') or ''),
                (row.get('spessore') or ''),
                (row.get('dimensione_x') or ''),
                (row.get('dimensione_y') or '')
            )
            if tev == 'ordine':
                # Nuova riga ordine: crea nodo con lista di figli vuota e registra come ultimo ordine per la combinazione
                node = {'parent': row, 'children': []}
                nodes_temp.append(node)
                last_order_for_combo[combo] = node
            elif tev == 'accettazione':
                # Riga di accettazione: prova ad associarla all'ultimo ordine per la stessa combinazione
                parent_node = last_order_for_combo.get(combo)
                if parent_node:
                    parent_node['children'].append(row)
                else:
                    # Nessun ordine associato (ad esempio per pagine successive): trattala come nodo autonomo
                    nodes_temp.append({'parent': None, 'children': [row]})
            else:
                # Eventi non categorizzati (fallback): visualizza come riga autonoma
                nodes_temp.append({'parent': row, 'children': []})
        # Dopo aver costruito l'elenco temporaneo, determina il produttore da mostrare per ciascun nodo ordine.
        for node in nodes_temp:
            parent = node.get('parent')
            children = node.get('children') or []
            if parent and children:
                # Raccogli i produttori distinti dalle righe figlie, ignorando valori vuoti
                prod_set = set()
                for ch in children:
                    pval = (ch.get('produttore') or '').strip()
                    if pval:
                        prod_set.add(pval)
                # Se più di un produttore è stato utilizzato, etichetta il padre come "Misto";
                # altrimenti, se è presente un solo produttore, usa quello anche per il padre.
                if len(prod_set) > 1:
                    parent['produttore'] = 'Misto'
                elif len(prod_set) == 1:
                    unique_prod = next(iter(prod_set))
                    parent['produttore'] = unique_prod
        # Riordina per avere il più recente all'inizio
        history_tree = list(reversed(nodes_temp))
    except Exception:
        # In caso di errore imprevisto utilizza una rappresentazione piatta
        history_tree = [{'parent': row, 'children': []} for row in history_paginated]

    # Recupera la lista dei fornitori con l'email (se presente) per la selezione nell'ordine
    try:
        suppliers = conn.execute(
            f"SELECT id, nome, email FROM {SUPPLIER_TABLE} ORDER BY nome"
        ).fetchall()
    except sqlite3.Error:
        suppliers = []
    # Recupera la lista dei produttori dal vocabolario
    try:
        produttori = conn.execute(
            f"SELECT id, nome FROM {PRODUTTORE_TABLE} ORDER BY nome"
        ).fetchall()
    except sqlite3.Error:
        produttori = []
    # Carica le righe RDO (richieste d'ordine) in sospeso
    rdo_rows: list[dict] = []
    try:
        rdo_cur = conn.execute("SELECT * FROM riordini_rdo ORDER BY datetime(data) DESC")
        for rr in rdo_cur.fetchall():
            rdo = dict(rr)
            # Genera liste di fornitori e produttori dalle stringhe CSV
            forn_raw = rdo.get('fornitori') or ''
            prod_raw = rdo.get('produttori') or ''
            forn_list = [f.strip() for f in str(forn_raw).split(',') if f.strip()]
            prod_list = [p.strip() for p in str(prod_raw).split(',') if p.strip()]
            rdo['fornitori_list'] = forn_list
            # Per la lista di produttori consenti la selezione di qualsiasi produttore
            # presente nel dizionario dei produttori.  La variabile ``produttori``
            # contiene un elenco di record con ``id`` e ``nome``; estraiamo
            # i nomi e li combiniamo con l'eventuale lista proveniente dalla
            # riga RDO.  Utilizziamo ``set`` per evitare duplicati e ordiniamo
            # alfabeticamente per un rendering coerente nel template.
            global_prod_names = [pr['nome'] for pr in produttori]
            combined_prods = sorted(set(prod_list + global_prod_names))
            rdo['produttori_list'] = combined_prods
            # Preleva eventuali date multiple e relative quantità per questo RDO.
            # Se presenti, verranno utilizzate per pre‑popolare i campi di data/quantità nel template.
            try:
                multi_dates_rows = conn.execute(
                    "SELECT data_prevista, quantita, produttore FROM rdo_dates WHERE rdo_id=?",
                    (rr['id'],)
                ).fetchall()
                rdo['multi_dates'] = [
                    {
                        'data_prevista': md['data_prevista'],
                        'quantita': md['quantita'],
                        'produttore': (md['produttore'] or '').strip() if md['produttore'] is not None else ''
                    }
                    for md in multi_dates_rows
                ]
            except sqlite3.Error:
                rdo['multi_dates'] = []
            rdo_rows.append(rdo)
    except sqlite3.Error:
        rdo_rows = []
    # Recupera il template ordine dell'utente corrente
    ordine_template = ''
    user_id = session.get('user_id')
    if user_id:
        try:
            row = conn.execute(
                f"SELECT ordine_template FROM {USERS_TABLE} WHERE id=?", (user_id,)
            ).fetchone()
            if row and row['ordine_template']:
                ordine_template = row['ordine_template']
        except sqlite3.Error:
            ordine_template = ''
    conn.close()
    return render_template(
        'riordini.html',
        title='Da gestire',
        materiali=reorder_rows,
        rdo_rows=rdo_rows,
        accettazioni=accettazioni,
        history=history_paginated,
        history_tree=history_tree,
        current_page=current_page,
        total_pages=total_pages,
        start_date=start_date_str,
        end_date=end_date_str,
        materiale_filter=materiale_filter,
        tipo_filter=tipo_filter,
        spessore_filter=spessore_filter,
        dx_filter=dx_filter,
        dy_filter=dy_filter,
        evento_filter=evento_filter,
        distinct_materiali=sorted(distinct_materiali),
        distinct_tipi=sorted(distinct_tipi),
        distinct_spessori=sorted(distinct_spessori),
        distinct_dxs=sorted(distinct_dxs),
        distinct_dys=sorted(distinct_dys),
        distinct_eventi=sorted(distinct_eventi),
        distinct_produttori=sorted(distinct_produttori),
        produttore_filter=produttore_filter,
        fornitori=suppliers,
        produttori=produttori,
        ordine_template=ordine_template,
        last_added_combo=session.get('last_added_combo')
    )

# ---------------------------------------------------------------------------
# Conferma riordino

@app.route('/conferma_riordino/<int:material_id>', methods=['POST'])
def conferma_riordino(material_id: int):
    """Conferma un riordino per il materiale indicato.

    Quando l'utente preme il pulsante "Conferma riordino" nella pagina
    dei riordini, viene inviata una richiesta POST a questa route con
    l'identificatore del materiale e la quantità desiderata.  Vengono
    registrati i dettagli del materiale e la quantità richiesta nella
    tabella ``riordini_effettuati`` insieme alla data corrente.  Dopo
    l'operazione viene reindirizzato l'utente alla pagina dei riordini.
    """
    try:
        qty_raw = request.form.get('quantita', '').strip()
        quantita = int(qty_raw) if qty_raw else DEFAULT_REORDER_THRESHOLD
    except (ValueError, TypeError):
        quantita = DEFAULT_REORDER_THRESHOLD
    # Recupera i dettagli del materiale
    with get_db_connection() as conn:
        mat = conn.execute("SELECT * FROM materiali WHERE id=?", (material_id,)).fetchone()
        if mat:
            # Calcola la data corrente ISO
            now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
            materiale = mat['materiale']
            tipo = mat['tipo'] or ''
            spessore = mat['spessore'] or ''
            fornitore = mat['fornitore'] or ''
            # Genera un codice ordine univoco per questo riordino
            order_code = generate_order_code(conn)
            # Recupera dimensioni e produttore dal materiale (se presenti) e inserisci nello storico con tutte le informazioni
            dx_val = mat['dimensione_x'] if 'dimensione_x' in mat.keys() else None
            dy_val = mat['dimensione_y'] if 'dimensione_y' in mat.keys() else None
            prod_val = mat['produttore'] if 'produttore' in mat.keys() else None
            # Inserisci nello storico includendo dimensioni, produttore e tipo_evento='ordine'
            conn.execute(
                "INSERT INTO riordini_effettuati (material_id, data, quantita, materiale, tipo, spessore, fornitore, produttore, dimensione_x, dimensione_y, tipo_evento, numero_ordine) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    material_id,
                    now_str,
                    quantita,
                    materiale,
                    tipo,
                    spessore,
                    fornitore,
                    prod_val,
                    dx_val,
                    dy_val,
                    'ordine',
                    order_code
                )
            )
            conn.commit()
            flash('Riordino registrato nello storico!', 'success')
        else:
            flash('Materiale non trovato per il riordino.', 'danger')
    return redirect(url_for('riordini'))

# ---------------------------------------------------------------------------
# Conferma ordine per combinazione (materiale/tipo/spessore/dimensioni)

@app.route('/conferma_ordine_combo', methods=['POST'])
def conferma_ordine_combo():
    """Conferma un ordine per una specifica combinazione di articolo.

    Registra nello storico con i campi materiale, tipo, spessore, dimensione_x, dimensione_y e la quantità confermata.
    Dopo il salvataggio reindirizza l'utente alla pagina dei riordini.
    """
    # Quantità confermata: deve essere un intero positivo
    try:
        quantita = int(request.form.get('quantita') or '0')
    except (TypeError, ValueError):
        quantita = 0
    materiale = (request.form.get('materiale') or '').strip()
    tipo = (request.form.get('tipo') or '').strip()
    spessore = (request.form.get('spessore') or '').strip()
    dimensione_x = (request.form.get('dimensione_x') or '').strip()
    dimensione_y = (request.form.get('dimensione_y') or '').strip()
    # Fallback fornitore: opzionale, potrebbe essere inviato dal form
    fornitore = (request.form.get('fornitore') or '').strip() or None
    if quantita <= 0:
        flash('Quantità non valida per confermare l\'ordine.', 'danger')
        return redirect(url_for('riordini'))
    data_now = datetime.now().isoformat(sep=' ', timespec='seconds')
    try:
        with get_db_connection() as conn:
            # Genera un codice ordine progressivo
            order_code = generate_order_code(conn)
            # Inserisci nella tabella di accettazione. All'inizio nessuna quantità è stata ricevuta.
            conn.execute(
                "INSERT INTO riordini_accettazione (data, materiale, tipo, spessore, dimensione_x, dimensione_y, quantita_totale, quantita_ricevuta, numero_ordine) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
                (
                    data_now,
                    materiale or None,
                    tipo or None,
                    spessore or None,
                    dimensione_x or None,
                    dimensione_y or None,
                    quantita,
                    order_code
                )
            )
            # Registra nello storico la conferma dell'ordine con tipo_evento='ordine', incluso il numero d'ordine e il produttore.
            conn.execute(
                "INSERT INTO riordini_effettuati (material_id, data, quantita, materiale, tipo, spessore, fornitore, produttore, dimensione_x, dimensione_y, tipo_evento, numero_ordine) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    None,
                    data_now,
                    quantita,
                    materiale if materiale else None,
                    tipo if tipo else None,
                    spessore if spessore else None,
                    fornitore,
                    None,  # produttore non specificato in conferma singola
                    dimensione_x if dimensione_x else None,
                    dimensione_y if dimensione_y else None,
                    'ordine',
                    order_code
                )
            )
            conn.commit()
        flash('Ordine confermato: sarà mostrato nella sezione di accettazione.', 'success')
    except sqlite3.Error:
        flash('Errore nel salvataggio dell\'ordine.', 'danger')
    return redirect(url_for('riordini'))

# ---------------------------------------------------------------------------
# Conferma e invia più ordini via email

@app.route('/conferma_e_invia', methods=['POST'])
def conferma_e_invia():
    """Conferma uno o più ordini e invia una email ai fornitori.

    Questa route riceve una lista di combinazioni di articoli (materiale, tipo, spessore,
    dimensioni e quantità) insieme a un elenco opzionale di fornitori selezionati. Per
    ogni combinazione registra l'ordine nella tabella ``riordini_effettuati`` e
    nella tabella ``riordini_accettazione`` con lo stesso numero d'ordine.  Viene
    generato un unico codice ordine progressivo per l'intero ordine.  Dopo la
    registrazione l'ordine viene inviato via email se la configurazione SMTP è
    disponibile; in caso contrario viene visualizzato un avviso.
    """
    # Verifica permessi
    allowed = session.get('allowed_tabs', [])
    if 'riordini' not in allowed:
        flash('Accesso non autorizzato.', 'danger')
        return redirect(url_for('riordini'))

    # Aggiorna eventuali email mancanti dei fornitori.
    # Se nel form sono presenti campi con nome "supplier_email_<id>" (dove <id> è l'ID del fornitore
    # oppure un indice generico), aggiorna la tabella fornitori con l'indirizzo email indicato.
    # Questo consente all'utente di specificare l'email direttamente nella pagina di preparazione ordine
    # quando il fornitore non ha ancora un contatto registrato.
    try:
        with get_db_connection() as email_conn:
            for key, value in request.form.items():
                if not key.startswith('supplier_email_'):
                    continue
                email_val = (value or '').strip()
                if not email_val:
                    continue
                # L'ID del fornitore è la parte dopo l'underscore; potrebbe non essere numerica
                try:
                    suffix = key.split('_', 2)[-1]
                except Exception:
                    suffix = ''
                # Prova a convertire l'ID in int; se fallisce salta l'aggiornamento
                try:
                    supplier_id = int(suffix)
                except Exception:
                    supplier_id = None
                if supplier_id:
                    try:
                        email_conn.execute(
                            f"UPDATE {SUPPLIER_TABLE} SET email=? WHERE id=?",
                            (email_val, supplier_id),
                        )
                    except sqlite3.Error:
                        pass
            email_conn.commit()
    except Exception:
        # Non interrompere la conferma in caso di errore sull'aggiornamento email
        pass
    # Estrai fornitori selezionati (lista di ID)
    supplier_ids_raw = request.form.getlist('fornitore_ids')
    supplier_ids: list[int] = []
    for sid in supplier_ids_raw:
        try:
            supplier_ids.append(int(sid))
        except (TypeError, ValueError):
            continue
    # Estrai gli ID dei produttori selezionati (lista di ID).  Questo campo
    # viene mantenuto per compatibilità con versioni precedenti ma non è
    # più utilizzato perché il produttore per ciascuna combinazione viene
    # specificato direttamente nelle righe selezionate.  Conserviamo
    # comunque i valori in una lista per eventuale uso futuro.
    producer_ids_raw = request.form.getlist('produttore_ids')
    producer_ids: list[int] = []
    for pid in producer_ids_raw:
        try:
            producer_ids.append(int(pid))
        except (TypeError, ValueError):
            continue
    # Estrai le liste di combinazioni dal form
    materials = request.form.getlist('materiale')
    types = request.form.getlist('tipo')
    spessori = request.form.getlist('spessore')
    dxs = request.form.getlist('dimensione_x')
    dys = request.form.getlist('dimensione_y')
    quantities = request.form.getlist('quantita')
    produttori = request.form.getlist('produttore')
    # Normalizza lunghezza degli array includendo il campo produttore
    length = max(len(materials), len(types), len(spessori), len(dxs), len(dys), len(quantities), len(produttori))
    def get_val(lst: list, idx: int) -> str:
        return lst[idx] if idx < len(lst) else ''
    items: list[dict] = []
    for i in range(length):
        items.append({
            'materiale': get_val(materials, i),
            'tipo': get_val(types, i),
            'spessore': get_val(spessori, i),
            'dimensione_x': get_val(dxs, i),
            'dimensione_y': get_val(dys, i),
            'quantita': get_val(quantities, i),
            'produttore': get_val(produttori, i),
        })
    # Recupera fornitori selezionati o dedotti e template email dell'utente
    suppliers: list[dict] = []
    producers: list[dict] = []
    ordine_template = ''
    with get_db_connection() as conn:
        # Fornitori selezionati manualmente
        if supplier_ids:
            for sid in supplier_ids:
                try:
                    row = conn.execute(
                        f"SELECT id, nome, email FROM {SUPPLIER_TABLE} WHERE id=?",
                        (sid,),
                    ).fetchone()
                    if row:
                        suppliers.append(dict(row))
                except sqlite3.Error:
                    continue
        # Se non specificato, deduci fornitori dai materiali
        if not suppliers:
            deduced_names: set[str] = set()
            for itm in items:
                mat = (itm.get('materiale') or '').strip()
                tp = (itm.get('tipo') or '').strip()
                sp = (itm.get('spessore') or '').strip()
                dx = (itm.get('dimensione_x') or '').strip()
                dy = (itm.get('dimensione_y') or '').strip()
                if not mat:
                    continue
                try:
                    for_rows = conn.execute(
                        "SELECT DISTINCT fornitore FROM materiali WHERE materiale=? "
                        "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                        "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                        "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                        "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                        "AND (fornitore IS NOT NULL AND TRIM(fornitore)!='') "
                        "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                        (
                            mat,
                            tp if tp else None, tp,
                            sp if sp else None, sp,
                            dx if dx else None, dx,
                            dy if dy else None, dy,
                        ),
                    ).fetchall()
                    for fr in for_rows:
                        fval = (fr['fornitore'] or '').strip()
                        if fval:
                            deduced_names.add(fval)
                except sqlite3.Error:
                    continue
            # Mappa i nomi dedotti con la tabella fornitori
            for name in sorted(deduced_names):
                try:
                    row = conn.execute(
                        f"SELECT id, nome, email FROM {SUPPLIER_TABLE} WHERE nome=? COLLATE NOCASE",
                        (name,),
                    ).fetchone()
                    if row:
                        suppliers.append(dict(row))
                    else:
                        suppliers.append({'id': None, 'nome': name, 'email': None})
                except sqlite3.Error:
                    suppliers.append({'id': None, 'nome': name, 'email': None})
        # Gestione dei produttori.  La pagina riordini invia il nome del
        # produttore per ciascuna combinazione selezionata, quindi
        # costruiamo l'elenco dei produttori univoci direttamente dalle
        # righe selezionate.  Manteniamo il supporto per producer_ids
        # legacy: se vengono forniti verranno utilizzati per integrare
        # l'elenco dei produttori.
        unique_prod_names: list[str] = []
        seen_names: set[str] = set()
        # Includi i nomi dai campi produttore delle righe selezionate
        for itm in items:
            pname = (itm.get('produttore') or '').strip()
            if pname and pname not in seen_names:
                seen_names.add(pname)
                unique_prod_names.append(pname)
        # Includi eventuali produttori selezionati manualmente
        if producer_ids:
            for pid in producer_ids:
                try:
                    row = conn.execute(
                        f"SELECT id, nome FROM {PRODUTTORE_TABLE} WHERE id=?",
                        (pid,),
                    ).fetchone()
                    if row:
                        # se il nome non è ancora presente, aggiungi
                        nm = (row['nome'] or '').strip()
                        if nm and nm not in seen_names:
                            seen_names.add(nm)
                            unique_prod_names.append(nm)
                except sqlite3.Error:
                    continue
        # Costruisci la lista 'producers' con id e nome (cerca nel vocabolario)
        for pname in unique_prod_names:
            try:
                row = conn.execute(
                    f"SELECT id, nome FROM {PRODUTTORE_TABLE} WHERE nome=? COLLATE NOCASE",
                    (pname,),
                ).fetchone()
                if row:
                    producers.append({'id': row['id'], 'nome': row['nome']})
                else:
                    producers.append({'id': None, 'nome': pname})
            except sqlite3.Error:
                producers.append({'id': None, 'nome': pname})
        # Recupera template email per l'utente corrente
        user_id = session.get('user_id')
        if user_id:
            try:
                row = conn.execute(
                    f"SELECT ordine_template FROM {USERS_TABLE} WHERE id=?", (user_id,),
                ).fetchone()
                if row and row['ordine_template']:
                    ordine_template = row['ordine_template']
            except sqlite3.Error:
                ordine_template = ''
    # Componi l'email (oggetto e corpo) come in prepara_ordine includendo i produttori
    date_str = datetime.now().strftime('%d/%m/%Y')
    subject = f"Richiesta riordino – {date_str}"
    # Se l'utente ha fornito un soggetto personalizzato tramite il form, usalo
    # al posto del soggetto generato automaticamente.
    submitted_subject = request.form.get('subject')
    if submitted_subject:
        # Conserva il testo così com'è (senza formattazioni extra) per rispetto
        # delle preferenze dell'utente
        subject = submitted_subject.strip() or subject
    body_lines: list[str] = []
    if ordine_template:
        body_lines.append(ordine_template.strip())
        body_lines.append('')
    body_lines.append('Dettaglio materiali da riordinare:')
    # Pre-calcola i produttori per ciascun articolo.  Se il campo
    # ``produttore`` dell'item è valorizzato, utilizza direttamente
    # quel valore; altrimenti deduci i produttori dal magazzino.
    item_producers: list[list[str]] = []
    with get_db_connection() as conn_tmp:
        for itm in items:
            pname = (itm.get('produttore') or '').strip()
            if pname:
                item_producers.append([pname])
                continue
            mat = (itm.get('materiale') or '').strip()
            tp_i = (itm.get('tipo') or '').strip()
            sp_i = (itm.get('spessore') or '').strip()
            dx_i = (itm.get('dimensione_x') or '').strip()
            dy_i = (itm.get('dimensione_y') or '').strip()
            prod_names: list[str] = []
            if mat:
                try:
                    pr_rows = conn_tmp.execute(
                        "SELECT DISTINCT produttore FROM materiali WHERE materiale=? "
                        "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                        "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                        "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                        "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                        "AND (produttore IS NOT NULL AND TRIM(produttore)!='') "
                        "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                        (
                            mat,
                            tp_i if tp_i else None, tp_i,
                            sp_i if sp_i else None, sp_i,
                            dx_i if dx_i else None, dx_i,
                            dy_i if dy_i else None, dy_i,
                        ),
                    ).fetchall()
                    seen_p: set[str] = set()
                    for pr in pr_rows:
                        val = (pr['produttore'] or '').strip()
                        if val and val not in seen_p:
                            seen_p.add(val)
                            prod_names.append(val)
                except sqlite3.Error:
                    prod_names = []
            item_producers.append(prod_names)
    for idx, itm in enumerate(items):
        dx_val = (itm.get('dimensione_x') or '').strip()
        dy_val = (itm.get('dimensione_y') or '').strip()
        dims = ''
        if dx_val or dy_val:
            dims = f" {dx_val}x{dy_val}"
        tipo_val = (itm.get('tipo') or '').strip()
        sp_val = (itm.get('spessore') or '').strip()
        desc_parts = [itm.get('materiale') or '']
        if tipo_val:
            desc_parts.append(tipo_val)
        if sp_val:
            desc_parts.append(sp_val)
        descr = ' '.join([p for p in desc_parts if p])
        qty_val = itm.get('quantita')
        prod_names = item_producers[idx] if idx < len(item_producers) else []
        if prod_names:
            prod_str = '/'.join(prod_names)
            body_lines.append(f"- {descr}{dims}: {qty_val} (Produttore: {prod_str})")
        else:
            body_lines.append(f"- {descr}{dims}: {qty_val}")
    body = '\n'.join(body_lines)
    # Se l'utente ha fornito un corpo personalizzato tramite il form,
    # utilizza direttamente quello.  Questo permette di modificare
    # liberamente il testo dell'email nella fase di preparazione.
    submitted_body = request.form.get('body')
    if submitted_body:
        # Non rimuovere spazi o ritorni a capo: l'utente potrebbe voler
        # conservare la formattazione specifica.  Usa il testo così com'è.
        body = submitted_body
    # Prepara lista email destinatari
    email_list = [s['email'] for s in suppliers if s and s.get('email')]
    email_sent = False
    if email_list:
        # Carica la configurazione SMTP dal file, se disponibile.  Le
        # impostazioni lette sovrascrivono temporaneamente le variabili
        # d'ambiente corrispondenti per questa richiesta.  Questo
        # consente di modificare server, porta, credenziali e altre
        # opzioni senza riavviare l'applicazione.
        smtp_conf = load_smtp_config()
        if smtp_conf:
            host_val = smtp_conf.get('host')
            port_val = smtp_conf.get('port')
            user_val = smtp_conf.get('user')
            pass_val = smtp_conf.get('pass')
            from_val = smtp_conf.get('from')
            tls_val = smtp_conf.get('tls')
            if host_val:
                os.environ['SMTP_HOST'] = host_val
            if port_val:
                os.environ['SMTP_PORT'] = str(port_val)
            if user_val:
                os.environ['SMTP_USER'] = user_val
            if pass_val is not None:
                os.environ['SMTP_PASS'] = pass_val
            if from_val:
                os.environ['SMTP_FROM'] = from_val
            if tls_val:
                os.environ['SMTP_TLS'] = tls_val
        smtp_host = os.environ.get('SMTP_HOST')
        if smtp_host:
            try:
                smtp_port = int(os.environ.get('SMTP_PORT', '25'))
            except Exception:
                smtp_port = 25
            smtp_user = os.environ.get('SMTP_USER')
            smtp_pass = os.environ.get('SMTP_PASS')
            smtp_from = os.environ.get('SMTP_FROM') or smtp_user or 'no-reply@example.com'
            try:
                msg = EmailMessage()
                msg['Subject'] = subject
                msg['From'] = smtp_from
                msg['To'] = ','.join(email_list)
                msg.set_content(body)
                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    if os.environ.get('SMTP_TLS', 'True').lower() in ('true', '1', 'yes'):
                        try:
                            server.starttls()
                        except Exception:
                            pass
                    if smtp_user:
                        try:
                            server.login(smtp_user, smtp_pass or '')
                        except Exception:
                            pass
                    server.send_message(msg)
                email_sent = True
            except Exception:
                email_sent = False
        else:
            email_sent = False
    # Prepara l'elenco dei fornitori e produttori associati all'ordine e determinare
    # i valori pre-bloccati (locked) se c'è un solo elemento.  Il fornitore
    # e il produttore scelti verranno salvati nello storico dell'ordine se unici.
    fornitore_name_str = None  # nome del fornitore scelto (se unico)
    fornitori_list_str = None  # lista CSV di tutti i fornitori da salvare
    pre_locked = 0             # 1 se c'è un solo fornitore e viene pre-bloccato
    forn_names: list[str] = []
    if suppliers:
        for s in suppliers:
            nome = (s.get('nome') or '').strip()
            if nome:
                forn_names.append(nome)
        if forn_names:
            fornitori_list_str = ','.join([n.strip() for n in forn_names])
            if len(forn_names) == 1:
                fornitore_name_str = forn_names[0]
                pre_locked = 1
    produttore_name_str = None  # nome del produttore scelto (se unico)
    produttori_list_str = None  # lista CSV di tutti i produttori da salvare
    prod_pre_locked = 0         # 1 se c'è un solo produttore e viene pre-bloccato
    prod_names: list[str] = []
    if producers:
        for p in producers:
            nomep = (p.get('nome') or '').strip()
            if nomep:
                prod_names.append(nomep)
        if prod_names:
            produttori_list_str = ','.join([n.strip() for n in prod_names])
            if len(prod_names) == 1:
                produttore_name_str = prod_names[0]
                prod_pre_locked = 1

    # Crea le richieste d'ordine (RDO) per ciascuna combinazione selezionata.
    # Invece di registrare immediatamente l'ordine nelle tabelle di accettazione,
    # inseriamo le righe nella tabella ``riordini_rdo``.  In questo modo
    # l'utente potrà gestire la fase RDO (selezione fornitore/produttore, date)
    # prima di confermare e passare all'accettazione.
    created_rdo = 0
    try:
        with get_db_connection() as conn:
            # Pre-carica i nomi dei fornitori selezionati (o dedotti)
            supplier_names_list: list[str] = []
            if suppliers:
                for sup in suppliers:
                    nome = (sup.get('nome') or '').strip()
                    if nome:
                        supplier_names_list.append(nome)
            # Pre-carica i nomi dei produttori selezionati (o dedotti)
            producer_names_list: list[str] = []
            if producers:
                for p in producers:
                    nomep = (p.get('nome') or '').strip()
                    if nomep:
                        producer_names_list.append(nomep)
            # Funzione per deduplicare mantenendo l'ordine
            def dedup(lst: list[str]) -> list[str]:
                seen = set()
                out: list[str] = []
                for val in lst:
                    if val not in seen:
                        seen.add(val)
                        out.append(val)
                return out
            supplier_names_list = dedup(supplier_names_list)
            producer_names_list = dedup(producer_names_list)
            for itm in items:
                # Converti quantità
                try:
                    q = int(itm.get('quantita') or 0)
                except (TypeError, ValueError):
                    q = 0
                if q <= 0:
                    continue
                mat = (itm.get('materiale') or '').strip() or None
                tp = (itm.get('tipo') or '').strip() or None
                sp = (itm.get('spessore') or '').strip() or None
                dx = (itm.get('dimensione_x') or '').strip() or None
                dy = (itm.get('dimensione_y') or '').strip() or None
                # Deduce fornitori se non selezionati: usa supplier_names_list già dedotta se presente
                forn_list = supplier_names_list[:]
                if not forn_list:
                    # Deduce fornitori dal magazzino per questa combinazione
                    try:
                        rows_f = conn.execute(
                            "SELECT DISTINCT fornitore FROM materiali WHERE materiale=? "
                            "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                            "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                            "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                            "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                            "AND (fornitore IS NOT NULL AND TRIM(fornitore)!='') "
                            "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                            (mat, tp or None, tp, sp or None, sp, dx or None, dx, dy or None, dy)
                        ).fetchall()
                        for rf in rows_f:
                            val = (rf['fornitore'] or '').strip()
                            if val:
                                forn_list.append(val)
                    except sqlite3.Error:
                        forn_list = []
                # Determina i produttori per questa combinazione.  Se nella
                # riga di input è presente un produttore specificato,
                # utilizza direttamente tale valore; altrimenti usa i
                # produttori globali selezionati o dedotti dal magazzino.
                prod_list: list[str] = []
                pval = (itm.get('produttore') or '').strip()
                if pval:
                    prod_list.append(pval)
                else:
                    # Usa l'elenco globale dei produttori se presente
                    prod_list = producer_names_list[:]
                    # Se ancora vuoto deduci i produttori dal magazzino
                    if not prod_list:
                        try:
                            rows_p = conn.execute(
                                "SELECT DISTINCT produttore FROM materiali WHERE materiale=? "
                                "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                                "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                                "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                                "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                                "AND (produttore IS NOT NULL AND TRIM(produttore)!='') "
                                "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                                (mat, tp or None, tp, sp or None, sp, dx or None, dx, dy or None, dy)
                            ).fetchall()
                            for rp in rows_p:
                                val = (rp['produttore'] or '').strip()
                                if val:
                                    prod_list.append(val)
                        except sqlite3.Error:
                            prod_list = []
                # Deduplica i fornitori e i produttori mantenendo l'ordine
                forn_list = dedup(forn_list)
                prod_list = dedup(prod_list)
                try:
                    # Data corrente per la riga RDO
                    now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
                    conn.execute(
                        "INSERT INTO riordini_rdo (data, materiale, tipo, spessore, dimensione_x, dimensione_y, quantita, fornitori, produttori) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            now_str,
                            mat,
                            tp,
                            sp,
                            dx,
                            dy,
                            q,
                            ','.join(forn_list) if forn_list else None,
                            ','.join(prod_list) if prod_list else None,
                        ),
                    )
                    created_rdo += 1
                except sqlite3.Error:
                    continue
            if created_rdo:
                conn.commit()
    except Exception:
        created_rdo = 0
    # Prepara messaggi di conferma
    if email_sent and created_rdo:
        flash(f'Ordine confermato, email inviata. Sono state create {created_rdo} richieste d\'ordine (RDO).', 'success')
    elif email_sent and not created_rdo:
        flash('Ordine confermato, email inviata. Nessuna richiesta d\'ordine è stata creata.', 'warning')
    elif not email_sent and created_rdo:
        flash(f'Ordine confermato (email non inviata). Sono state create {created_rdo} richieste d\'ordine (RDO).', 'success')
    else:
        flash('Ordine confermato (email non inviata). Nessuna richiesta d\'ordine è stata creata.', 'warning')
    return redirect(url_for('riordini'))

# ---------------------------------------------------------------------------
# Aggiornamento stato di accettazione: aggiungi quantità parziale o modifica totale

@app.route('/accettazione_update/<int:acc_id>', methods=['POST'])
def accettazione_update(acc_id: int):
    """Reindirizza l'aggiornamento di una riga di accettazione alla pagina di aggiunta.

    Questo endpoint non aggiorna immediatamente la tabella ``riordini_accettazione``.
    Invece, recupera le quantità parziali inviate dal form e reindirizza l'utente
    alla pagina di inserimento materiale (``/add``) con i parametri precompilati.
    La logica di aggiornamento e chiusura dell'accettazione sarà eseguita dopo
    l'inserimento dei materiali.
    """
    # Quantità inserita nel form: può essere "quantita" (nuovo campo) oppure "quantita_parziale" per compatibilità.
    qty_raw = request.form.get('quantita')
    if qty_raw in (None, '', 'None'):
        qty_raw = request.form.get('quantita_parziale') or '0'
    try:
        q_parziale = int(qty_raw or '0')
    except (TypeError, ValueError):
        q_parziale = 0
    # Recupera la riga di accettazione per passare i parametri di combinazione e per calcolare il residuo.
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT * FROM riordini_accettazione WHERE id=?",
            (acc_id,)
        ).fetchone()
    if not row:
        flash('Riga di accettazione non trovata.', 'danger')
        return redirect(url_for('riordini'))
    # Verifica che per questo ordine sia stato selezionato un fornitore bloccato.
    # Se non è stato ancora scelto un fornitore (locked=0 o fornitore_scelto vuoto),
    # l'accettazione non è consentita.
    try:
        numero_ord = row['numero_ordine']
    except Exception:
        numero_ord = None
    if numero_ord:
        try:
            with get_db_connection() as conn_check:
                frn_row = conn_check.execute(
                    "SELECT locked, fornitore_scelto FROM ordine_fornitori WHERE numero_ordine=?",
                    (str(numero_ord),)
                ).fetchone()
        except sqlite3.Error:
            frn_row = None
        # Se la riga esiste e non è bloccata o non ha un fornitore selezionato, blocca l'accettazione
        if not frn_row or not frn_row['locked'] or not (frn_row['fornitore_scelto'] or '').strip():
            flash('Seleziona e conferma un fornitore prima di accettare l\'ordine.', 'danger')
            return redirect(url_for('riordini'))
    # Estrai quantità totale e ricevuta correnti
    try:
        current_total = int(row['quantita_totale'] or 0)
    except (ValueError, TypeError):
        current_total = 0
    try:
        current_received = int(row['quantita_ricevuta'] or 0)
    except (ValueError, TypeError):
        current_received = 0
    # L'utente non può modificare il totale: usa sempre l'attuale totale per il calcolo del residuo.
    effective_total = current_total
    residuo = effective_total - current_received
    if residuo < 0:
        residuo = 0
    # Validazioni: q_parziale deve essere >0 e <= residuo
    if q_parziale <= 0:
        flash('La quantità da accettare deve essere maggiore di zero.', 'danger')
        return redirect(url_for('riordini'))
    if q_parziale > residuo:
        flash('La quantità da accettare non può superare la quantità residua dell’ordine.', 'danger')
        return redirect(url_for('riordini'))
    # Costruisci i parametri di redirezione per precompilare la pagina /add.  Oltre ai
    # parametri standard (materiale, tipo, spessore, dimensioni), verranno
    # determinati il fornitore e il produttore correntemente selezionati per
    # l'ordine e, se possibile, l'ubicazione suggerita in base alla combinazione
    # materiale/tipo/spessore/fornitore/produttore.  In questo modo l'utente
    # troverà i campi del modulo /add già impostati in base allo stato dell'ordine.
    # Parametri base obbligatori
    params = {
        'acc_id': acc_id,
        'q_parziale': q_parziale,
        'materiale': row['materiale'] or '',
        'tipo': row['tipo'] or '',
        'spessore': row['spessore'] or '',
        'dimensione_x': row['dimensione_x'] or '',
        'dimensione_y': row['dimensione_y'] or '',
    }
    # Determina il fornitore e il produttore effettivamente selezionati per questo ordine.
    # Se esistono voci nella tabella ordine_fornitori o ordine_produttori con il
    # campo locked=1 e un valore scelto, questi prevalgono sui valori presenti
    # nella riga di accettazione.  In caso contrario, utilizziamo i valori
    # attualmente presenti nella riga di accettazione (che potrebbero essere
    # ancora vuoti).  Questo consente di tenere conto dei cambiamenti di
    # fornitore/produttore effettuati dall'utente dopo la creazione dell'ordine.
    selected_forn = None
    selected_prod = None
    if numero_ord:
        try:
            with get_db_connection() as conn_sel:
                # Controlla eventuale fornitore bloccato
                try:
                    frn_row = conn_sel.execute(
                        "SELECT fornitore_scelto, locked FROM ordine_fornitori WHERE numero_ordine=?",
                        (str(numero_ord),)
                    ).fetchone()
                except sqlite3.Error:
                    frn_row = None
                # Controlla eventuale produttore bloccato
                try:
                    prod_row = conn_sel.execute(
                        "SELECT produttore_scelto, locked FROM ordine_produttori WHERE numero_ordine=?",
                        (str(numero_ord),)
                    ).fetchone()
                except sqlite3.Error:
                    prod_row = None
        except Exception:
            frn_row = None
            prod_row = None
        # Converti eventuali righe in dizionari per uso comodo di .get
        if frn_row is not None:
            try:
                frn_row = dict(frn_row)
            except Exception:
                pass
        if prod_row is not None:
            try:
                prod_row = dict(prod_row)
            except Exception:
                pass
        # Se il fornitore è stato bloccato per l'ordine, usalo come default
        if frn_row and frn_row.get('locked') and (frn_row.get('fornitore_scelto') or '').strip():
            selected_forn = (frn_row.get('fornitore_scelto') or '').strip()
        # Prima di tutto, tenta di utilizzare il produttore indicato nella riga di accettazione.
        try:
            row_prod_val = (row['produttore'] or '').strip() if row['produttore'] is not None else ''
        except Exception:
            row_prod_val = ''
        if row_prod_val:
            selected_prod = row_prod_val
        # Se la riga non specifica alcun produttore, usa eventuale produttore bloccato dall'ordine
        elif prod_row and prod_row.get('locked') and (prod_row.get('produttore_scelto') or '').strip():
            selected_prod = (prod_row.get('produttore_scelto') or '').strip()
    # Fallback sui valori presenti nella riga di accettazione se non sono stati
    # determinati da ordine_fornitori/ordine_produttori.
    if not selected_forn:
        try:
            selected_forn = row['fornitore'] if row['fornitore'] is not None else ''
        except Exception:
            selected_forn = ''
    if not selected_prod:
        try:
            selected_prod = row['produttore'] if row['produttore'] is not None else ''
        except Exception:
            selected_prod = ''
    # Popola i parametri con i fornitori/produttori selezionati se non vuoti
    if selected_forn:
        params['fornitore'] = selected_forn
    if selected_prod:
        params['produttore'] = selected_prod
    # Determina l'ubicazione da precompilare.  Se l'ordine ha già registrato una
    # ubicazione (prima accettazione) e il produttore corrente non è cambiato,
    # utilizziamo quella.  In caso contrario cerchiamo un bancale esistente per
    # la combinazione materiale/tipo/spessore/fornitore/produttore in modo da
    # suggerire la stessa ubicazione del bancale già presente in magazzino.  Se
    # nessun bancale è trovato, utilizziamo comunque l'ubicazione salvata nella
    # riga di accettazione se esiste; altrimenti l'utente dovrà selezionarla manualmente.
    # Ubicazione memorizzata nella riga di accettazione (prima accettazione)
    try:
        row_loc_lettera = row['ubicazione_lettera'] if row['ubicazione_lettera'] is not None else None
    except Exception:
        row_loc_lettera = None
    try:
        row_loc_numero = row['ubicazione_numero'] if row['ubicazione_numero'] is not None else None
    except Exception:
        row_loc_numero = None
    # Produttore originale della riga di accettazione (prima della selezione eventuale)
    try:
        row_prod_original = row['produttore'] if row['produttore'] is not None else ''
    except Exception:
        row_prod_original = ''
    prefill_lettera = None
    prefill_numero = None
    # Se esiste una ubicazione memorizzata e il produttore non è cambiato, riutilizzala
    if row_loc_lettera and ((row_prod_original or '') == (selected_prod or '')):
        prefill_lettera = row_loc_lettera
        prefill_numero = row_loc_numero
    else:
        # Altrimenti cerca un bancale esistente con la stessa combinazione.  Le colonne
        # tipo e spessore possono essere NULL, per cui utilizziamo OR nei confronti.
        try:
            with get_db_connection() as conn_search:
                pal = conn_search.execute(
                    "SELECT ubicazione_lettera, ubicazione_numero FROM materiali WHERE is_pallet=1 AND materiale=? "
                    "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                    "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                    "AND (fornitore=? OR (fornitore IS NULL AND ?='')) "
                    "AND (produttore=? OR (produttore IS NULL AND ?='')) "
                    "AND (is_sfrido IS NULL OR is_sfrido != 1) ORDER BY id LIMIT 1",
                    (
                        row['materiale'],
                        row['tipo'] or None, row['tipo'] or '',
                        row['spessore'] or None, row['spessore'] or '',
                        selected_forn or None, selected_forn or '',
                        selected_prod or None, selected_prod or ''
                    )
                ).fetchone()
                if pal:
                    prefill_lettera = pal['ubicazione_lettera']
                    prefill_numero = pal['ubicazione_numero']
        except Exception:
            prefill_lettera = prefill_numero = None
        # Se non abbiamo trovato alcun bancale, fallback all'ubicazione salvata nella riga
        if not prefill_lettera and row_loc_lettera:
            prefill_lettera = row_loc_lettera
            prefill_numero = row_loc_numero
    # Aggiungi l'ubicazione ai parametri se determinata
    if prefill_lettera:
        params['ubicazione_lettera'] = prefill_lettera
    if prefill_numero is not None:
        params['ubicazione_numero'] = prefill_numero
    # Reindirizza alla pagina di inserimento materiale con i parametri calcolati
    return redirect(url_for('add', **params))

# ---------------------------------------------------------------------------
# Imposta il fornitore scelto per un ordine (numero_ordine).

@app.route('/set_fornitore_ordine', methods=['POST'])
def set_fornitore_ordine():
    """Aggiorna la tabella ordine_fornitori con il fornitore scelto per l'ordine.

    Accetta un JSON con chiavi ``numero_ordine`` e ``fornitore``.  Se l'ordine è già bloccato,
    restituisce errore.  In caso di successo, il campo ``locked`` viene impostato a 1
    e ``fornitore_scelto`` viene aggiornato.
    """
    data = request.get_json(force=True, silent=True) or {}
    numero_ordine = (data.get('numero_ordine') or '').strip()
    fornitore_val = (data.get('fornitore') or '').strip()
    if not numero_ordine or not fornitore_val:
        return jsonify({'success': False, 'error': 'Dati mancanti'}), 400
    try:
        with get_db_connection() as conn:
            # Assicurati che la tabella esista
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ordine_fornitori (numero_ordine TEXT PRIMARY KEY, fornitori TEXT, fornitore_scelto TEXT, locked INTEGER)"
            )
            # Controlla se già selezionato e bloccato
            row = conn.execute(
                "SELECT locked, fornitore_scelto FROM ordine_fornitori WHERE numero_ordine=?",
                (numero_ordine,)
            ).fetchone()
            if row and row['locked']:
                # È già stato selezionato un fornitore: non modificabile
                return jsonify({'success': False, 'error': 'Fornitore già confermato'}), 400
            # Inserisci riga se non presente
            if not row:
                conn.execute(
                    "INSERT OR IGNORE INTO ordine_fornitori (numero_ordine, fornitori, fornitore_scelto, locked) VALUES (?, NULL, NULL, 0)",
                    (numero_ordine,),
                )
            # Aggiorna la scelta e blocca l'ordine
            conn.execute(
                "UPDATE ordine_fornitori SET fornitore_scelto=?, locked=1 WHERE numero_ordine=?",
                (fornitore_val, numero_ordine),
            )
            # Aggiorna anche lo storico dell'ordine (solo righe di tipo 'ordine') con il fornitore selezionato.
            conn.execute(
                "UPDATE riordini_effettuati SET fornitore=? WHERE numero_ordine=? AND tipo_evento='ordine'",
                (fornitore_val, numero_ordine),
            )
            conn.commit()
        return jsonify({'success': True})
    except sqlite3.Error:
        return jsonify({'success': False, 'error': 'Errore DB'}), 500
    # Nessun codice aggiuntivo deve essere eseguito dopo il ritorno JSON di cui sopra.

# ---------------------------------------------------------------------------
# Imposta il produttore scelto per un ordine.  Funziona in modo analogo a
# ``set_fornitore_ordine`` ma utilizza la tabella ``ordine_produttori`` e
# aggiorna il campo ``produttore`` negli eventi dello storico.  La
# selezione del produttore è una operazione "una tantum": una volta
# confermata, non può più essere modificata.

@app.route('/set_produttore_ordine', methods=['POST'])
def set_produttore_ordine():
    """Aggiorna la tabella ordine_produttori con il produttore scelto per l'ordine.

    Accetta un JSON con chiavi ``numero_ordine`` e ``produttore``.  Se
    l'ordine è già stato bloccato, restituisce un errore.  In caso di
    successo, il campo ``locked`` viene impostato a 1 e
    ``produttore_scelto`` viene aggiornato.  Aggiorna inoltre il
    produttore associato all'evento ``ordine`` nella tabella
    ``riordini_effettuati``.
    """
    data = request.get_json(force=True, silent=True) or {}
    numero_ordine = (data.get('numero_ordine') or '').strip()
    produttore_val = (data.get('produttore') or '').strip()
    if not numero_ordine or not produttore_val:
        return jsonify({'success': False, 'error': 'Dati mancanti'}), 400
    try:
        with get_db_connection() as conn:
            # Assicurati che la tabella esista
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ordine_produttori (numero_ordine TEXT PRIMARY KEY, produttori TEXT, produttore_scelto TEXT, locked INTEGER)"
            )
            # Controlla se già selezionato e bloccato
            row = conn.execute(
                "SELECT locked, produttore_scelto FROM ordine_produttori WHERE numero_ordine=?",
                (numero_ordine,)
            ).fetchone()
            if row and row['locked']:
                # È già stato selezionato un produttore: non modificabile
                return jsonify({'success': False, 'error': 'Produttore già confermato'}), 400
            # Inserisci riga se non presente
            if not row:
                conn.execute(
                    "INSERT OR IGNORE INTO ordine_produttori (numero_ordine, produttori, produttore_scelto, locked) VALUES (?, NULL, NULL, 0)",
                    (numero_ordine,),
                )
            # Aggiorna la scelta e blocca l'ordine
            conn.execute(
                "UPDATE ordine_produttori SET produttore_scelto=?, locked=1 WHERE numero_ordine=?",
                (produttore_val, numero_ordine),
            )
            # Aggiorna anche lo storico dell'ordine (solo righe di tipo 'ordine') con il produttore selezionato.
            conn.execute(
                "UPDATE riordini_effettuati SET produttore=? WHERE numero_ordine=? AND tipo_evento='ordine'",
                (produttore_val, numero_ordine),
            )
            conn.commit()
        return jsonify({'success': True})
    except sqlite3.Error:
        return jsonify({'success': False, 'error': 'Errore DB'}), 500

# ---------------------------------------------------------------------------
# Imposta il produttore di una singola riga di accettazione.
#
# A differenza di ``set_produttore_ordine``, che opera a livello di intero
# ordine e blocca la scelta del produttore per tutte le consegne, questo
# endpoint permette di assegnare o modificare il produttore direttamente sulla
# singola riga di ``riordini_accettazione``.  Ciò consente di gestire
# ordini con consegne in date multiple selezionando un produttore diverso per
# ciascuna consegna.  L'operazione aggiorna esclusivamente la colonna
# ``produttore`` della riga di accettazione e non modifica la tabella
# ``ordine_produttori``.

@app.route('/set_produttore_accettazione', methods=['POST'])
def set_produttore_accettazione():
    """Aggiorna il produttore per una riga di accettazione.

    Riceve un JSON con chiavi ``acc_id`` (l'ID della riga nella tabella
    ``riordini_accettazione``) e ``produttore`` (il nome del produttore da
    assegnare).  Verifica che l'utente abbia accesso alla sezione
    ``riordini`` e che la riga esista, quindi aggiorna il campo
    ``produttore``.  In caso di dati mancanti o errori di database,
    restituisce un errore JSON.
    """
    # Controlla autorizzazioni basilari: l'utente deve avere accesso alla tab ``riordini``.
    allowed = session.get('allowed_tabs', []) or []
    if 'riordini' not in allowed:
        return jsonify({'success': False, 'error': 'Accesso non autorizzato'}), 403
    # Recupera e normalizza i dati JSON.
    data = request.get_json(force=True, silent=True) or {}
    try:
        acc_id_raw = data.get('acc_id')
    except Exception:
        acc_id_raw = None
    try:
        produttore_val = (data.get('produttore') or '').strip()
    except Exception:
        produttore_val = ''
    # Validazione: richiede ID e produttore
    try:
        acc_id_int = int(acc_id_raw)
    except (TypeError, ValueError):
        acc_id_int = None
    if not acc_id_int or not produttore_val:
        return jsonify({'success': False, 'error': 'Dati mancanti'}), 400
    try:
        with get_db_connection() as conn:
            # Verifica che la riga di accettazione esista
            row = conn.execute(
                "SELECT id FROM riordini_accettazione WHERE id=?",
                (acc_id_int,),
            ).fetchone()
            if not row:
                return jsonify({'success': False, 'error': 'Riga non trovata'}), 404
            # Aggiorna la colonna produttore per la riga
            conn.execute(
                "UPDATE riordini_accettazione SET produttore=? WHERE id=?",
                (produttore_val, acc_id_int),
            )
            # Non modifichiamo ordine_produttori qui: la scelta resta a livello di riga
            conn.commit()
        return jsonify({'success': True})
    except sqlite3.Error:
        return jsonify({'success': False, 'error': 'Errore DB'}), 500

# ---------------------------------------------------------------------------
# Creazione di richieste d'ordine (RDO)

@app.route('/create_rdo', methods=['POST'])
def create_rdo():
    """Crea uno o più record nella tabella ``riordini_rdo`` a partire dalle
    combinazioni selezionate dall'utente.

    Questa funzione sostituisce la vecchia ``prepara_ordine`` quando è
    attiva la nuova interfaccia a tre fasi (Da gestire → RDO → Accettazione).
    Le combinazioni sono passate tramite campi nascosti ``materiale``,
    ``tipo``, ``spessore``, ``dimensione_x``, ``dimensione_y`` e
    ``quantita``.  Inoltre vengono forniti ``fornitore_ids`` e
    ``produttore_ids`` (opzionali) per specificare l'elenco di
    fornitori/produttori da associare alle righe RDO.  Se non vengono
    specificati, l'applicazione deduce automaticamente i nomi da
    associare interrogando la tabella ``materiali``.
    """
    # Verifica permessi
    allowed = session.get('allowed_tabs', []) or []
    if 'riordini' not in allowed:
        flash('Accesso non autorizzato.', 'danger')
        return redirect(url_for('dashboard'))
    # Raccogli gli ID dei fornitori selezionati
    supplier_ids_raw = request.form.getlist('fornitore_ids') or []
    supplier_ids: list[int] = []
    for sid in supplier_ids_raw:
        try:
            supplier_ids.append(int(sid))
        except (TypeError, ValueError):
            continue
    # Raccogli gli ID dei produttori selezionati
    producer_ids_raw = request.form.getlist('produttore_ids') or []
    producer_ids: list[int] = []
    for pid in producer_ids_raw:
        try:
            producer_ids.append(int(pid))
        except (TypeError, ValueError):
            continue
    # Raccogli le combinazioni selezionate
    materiali = request.form.getlist('materiale') or []
    tipi = request.form.getlist('tipo') or []
    spessori = request.form.getlist('spessore') or []
    dxs = request.form.getlist('dimensione_x') or []
    dys = request.form.getlist('dimensione_y') or []
    quantitas = request.form.getlist('quantita') or []
    # Calcola lunghezza massima per iterare in parallelo
    length = max(len(materiali), len(tipi), len(spessori), len(dxs), len(dys), len(quantitas))
    created = 0
    with get_db_connection() as conn:
        # Pre-carica mappatura id→nome per fornitori selezionati
        supplier_names: list[str] = []
        if supplier_ids:
            for sid in supplier_ids:
                try:
                    row = conn.execute(
                        f"SELECT nome FROM {SUPPLIER_TABLE} WHERE id=?",
                        (sid,),
                    ).fetchone()
                    if row and row['nome']:
                        supplier_names.append(row['nome'])
                except sqlite3.Error:
                    continue
        # Pre-carica mappatura id→nome per produttori selezionati
        producer_names: list[str] = []
        if producer_ids:
            for pid in producer_ids:
                try:
                    row = conn.execute(
                        f"SELECT nome FROM {PRODUTTORE_TABLE} WHERE id=?",
                        (pid,),
                    ).fetchone()
                    if row and row['nome']:
                        producer_names.append(row['nome'])
                except sqlite3.Error:
                    continue
        for i in range(length):
            # Estrai i valori o stringa vuota se non presenti
            mat = materiali[i] if i < len(materiali) else ''
            tp = tipi[i] if i < len(tipi) else ''
            sp = spessori[i] if i < len(spessori) else ''
            dx = dxs[i] if i < len(dxs) else ''
            dy = dys[i] if i < len(dys) else ''
            qty_raw = quantitas[i] if i < len(quantitas) else ''
            try:
                qty = int(qty_raw)
            except (TypeError, ValueError):
                continue
            if qty <= 0:
                continue
            # Deduce fornitori se non specificati
            forn_list: list[str] = []
            if supplier_names:
                forn_list = supplier_names[:]
            else:
                try:
                    rows = conn.execute(
                        "SELECT DISTINCT fornitore FROM materiali WHERE materiale=? "
                        "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                        "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                        "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                        "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                        "AND (fornitore IS NOT NULL AND TRIM(fornitore)!='') "
                        "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                        (mat, tp or None, tp, sp or None, sp, dx or None, dx, dy or None, dy),
                    ).fetchall()
                    for fr in rows:
                        val = (fr['fornitore'] or '').strip()
                        if val:
                            forn_list.append(val)
                except sqlite3.Error:
                    forn_list = []
            # Deduce produttori se non specificati
            prod_list: list[str] = []
            if producer_names:
                prod_list = producer_names[:]
            else:
                try:
                    rows = conn.execute(
                        "SELECT DISTINCT produttore FROM materiali WHERE materiale=? "
                        "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                        "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                        "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                        "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                        "AND (produttore IS NOT NULL AND TRIM(produttore)!='') "
                        "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                        (mat, tp or None, tp, sp or None, sp, dx or None, dx, dy or None, dy),
                    ).fetchall()
                    for pr in rows:
                        val = (pr['produttore'] or '').strip()
                        if val:
                            prod_list.append(val)
                except sqlite3.Error:
                    prod_list = []
            # Normalizza rimuovendo duplicati mantenendo l'ordine
            def dedup(lst: list[str]) -> list[str]:
                seen = set()
                out: list[str] = []
                for val in lst:
                    if val not in seen:
                        seen.add(val)
                        out.append(val)
                return out
            forn_list = dedup(forn_list)
            prod_list = dedup(prod_list)
            # Inserisci la riga nella tabella riordini_rdo
            try:
                conn.execute(
                    "INSERT INTO riordini_rdo (data, materiale, tipo, spessore, dimensione_x, dimensione_y, quantita, fornitori, produttori) "
                    "VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        mat or None,
                        tp or None,
                        sp or None,
                        dx or None,
                        dy or None,
                        qty,
                        ','.join(forn_list) if forn_list else None,
                        ','.join(prod_list) if prod_list else None,
                    ),
                )
                created += 1
            except sqlite3.Error:
                continue
        if created:
            conn.commit()
    if created:
        flash(f'Sono state aggiunte {created} richieste d\'ordine (RDO).', 'success')
    else:
        flash('Nessuna richiesta d\'ordine è stata creata.', 'warning')
    return redirect(url_for('riordini'))

# ---------------------------------------------------------------------------
# Aggiornamento di una riga RDO

@app.route('/update_rdo/<int:rdo_id>', methods=['POST'])
def update_rdo(rdo_id: int):
    """Aggiorna una riga esistente nella tabella ``riordini_rdo``.

    L'utente può modificare la quantità, selezionare il fornitore e il
    produttore preferiti, impostare una data di arrivo singola oppure
    specificare più date/quantità mediante il campo ``multiple_dates``.
    Le date multiple devono essere una stringa con coppie ``YYYY-MM-DD:qty``
    separate da virgola.  Se vengono fornite date multiple, la data
    singola ``data_prevista`` viene ignorata e le righe vengono create
    successivamente in base alla tabella ``rdo_dates``.
    """
    allowed = session.get('allowed_tabs', []) or []
    if 'riordini' not in allowed:
        flash('Accesso non autorizzato.', 'danger')
        return redirect(url_for('dashboard'))
    quantita_raw = request.form.get('quantita', '').strip()
    fornitore_scelto = (request.form.get('fornitore_scelto') or '').strip()
    produttore_scelto = (request.form.get('produttore_scelto') or '').strip()
    data_prevista = (request.form.get('data_prevista') or '').strip()
    multiple_dates = (request.form.get('multiple_dates') or '').strip()
    try:
        quantita_val = int(quantita_raw)
    except (TypeError, ValueError):
        quantita_val = None
    if quantita_val is not None and quantita_val <= 0:
        quantita_val = None
    with get_db_connection() as conn:
        # Aggiorna campi base
        fields: list[str] = []
        params: list = []
        if quantita_val is not None:
            fields.append('quantita=?')
            params.append(quantita_val)
        # aggiorna fornitore_scelto solo se fornito
        fields.append('fornitore_scelto=?')
        params.append(fornitore_scelto or None)
        fields.append('produttore_scelto=?')
        params.append(produttore_scelto or None)
        # Gestisci data_prevista se non stiamo usando multiple_dates
        if multiple_dates:
            # Clear data_prevista when multiple dates are provided
            fields.append('data_prevista=?')
            params.append(None)
        else:
            fields.append('data_prevista=?')
            params.append(data_prevista or None)
        params.append(rdo_id)
        try:
            conn.execute(
                f"UPDATE riordini_rdo SET {', '.join(fields)} WHERE id=?",
                params,
            )
        except sqlite3.Error:
            flash('Errore nell\'aggiornamento della RDO.', 'danger')
            return redirect(url_for('riordini'))
        # Gestisci le date multiple: cancella date esistenti e reinserisci applicando i limiti sulle quantità.
        if multiple_dates:
            # Elimina vecchi record per questa RDO
            try:
                conn.execute("DELETE FROM rdo_dates WHERE rdo_id=?", (rdo_id,))
            except sqlite3.Error:
                pass
            # Determina la quantità totale dell'ordine per applicare i limiti
            total_qty = None
            # Se l'utente ha specificato una nuova quantità, usa quella
            if quantita_val is not None:
                total_qty = quantita_val
            else:
                # Altrimenti recupera la quantità corrente dalla riga RDO
                try:
                    r = conn.execute("SELECT quantita FROM riordini_rdo WHERE id=?", (rdo_id,)).fetchone()
                    if r and r['quantita'] is not None:
                        total_qty = int(r['quantita'])
                    else:
                        total_qty = 0
                except Exception:
                    total_qty = 0
            if total_qty is None:
                total_qty = 0
            # Prepara le coppie pulite applicando i vincoli: la prima quantità almeno 1 e le successive non superano il residuo
            remaining = total_qty
            pairs_raw = [p.strip() for p in multiple_dates.split(',') if p.strip()]
            for idx, pair in enumerate(pairs_raw):
                try:
                    date_str, qty = pair.split(':', 1)
                except ValueError:
                    continue
                dt_str = date_str.strip()
                if not dt_str:
                    continue
                try:
                    qty_int = int(str(qty).strip())
                except (TypeError, ValueError):
                    qty_int = 0
                # La prima consegna deve avere almeno 1 unità
                if idx == 0 and qty_int < 1:
                    qty_int = 1
                # Limita la quantità alla quantità residua
                if qty_int > remaining:
                    qty_int = remaining
                # Se la quantità dopo il clamp è positiva, inserisci la riga
                if qty_int > 0:
                    try:
                        conn.execute(
                            "INSERT INTO rdo_dates (rdo_id, data_prevista, quantita) VALUES (?, ?, ?)",
                            (rdo_id, dt_str, qty_int),
                        )
                    except sqlite3.Error:
                        pass
                    remaining -= qty_int
                # Termina se non c'è più residuo
                if remaining <= 0:
                    break
        conn.commit()
    flash('Riga RDO aggiornata.', 'success')
    return redirect(url_for('riordini'))

# ---------------------------------------------------------------------------
# Conferma di una riga RDO e migrazione verso accettazione

@app.route('/confirm_rdo/<int:rdo_id>', methods=['POST'])
def confirm_rdo(rdo_id: int):
    """Conferma una richiesta d'ordine (RDO) esistente.

    Questa operazione genera un nuovo numero d'ordine, aggiorna le tabelle
    ``ordine_fornitori`` e ``ordine_produttori`` con le liste complete e
    la scelta selezionata, inserisce un evento ``ordine`` nello
    storico (riordini_effettuati) e popola la tabella
    ``riordini_accettazione`` con una o più righe a seconda di quante
    date di consegna sono state definite (singola o multiple).  Alla
    fine la riga RDO e le eventuali date multiple vengono rimosse.
    """
    allowed = session.get('allowed_tabs', []) or []
    if 'riordini' not in allowed:
        flash('Accesso non autorizzato.', 'danger')
        return redirect(url_for('dashboard'))
    with get_db_connection() as conn:
        # Recupera la riga RDO
        row = conn.execute("SELECT * FROM riordini_rdo WHERE id=?", (rdo_id,)).fetchone()
        if not row:
            flash('RDO non trovata.', 'warning')
            return redirect(url_for('riordini'))
        rdo = dict(row)
        # Prima di confermare, aggiorna la riga RDO in base ai dati inviati dal form.
        # In questo modo la conferma ingloba anche eventuali modifiche di quantità,
        # fornitore, produttore e date di consegna, rendendo superfluo un pulsante "Salva".
        try:
            quantita_raw = request.form.get('quantita', '').strip()
            quantita_val = None
            if quantita_raw:
                try:
                    quantita_tmp = int(quantita_raw)
                    if quantita_tmp > 0:
                        quantita_val = quantita_tmp
                except ValueError:
                    quantita_val = None
            # Fornitore scelto e produttore scelto (opzionale).
            form_forn = (request.form.get('fornitore_scelto') or '').strip()
            # Il produttore non è più richiesto obbligatoriamente; se non fornito
            # viene mantenuto quello presente nel record RDO.  Recuperalo dal form,
            # ma potrebbe essere una stringa vuota.  Quando l'utente specifica
            # date multiple con relativi produttori (prods[]), questo valore
            # rappresenta il produttore della riga principale o il default per
            # eventuali date senza produttore specificato.
            form_prod = (request.form.get('produttore_scelto') or '').strip()
            # Recupera le liste di date e quantità inserite dal form.  Saranno
            # utilizzate per popolare la tabella ``rdo_dates``.  Anche nel caso in
            # cui venga inserita una sola data, usiamo sempre le liste.  Oltre a
            # queste liste vengono recuperati i produttori associati alle date
            # tramite il campo ``produttore_scelti[]`` (in precedenza ``prods[]``) se presente.
            dates_list = request.form.getlist('dates[]') or request.form.getlist('dates')
            qtys_list = request.form.getlist('qtys[]') or request.form.getlist('qtys')
            # Recupera la lista dei produttori specificata per ciascuna data.
            # In precedenza questo campo si chiamava ``prods[]``; dopo le
            # modifiche all'interfaccia utente è stato rinominato in
            # ``produttore_scelti[]`` per maggiore chiarezza.  Per
            # retro‑compatibilità cerchiamo prima ``produttore_scelti[]`` e
            # ``produttore_scelti``, poi ``prods[]``/``prods``.
            prods_list = (
                request.form.getlist('produttore_scelti[]')
                or request.form.getlist('produttore_scelti')
                or request.form.getlist('prods[]')
                or request.form.getlist('prods')
            )

            # Validazione: richiede che tutti i campi siano stati compilati
            # prima di procedere con la conferma.  In particolare, è necessario
            # che la quantità sia presente (o esista già nella riga RDO), che un fornitore
            # e un produttore siano stati selezionati, e che sia stata specificata
            # almeno una data prevista.  Se uno di questi requisiti non è soddisfatto,
            # interrompiamo la procedura di conferma e rimandiamo l'utente alla
            # pagina dei riordini con un messaggio di avviso.
            valid_dates = [dt.strip() for dt in (dates_list or []) if dt and dt.strip()]
            existing_qty = rdo.get('quantita')
            # La conferma richiede la presenza di una quantità (o quella esistente),
            # di un fornitore e almeno una data prevista.  Il produttore non è
            # più obbligatorio, poiché viene determinato automaticamente dalle
            # anagrafiche o già presente nella riga RDO.
            if ((quantita_val is None and (existing_qty is None or existing_qty == 0)) or not form_forn or not valid_dates):
                flash('Compila tutti i campi, inclusi fornitore e data prevista, prima di confermare.', 'warning')
                return redirect(url_for('riordini'))

            # Costruisci query di aggiornamento
            upd_fields = []
            upd_params = []
            # Aggiorna la quantità totale dell'ordine se specificata
            if quantita_val is not None:
                upd_fields.append('quantita=?')
                upd_params.append(quantita_val)
            # Aggiorna fornitore scelto e blocca la scelta
            upd_fields.append('fornitore_scelto=?')
            upd_params.append(form_forn or None)
            upd_fields.append('locked_forn=?')
            upd_params.append(1)
            # Aggiorna produttore scelto solo se fornito; in caso contrario
            # mantieni il valore esistente.  Se viene selezionato un
            # produttore, blocca la scelta impostando locked_prod=1.  Se
            # l'utente non fornisce alcun produttore, non modifichiamo
            # produttore_scelto né il flag di blocco.
            if form_prod:
                upd_fields.append('produttore_scelto=?')
                upd_params.append(form_prod or None)
                upd_fields.append('locked_prod=?')
                upd_params.append(1)
            # Prepara la lista di coppie (data, quantita) pulita e coerente con le
            # regole di ripartizione: la prima quantità deve essere almeno 1,
            # le successive non possono superare il residuo per completare
            # l'ordine.  Se sono state fornite quantità negative o non numeriche,
            # vengono considerate come 0 e quindi ignorate.
            clean_pairs = []
            # Determina la quantità totale dell'ordine come riferimento
            total_qty = None
            try:
                if quantita_val is not None:
                    total_qty = int(quantita_val)
                else:
                    total_qty = int(rdo['quantita']) if rdo.get('quantita') is not None else None
            except (TypeError, ValueError):
                total_qty = None
            # Se non è definita la quantità totale (dovrebbe essere impossibile),
            # imponiamo 0 per evitare calcoli
            if total_qty is None:
                total_qty = 0
            # Pulisci e valida coppie, abbinando eventuali produttori.
            if dates_list:
                remaining = total_qty
                for idx, (dt_raw, q_raw) in enumerate(zip(dates_list, qtys_list)):
                    dt = (dt_raw or '').strip()
                    if not dt:
                        continue
                    # Converti quantità
                    try:
                        qty_int = int(str(q_raw).strip())
                    except (TypeError, ValueError):
                        qty_int = 0
                    # Prima data deve avere almeno 1
                    if idx == 0 and qty_int < 1:
                        qty_int = 1
                    # Se la quantità eccede il residuo, limitala
                    if qty_int > remaining:
                        qty_int = remaining
                    # Determina il produttore per questa data.  Se è presente una lista
                    # ``prods_list`` utilizza lo stesso indice.  In caso contrario
                    # usa ``form_prod`` come fallback.  Se anche questo è vuoto,
                    # lascia None (sarà gestito successivamente).
                    prod_val = None
                    try:
                        if prods_list and idx < len(prods_list):
                            prod_val = (prods_list[idx] or '').strip()
                    except Exception:
                        prod_val = None
                    if not prod_val:
                        prod_val = form_prod or None
                    # Aggiorna residuo e aggiungi la terna solo se qty_int > 0
                    if qty_int > 0:
                        clean_pairs.append((dt, qty_int, prod_val))
                        remaining -= qty_int
                    # Se non c'è più residuo, interrompi
                    if remaining <= 0:
                        break
            # Determina se l'utente ha inserito più date oppure una sola.  Se non
            # ci sono date valide (clean_pairs vuoto), lasciamo data_prevista
            # invariata; altrimenti impostiamo data_prevista alla prima data per
            # la visualizzazione se è l'unica, altrimenti la azzeriamo.
            if clean_pairs:
                if len(clean_pairs) == 1:
                    # Singola data: salva come data prevista principale usando la prima terna
                    upd_fields.append('data_prevista=?')
                    # Indice 0: data_prevista, indice 1: quantita, indice 2: produttore
                    upd_params.append(clean_pairs[0][0])
                else:
                    upd_fields.append('data_prevista=?')
                    upd_params.append(None)
            # Esegui l'aggiornamento della riga RDO
            upd_params.append(rdo_id)
            if upd_fields:
                conn.execute(f"UPDATE riordini_rdo SET {', '.join(upd_fields)} WHERE id=?", upd_params)
            # Gestisci la tabella rdo_dates: rimuovi le date esistenti e reinserisci
            try:
                conn.execute("DELETE FROM rdo_dates WHERE rdo_id=?", (rdo_id,))
            except sqlite3.Error:
                pass
            for triple in clean_pairs:
                try:
                    # triple è (data_prevista, quantita, produttore)
                    dt, qty_int, prod_val = triple
                    conn.execute(
                        "INSERT INTO rdo_dates (rdo_id, data_prevista, quantita, produttore) VALUES (?, ?, ?, ?)",
                        (rdo_id, dt, qty_int, prod_val),
                    )
                except sqlite3.Error:
                    pass
            conn.commit()
        except Exception:
            # Se si verifica un errore durante l'aggiornamento, continuiamo senza fermare la conferma
            pass
        # Ricarica la riga RDO aggiornata in modo da utilizzare i valori più recenti
        row = conn.execute("SELECT * FROM riordini_rdo WHERE id=?", (rdo_id,)).fetchone()
        if not row:
            flash('RDO non trovata.', 'warning')
            return redirect(url_for('riordini'))
        rdo = dict(row)
        # Parsea liste di fornitori e produttori
        forn_list = [f.strip() for f in str(rdo.get('fornitori') or '').split(',') if f.strip()]
        prod_list = [p.strip() for p in str(rdo.get('produttori') or '').split(',') if p.strip()]
        # Determina il fornitore e il produttore scelti
        fornitore_scelto = (rdo.get('fornitore_scelto') or '').strip() or (forn_list[0] if forn_list else None)
        produttore_scelto = (rdo.get('produttore_scelto') or '').strip() or (prod_list[0] if prod_list else None)
        # Quantità totale per l'accettazione
        quantita_totale = int(rdo['quantita']) if rdo.get('quantita') is not None else 0
        # Genera nuovo numero ordine
        try:
            numero_ordine = generate_order_code(conn)
        except Exception:
            # Fallback: timestamp random
            numero_ordine = datetime.now().strftime('%Y%m%d%H%M%S')
        # Inserisci/aggiorna ordine_fornitori
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ordine_fornitori (numero_ordine TEXT PRIMARY KEY, fornitori TEXT, fornitore_scelto TEXT, locked INTEGER)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO ordine_fornitori (numero_ordine, fornitori, fornitore_scelto, locked) VALUES (?, ?, ?, 1)",
                (
                    numero_ordine,
                    ','.join(forn_list) if forn_list else None,
                    fornitore_scelto or None,
                ),
            )
            conn.execute(
                "UPDATE ordine_fornitori SET fornitori=?, fornitore_scelto=?, locked=1 WHERE numero_ordine=?",
                (
                    ','.join(forn_list) if forn_list else None,
                    fornitore_scelto or None,
                    numero_ordine,
                ),
            )
        except sqlite3.Error:
            pass
        # Inserisci/aggiorna ordine_produttori
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ordine_produttori (numero_ordine TEXT PRIMARY KEY, produttori TEXT, produttore_scelto TEXT, locked INTEGER)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO ordine_produttori (numero_ordine, produttori, produttore_scelto, locked) VALUES (?, ?, ?, 1)",
                (
                    numero_ordine,
                    ','.join(prod_list) if prod_list else None,
                    produttore_scelto or None,
                ),
            )
            conn.execute(
                "UPDATE ordine_produttori SET produttori=?, produttore_scelto=?, locked=1 WHERE numero_ordine=?",
                (
                    ','.join(prod_list) if prod_list else None,
                    produttore_scelto or None,
                    numero_ordine,
                ),
            )
        except sqlite3.Error:
            pass
        # Inserisci evento nello storico riordini_effettuati
        # Utilizza la data corrente come timestamp dell'ordine, così da riflettere il momento
        # in cui l'ordine viene effettivamente confermato.  In questo modo la riga padre
        # apparirà correttamente nella cronologia con la data di conferma e non con la data
        # di creazione della RDO.
        try:
            now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
            conn.execute(
                "INSERT INTO riordini_effettuati (data, tipo_evento, materiale, tipo, spessore, dimensione_x, dimensione_y, quantita, numero_ordine, fornitore, produttore) "
                "VALUES (?, 'ordine', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now_str,
                    rdo.get('materiale'),
                    rdo.get('tipo'),
                    rdo.get('spessore'),
                    rdo.get('dimensione_x'),
                    rdo.get('dimensione_y'),
                    quantita_totale,
                    numero_ordine,
                    fornitore_scelto,
                    produttore_scelto,
                ),
            )
        except sqlite3.Error:
            pass
        # Sposta la riga in accettazione.  Verifica se ci sono date multiple
        acc_rows_inserted = 0
        try:
            # Recupera eventuali date multiple, includendo il produttore associato se presente
            dates = conn.execute("SELECT data_prevista, quantita, produttore FROM rdo_dates WHERE rdo_id=?", (rdo_id,)).fetchall()
        except sqlite3.Error:
            dates = []
        if dates:
            for drow in dates:
                try:
                    qty = int(drow['quantita']) if drow['quantita'] is not None else 0
                except (TypeError, ValueError):
                    qty = 0
                if qty <= 0:
                    continue
                # Determina il produttore per questa consegna: se specificato nel record rdo_dates usa quello,
                # altrimenti utilizza ``produttore_scelto`` come default.
                prod_for_date = None
                try:
                    prod_for_date = (drow['produttore'] or '').strip()
                except Exception:
                    prod_for_date = ''
                if not prod_for_date:
                    prod_for_date = produttore_scelto
                conn.execute(
                    "INSERT INTO riordini_accettazione (data, materiale, tipo, spessore, dimensione_x, dimensione_y, quantita_totale, quantita_ricevuta, numero_ordine, fornitore, produttore, data_prevista) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
                    (
                        rdo['data'],
                        rdo.get('materiale'),
                        rdo.get('tipo'),
                        rdo.get('spessore'),
                        rdo.get('dimensione_x'),
                        rdo.get('dimensione_y'),
                        qty,
                        numero_ordine,
                        fornitore_scelto,
                        prod_for_date,
                        drow['data_prevista'],
                    ),
                )
                acc_rows_inserted += 1
        else:
            # Inserimento singolo con data prevista se presente
            conn.execute(
                "INSERT INTO riordini_accettazione (data, materiale, tipo, spessore, dimensione_x, dimensione_y, quantita_totale, quantita_ricevuta, numero_ordine, fornitore, produttore, data_prevista) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
                (
                    rdo['data'],
                    rdo.get('materiale'),
                    rdo.get('tipo'),
                    rdo.get('spessore'),
                    rdo.get('dimensione_x'),
                    rdo.get('dimensione_y'),
                    quantita_totale,
                    numero_ordine,
                    fornitore_scelto,
                    produttore_scelto,
                    rdo.get('data_prevista'),
                ),
            )
            acc_rows_inserted = 1
        # Rimuovi RDO e relative date multiple
        try:
            conn.execute("DELETE FROM rdo_dates WHERE rdo_id=?", (rdo_id,))
        except sqlite3.Error:
            pass
        try:
            conn.execute("DELETE FROM riordini_rdo WHERE id=?", (rdo_id,))
        except sqlite3.Error:
            pass
        conn.commit()
    flash(f'RDO confermata: creato ordine {numero_ordine} con {acc_rows_inserted} riga/e di accettazione.', 'success')
    return redirect(url_for('riordini'))

# ---------------------------------------------------------------------------
# Preparazione ordine multiplo

@app.route('/prepara_ordine', methods=['POST'])
def prepara_ordine():
    """Genera una bozza di email per il riordino di più combinazioni.

    L'utente seleziona una o più righe di riordino e sceglie un fornitore.
    Questa vista costruisce l'oggetto e il corpo dell'email utilizzando il
    template definito per l'utente corrente e le righe selezionate.  Viene
    generato un link mailto che apre il client di posta pre‑compilato.
    """
    # Verifica permesso alla pagina riordini
    allowed = session.get('allowed_tabs', [])
    if 'riordini' not in allowed:
        flash('Accesso non autorizzato.', 'danger')
        return redirect(url_for('dashboard'))
    # Raccogli i parametri dal form.  Supporta la selezione multipla dei fornitori e dei produttori.
    # Elenco ID fornitori (stringhe) se presente.  Inoltre gestisce il campo legacy "fornitore_id".
    supplier_ids_raw = request.form.getlist('fornitore_ids')
    supplier_ids: list[int] = []
    if supplier_ids_raw:
        for sid in supplier_ids_raw:
            try:
                supplier_ids.append(int(sid))
            except (TypeError, ValueError):
                continue
    else:
        # Compatibilità retro: se è presente un singolo fornitore_id usa quello
        forn_id_raw = request.form.get('fornitore_id')
        if forn_id_raw:
            try:
                supplier_ids.append(int(forn_id_raw))
            except (TypeError, ValueError):
                supplier_ids = []
    # Raccogli gli ID dei produttori (lista di ID).  Questo campo è opzionale e
    # verrà dedotto se non specificato.
    producer_ids_raw = request.form.getlist('produttore_ids')
    producer_ids: list[int] = []
    for pid in producer_ids_raw:
        try:
            producer_ids.append(int(pid))
        except (TypeError, ValueError):
            continue
    # Raccogli le combinazioni di articolo selezionate dal form
    # Raccogli i campi degli articoli selezionati dal form.  Oltre a
    # materiale/tipo/spessore/dimensioni e quantità, includi anche il
    # produttore che viene inviato come input nascosto dalla pagina riordini.
    materiali = request.form.getlist('materiale')
    tipi = request.form.getlist('tipo')
    spessori = request.form.getlist('spessore')
    dxs = request.form.getlist('dimensione_x')
    dys = request.form.getlist('dimensione_y')
    quantitas = request.form.getlist('quantita')
    produttori = request.form.getlist('produttore')
    items: list[dict] = []
    # Normalizza le lunghezze: Flask getlist restituisce lista vuota se il
    # nome non esiste.  Calcoliamo la massima lunghezza tra tutte le
    # liste per iterare correttamente.
    length = max(len(materiali), len(tipi), len(spessori), len(dxs), len(dys), len(quantitas), len(produttori))
    for i in range(length):
        # Funzione di utilità per recuperare il valore alla posizione i o
        # stringa vuota se la lista è più corta.
        def get_val(lst: list, idx: int) -> str:
            return lst[idx] if idx < len(lst) else ''
        items.append({
            'materiale': get_val(materiali, i),
            'tipo': get_val(tipi, i),
            'spessore': get_val(spessori, i),
            'dimensione_x': get_val(dxs, i),
            'dimensione_y': get_val(dys, i),
            'quantita': get_val(quantitas, i),
            'produttore': get_val(produttori, i),
        })
    # Recupera i fornitori e i produttori selezionati (o dedotti) e il template email dell'utente.
    suppliers: list[dict] = []
    producers: list[dict] = []
    ordine_template = ''
    # Collega al DB per raccogliere dati
    with get_db_connection() as conn:
        # ---------------------------------------------
        # Gestione fornitori
        # ---------------------------------------------
        # Se l'utente ha selezionato uno o più fornitori, recupera questi record dal DB
        if supplier_ids:
            for sid in supplier_ids:
                try:
                    row = conn.execute(
                        f"SELECT id, nome, email FROM {SUPPLIER_TABLE} WHERE id=?",
                        (sid,),
                    ).fetchone()
                    if row:
                        suppliers.append({'id': row['id'], 'nome': row['nome'], 'email': row['email']})
                except sqlite3.Error:
                    continue
        # Se nessun fornitore selezionato, deduci i fornitori dai materiali
        if not suppliers:
            deduced_names: set[str] = set()
            for itm in items:
                mat = (itm.get('materiale') or '').strip()
                tp = (itm.get('tipo') or '').strip()
                sp = (itm.get('spessore') or '').strip()
                dx = (itm.get('dimensione_x') or '').strip()
                dy = (itm.get('dimensione_y') or '').strip()
                if not mat:
                    continue
                try:
                    # Trova i fornitori non vuoti per la combinazione, escludendo gli sfridi
                    for_rows = conn.execute(
                        "SELECT DISTINCT fornitore FROM materiali WHERE materiale=? "
                        "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                        "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                        "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                        "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                        "AND (fornitore IS NOT NULL AND TRIM(fornitore)!='') "
                        "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                        (
                            mat,
                            tp if tp else None, tp,
                            sp if sp else None, sp,
                            dx if dx else None, dx,
                            dy if dy else None, dy,
                        ),
                    ).fetchall()
                    for fr in for_rows:
                        fval = (fr['fornitore'] or '').strip()
                        if fval:
                            deduced_names.add(fval)
                except sqlite3.Error:
                    continue
            # Mappa i nomi dedotti alla tabella fornitori, se esistono
            for name in sorted(deduced_names):
                try:
                    row = conn.execute(
                        f"SELECT id, nome, email FROM {SUPPLIER_TABLE} WHERE nome=? COLLATE NOCASE",
                        (name,),
                    ).fetchone()
                    if row:
                        suppliers.append({'id': row['id'], 'nome': row['nome'], 'email': row['email']})
                    else:
                        # Nome non presente nella tabella: includi come fornitore fittizio senza email
                        suppliers.append({'id': None, 'nome': name, 'email': None})
                except sqlite3.Error:
                    suppliers.append({'id': None, 'nome': name, 'email': None})
        # ---------------------------------------------
        # Gestione produttori
        # ---------------------------------------------
        # In questa versione non consenti la selezione manuale dei produttori nella pagina
        # riordini.  Ogni combinazione include il proprio produttore, inviato come
        # campo nascosto.  Pertanto, costruiamo l'elenco dei produttori
        # univoci direttamente dalle righe selezionate.  Se il nome del
        # produttore non è presente nella tabella ``produttori_vocabolario``
        # lo aggiungiamo con id=None.
        unique_prod_names: list[str] = []
        seen_prods: set[str] = set()
        for itm in items:
            pname = (itm.get('produttore') or '').strip()
            if pname and pname not in seen_prods:
                seen_prods.add(pname)
                unique_prod_names.append(pname)
        for pname in unique_prod_names:
            try:
                row = conn.execute(
                    f"SELECT id, nome FROM {PRODUTTORE_TABLE} WHERE nome=? COLLATE NOCASE",
                    (pname,),
                ).fetchone()
                if row:
                    producers.append({'id': row['id'], 'nome': row['nome']})
                else:
                    producers.append({'id': None, 'nome': pname})
            except sqlite3.Error:
                producers.append({'id': None, 'nome': pname})
        # ---------------------------------------------
        # Recupera il template email per l'utente corrente
        user_id = session.get('user_id')
        if user_id:
            try:
                row = conn.execute(
                    f"SELECT ordine_template FROM {USERS_TABLE} WHERE id=?",
                    (user_id,),
                ).fetchone()
                if row and row['ordine_template']:
                    ordine_template = row['ordine_template']
            except sqlite3.Error:
                ordine_template = ''
    # ----------------------------------------------------
    # Composizione dell'email con i produttori
    # ----------------------------------------------------
    # Oggetto
    date_str = datetime.now().strftime('%d/%m/%Y')
    subject = f"Richiesta riordino – {date_str}"
    # Corpo: includi eventuale template personalizzato dell'utente
    body_lines: list[str] = []
    if ordine_template:
        body_lines.append(ordine_template.strip())
        body_lines.append('')
    body_lines.append('Dettaglio materiali da riordinare:')
    # Pre-calcola i produttori per ciascun articolo da visualizzare accanto
    # alla quantità.  In questa implementazione, se l'articolo ha un
    # produttore specificato (inviato dal form), utilizziamo tale valore.
    # Se il campo produttore è vuoto deduciamo comunque i produttori dal
    # magazzino come fallback.
    item_producers: list[list[str]] = []
    with get_db_connection() as conn_tmp:
        for itm in items:
            pname = (itm.get('produttore') or '').strip()
            # Se il produttore è stato specificato nella combinazione,
            # utilizza direttamente quel valore come lista singola.
            if pname:
                item_producers.append([pname])
                continue
            # Altrimenti deduci i produttori come in precedenza.
            mat = (itm.get('materiale') or '').strip()
            tp_i = (itm.get('tipo') or '').strip()
            sp_i = (itm.get('spessore') or '').strip()
            dx_i = (itm.get('dimensione_x') or '').strip()
            dy_i = (itm.get('dimensione_y') or '').strip()
            prod_names: list[str] = []
            if mat:
                try:
                    pr_rows = conn_tmp.execute(
                        "SELECT DISTINCT produttore FROM materiali WHERE materiale=? "
                        "AND (tipo=? OR (tipo IS NULL AND ?='')) "
                        "AND (spessore=? OR (spessore IS NULL AND ?='')) "
                        "AND (dimensione_x=? OR (dimensione_x IS NULL AND ?='')) "
                        "AND (dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                        "AND (produttore IS NOT NULL AND TRIM(produttore)!='') "
                        "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                        (
                            mat,
                            tp_i if tp_i else None, tp_i,
                            sp_i if sp_i else None, sp_i,
                            dx_i if dx_i else None, dx_i,
                            dy_i if dy_i else None, dy_i,
                        ),
                    ).fetchall()
                    seen_p: set[str] = set()
                    for pr in pr_rows:
                        val = (pr['produttore'] or '').strip()
                        if val and val not in seen_p:
                            seen_p.add(val)
                            prod_names.append(val)
                except sqlite3.Error:
                    prod_names = []
            item_producers.append(prod_names)
    # Costruisci le righe del corpo
    for idx, itm in enumerate(items):
        dx_val = (itm.get('dimensione_x') or '').strip()
        dy_val = (itm.get('dimensione_y') or '').strip()
        dims = ''
        if dx_val or dy_val:
            dims = f" {dx_val}x{dy_val}"
        tipo_val = (itm.get('tipo') or '').strip()
        sp_val = (itm.get('spessore') or '').strip()
        desc_parts = [itm.get('materiale') or '']
        if tipo_val:
            desc_parts.append(tipo_val)
        if sp_val:
            desc_parts.append(sp_val)
        descr = ' '.join([p for p in desc_parts if p])
        qty_val = itm.get('quantita')
        prod_names = item_producers[idx] if idx < len(item_producers) else []
        if prod_names:
            prod_str = '/'.join(prod_names)
            body_lines.append(f"- {descr}{dims}: {qty_val} (Produttore: {prod_str})")
        else:
            body_lines.append(f"- {descr}{dims}: {qty_val}")
    body = '\n'.join(body_lines)
    # ----------------------------------------------------
    # Prepara link mailto utilizzando gli indirizzi email dei fornitori
    mailto_link = None
    email_list = [s['email'] for s in suppliers if s and s.get('email')]
    if email_list:
        try:
            to_field = ','.join(email_list)
            mailto_link = f"mailto:{to_field}?subject={quote(subject)}&body={quote(body)}"
        except Exception:
            mailto_link = None
    # Rendi la pagina con i produttori
    return render_template(
        'prepara_ordine.html',
        items=items,
        suppliers=suppliers,
        producers=producers,
        subject=subject,
        body=body,
        mailto_link=mailto_link
    )

# ---------------------------------------------------------------------------
# Riepilogo preferiti

@app.route('/riordini_preferiti')
def riordini_preferiti():
    """Pagina che mostra gli articoli preferiti che necessitano riordino.

    A differenza della versione precedente, questa vista non elenca tutte le
    combinazioni inserite nell'anagrafica articoli, ma filtra solo quelle
    contrassegnate come "preferite" (campo ``preferito`` = 1 nella tabella
    ``articoli_catalogo``) e per le quali la quantità aggregata in
    magazzino è inferiore o uguale alla soglia di riordino definita.
    Le soglie vengono lette dalla tabella ``riordino_soglie`` al pari della
    pagina principale dei riordini.  Le dimensioni X e Y sono utilizzate
    esclusivamente come chiave per aggregare le quantità, ma le soglie
    vengono calcolate sulla terna materiale/tipo/spessore.
    """
    # Verifica permessi: solo gli utenti con il tab "riordini" possono accedere.
    allowed = session.get('allowed_tabs', [])
    if 'riordini' not in allowed:
        flash('Accesso non autorizzato ai riordini.', 'danger')
        return redirect(url_for('dashboard'))
    combos: list[dict] = []
    with get_db_connection() as conn:
        # Recupera le combinazioni preferite dall'anagrafica articoli
        try:
            fav_rows = conn.execute(
                "SELECT materiale, tipo, spessore, dimensione_x, dimensione_y "
                "FROM articoli_catalogo WHERE preferito=1"
            ).fetchall()
        except sqlite3.Error:
            fav_rows = []
        if fav_rows:
            # Costruisci una mappa soglie per materiale/tipo/spessore
            threshold_map: dict[tuple[str, str, str], int] = {}
            try:
                th_rows = conn.execute(
                    f"SELECT materiale, tipo, spessore, threshold FROM {RIORDINO_SOGGIE_TABLE}"
                ).fetchall()
                for tr in th_rows:
                    key = (tr['materiale'], tr['tipo'] or '', tr['spessore'] or '')
                    try:
                        threshold_map[key] = int(tr['threshold'])
                    except (ValueError, TypeError):
                        threshold_map[key] = DEFAULT_REORDER_THRESHOLD
            except sqlite3.Error:
                threshold_map = {}
            # Per ogni combinazione preferita calcola la quantità totale disponibile
            for fav in fav_rows:
                mat = fav['materiale']
                tp = fav['tipo'] or ''
                sp = fav['spessore'] or ''
                dx = (fav['dimensione_x'] or '').strip()
                dy = (fav['dimensione_y'] or '').strip()
                # Aggrega la quantità di tutte le righe (pallet radice e lastre figlie) con
                # dimensioni esattamente uguali alla combinazione.  Non filtriamo per parent_id
                # per replicare il comportamento dell'anagrafica articoli.
                try:
                    qty_res = conn.execute(
                        "SELECT SUM(quantita) as tot FROM materiali WHERE materiale=? "
                        "AND (tipo=? OR (tipo IS NULL AND ?='')) AND "
                        "(spessore=? OR (spessore IS NULL AND ?='')) AND "
                        "(dimensione_x=? OR (dimensione_x IS NULL AND ?='')) AND "
                        "(dimensione_y=? OR (dimensione_y IS NULL AND ?='')) "
                        "AND (is_sfrido IS NULL OR is_sfrido != 1)",
                        (
                            mat,
                            tp if tp else None, tp,
                            sp if sp else None, sp,
                            dx if dx else None, dx,
                            dy if dy else None, dy,
                        ),
                    ).fetchone()
                    total_qty = int(qty_res['tot']) if qty_res and qty_res['tot'] is not None else 0
                except sqlite3.Error:
                    total_qty = 0
                # Determina la soglia di riordino per la terna
                th_val = threshold_map.get((mat, tp, sp), DEFAULT_REORDER_THRESHOLD)
                # Se la soglia è zero la combinazione non va considerata
                try:
                    if int(th_val) == 0:
                        continue
                except Exception:
                    pass
                # Includi la combinazione solo se la quantità corrente è <= soglia
                if total_qty <= th_val:
                    combos.append({
                        'materiale': mat,
                        'tipo': tp,
                        'spessore': sp,
                        'dimensione_x': dx,
                        'dimensione_y': dy,
                        'quantita_totale': total_qty,
                        'soglia': th_val,
                    })
        # Fine with block
    # Ordina le combinazioni preferite per materiale, tipo, spessore, dimensioni
    combos.sort(key=lambda c: (c['materiale'], c['tipo'], c['spessore'], c['dimensione_x'], c['dimensione_y']))
    return render_template(
        'riordini_preferiti.html',
        title='Articoli preferiti',
        combos=combos,
    )

# ---------------------------------------------------------------------------
# Gestione Magazzino Live

@app.route('/live')
def live():
    """Pagina che mostra tutte le prenotazioni attive con stato e countdown.

    Oltre a calcolare un colore proporzionale alla distanza dalla
    scadenza (come in precedenza), questa versione calcola un
    semaforo (verde, giallo, rosso) per indicare a colpo d'occhio
    l'urgenza della prenotazione.  Le righe includono anche il nome
    della macchina, se presente, associata alla prenotazione.
    """
    with get_db_connection() as conn:
        # Recupera anche il flag "is_generic" dalla tabella prenotazioni per sapere se la prenotazione è generica o specifica.
        rows = conn.execute(
            f"SELECT p.id AS pren_id, p.due_time, p.created_at, p.macchina_id, p.is_generic, m.*"
            f" FROM {PRENOTAZIONI_TABLE} p JOIN materiali m ON p.material_id = m.id"
            f" ORDER BY p.due_time"
        ).fetchall()
    # Precarica dizionario delle macchine (id -> nome) per associare
    # rapidamente i nomi alle prenotazioni.  Se non ci sono macchine,
    # la lista sarà vuota.
    macchina_dict = {item['id']: item['nome'] for item in get_macchine_vocabolario()}
    reservations: list[dict] = []
    now = datetime.now()
    total_window = 24 * 3600  # 24 ore
    for row in rows:
        try:
            due = datetime.fromisoformat(row['due_time'])
        except Exception:
            due = now
        diff_seconds = (due - now).total_seconds()
        # Calcola rapporto normalizzato: 0 (lontano) -> 1 (scaduto)
        if diff_seconds >= total_window:
            ratio = 0.0
        elif diff_seconds <= 0:
            ratio = 1.0
        else:
            ratio = 1.0 - (diff_seconds / total_window)
        # Colore di sfondo (legacy) per compatibilità: da bianco a rosso
        r_val = int(255 * ratio)
        g_val = int(255 * (1 - ratio))
        b_val = int(255 * (1 - ratio))
        color = f"rgb({r_val},{g_val},{b_val})"
        # Determina lo stato del semaforo
        if diff_seconds <= 0:
            status = 'red'
        else:
            # Usa ratio per decidere la fascia
            if ratio < 0.33:
                status = 'green'
            elif ratio < 0.66:
                status = 'yellow'
            else:
                status = 'red'
        res_dict = dict(row)
        res_dict['color'] = color
        res_dict['due'] = due
        res_dict['diff_seconds'] = diff_seconds
        # Nome macchina se presente
        macchina_id_val = row['macchina_id'] if 'macchina_id' in row.keys() else None
        res_dict['macchina_nome'] = macchina_dict.get(macchina_id_val) if macchina_id_val else None
        res_dict['status'] = status
        # Calcola l'ID del bancale associato alla prenotazione.
        # Se il materiale stesso è un bancale (is_pallet==1) l'ID coincide con il materiale,
        # altrimenti se è una lastra figlia (parent_id non nullo) l'ID del bancale è parent_id.
        # In caso di lastra indipendente (no parent_id e is_pallet==0) non esiste un bancale.
        try:
            is_pallet_flag = int(res_dict.get('is_pallet') or 0)
        except Exception:
            is_pallet_flag = 0
        parent_val = res_dict.get('parent_id')
        if is_pallet_flag:
            res_dict['bancale_id'] = res_dict.get('id')
        elif parent_val:
            # parent_val può essere None oppure un intero; se definito lo usiamo
            try:
                res_dict['bancale_id'] = int(parent_val)
            except Exception:
                res_dict['bancale_id'] = None
        else:
            res_dict['bancale_id'] = None
        reservations.append(res_dict)
    # Calcola eventuali materiali in attesa di accettazione come nella pagina riordini
    accettazioni: list[dict] = []
    try:
        with get_db_connection() as conn2:
            acc_rows = conn2.execute("SELECT * FROM riordini_accettazione ORDER BY datetime(data) DESC").fetchall()
            # Precarica una mappa per ricostruire dimensioni mancanti a partire dal catalogo articoli
            catalog_map: dict[tuple[str, str, str], tuple[str, str]] = {}
            try:
                cat_rows = conn2.execute(
                    "SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy FROM articoli_catalogo"
                ).fetchall()
                for cr in cat_rows:
                    key = (cr['materiale'], cr['tipo'] or '', cr['spessore'] or '')
                    if key not in catalog_map:
                        catalog_map[key] = ((cr['dx'] or '').strip(), (cr['dy'] or '').strip())
            except sqlite3.Error:
                catalog_map = {}
            # Precarica le mappe dei fornitori e dei produttori per velocizzare l'accesso
            forn_map: dict[str, dict] = {}
            prod_map: dict[str, dict] = {}
            try:
                forrows = conn2.execute(
                    "SELECT numero_ordine, fornitori, fornitore_scelto, locked FROM ordine_fornitori"
                ).fetchall()
                for fr in forrows:
                    num = fr['numero_ordine']
                    if num:
                        forn_map[str(num)] = dict(fr)
            except sqlite3.Error:
                forn_map = {}
            try:
                prodrows = conn2.execute(
                    "SELECT numero_ordine, produttori, produttore_scelto, locked FROM ordine_produttori"
                ).fetchall()
                for pr in prodrows:
                    nump = pr['numero_ordine']
                    if nump:
                        prod_map[str(nump)] = dict(pr)
            except sqlite3.Error:
                prod_map = {}
            for ar in acc_rows:
                row_dict = dict(ar)
                dx = (row_dict.get('dimensione_x') or '').strip()
                dy = (row_dict.get('dimensione_y') or '').strip()
                if (not dx or not dy) and row_dict.get('materiale'):
                    key = (
                        row_dict.get('materiale'),
                        (row_dict.get('tipo') or ''),
                        (row_dict.get('spessore') or '')
                    )
                    if key in catalog_map:
                        cdx, cdy = catalog_map[key]
                        if not dx:
                            dx = cdx
                        if not dy:
                            dy = cdy
                        row_dict['dimensione_x'] = dx
                        row_dict['dimensione_y'] = dy
                # Calcola il residuo, lo stato e la percentuale di avanzamento
                try:
                    qt = int(row_dict.get('quantita_totale') or 0)
                except (ValueError, TypeError):
                    qt = 0
                try:
                    qr = int(row_dict.get('quantita_ricevuta') or 0)
                except (ValueError, TypeError):
                    qr = 0
                residuo = qt - qr
                if residuo < 0:
                    residuo = 0
                row_dict['residuo'] = residuo
                row_dict['stato'] = 'Completo' if residuo == 0 else 'Parziale'
                progress_pct = 0
                try:
                    if qt and qt > 0:
                        progress_pct = int((qr * 100) / qt)
                except Exception:
                    progress_pct = 0
                if progress_pct < 0:
                    progress_pct = 0
                if progress_pct > 100:
                    progress_pct = 100
                row_dict['progress_pct'] = progress_pct
                # Assegna fornitori e fornitori scelti
                numero_ordine_val = str(row_dict.get('numero_ordine') or '')
                if numero_ordine_val and numero_ordine_val in forn_map:
                    forn_entry = forn_map[numero_ordine_val]
                    fornitori_str = (forn_entry.get('fornitori') or '')
                    if fornitori_str:
                        row_dict['fornitori'] = [fn.strip() for fn in fornitori_str.split(',') if fn.strip()]
                    else:
                        row_dict['fornitori'] = []
                    row_dict['fornitore_scelto'] = forn_entry.get('fornitore_scelto')
                    try:
                        row_dict['forn_locked'] = bool(int(forn_entry.get('locked', 0)))
                    except Exception:
                        row_dict['forn_locked'] = False
                else:
                    row_dict['fornitori'] = []
                    row_dict['fornitore_scelto'] = None
                    row_dict['forn_locked'] = False
                # Assegna produttori e produttori scelti
                if numero_ordine_val and numero_ordine_val in prod_map:
                    prod_entry = prod_map[numero_ordine_val]
                    prod_str = (prod_entry.get('produttori') or '')
                    if prod_str:
                        row_dict['produttori'] = [pd.strip() for pd in prod_str.split(',') if pd.strip()]
                    else:
                        row_dict['produttori'] = []
                    row_dict['produttore_scelto'] = prod_entry.get('produttore_scelto')
                    try:
                        row_dict['prod_locked'] = bool(int(prod_entry.get('locked', 0)))
                    except Exception:
                        row_dict['prod_locked'] = False
                else:
                    row_dict['produttori'] = []
                    row_dict['produttore_scelto'] = None
                    row_dict['prod_locked'] = False
                accettazioni.append(row_dict)
    except Exception:
        accettazioni = []
    return render_template('live.html', title='Magazzino Live', reservations=reservations, accettazioni=accettazioni)


@app.route('/prenota/<int:material_id>', methods=['GET', 'POST'])
def prenota(material_id: int):
    """Permette all'utente di prenotare una lastra specificando un orario di scadenza.

    In GET viene mostrato un modulo per scegliere data e ora (input
    ``datetime-local``). In POST viene inserita la prenotazione nel
    database e l'utente viene reindirizzato alla pagina delle prenotazioni.
    """
    # Recupera materiale per visualizzare informazioni nella pagina
    with get_db_connection() as conn:
        materiale = conn.execute(
            "SELECT * FROM materiali WHERE id=?", (material_id,)
        ).fetchone()
        if not materiale:
            flash('Materiale non trovato per prenotazione.', 'danger')
            return redirect(url_for('dashboard'))
    if request.method == 'POST':
        # Verifica se la lastra è già prenotata.  Se esiste una prenotazione attiva per
        # questo materiale non consentiamo la duplicazione e mostriamo un messaggio
        # di errore all'utente.  La funzione ``get_reserved_material_ids`` restituisce
        # l'insieme di tutti gli ID prenotati in modo efficiente.
        try:
            reserved_ids = get_reserved_material_ids()
        except Exception:
            reserved_ids = set()
        if material_id in reserved_ids:
            flash('Questa lastra è già prenotata. Cancella la prenotazione corrente per prenotarla di nuovo.', 'warning')
            return redirect(url_for('dashboard'))

        due_str = request.form.get('due_time', '').strip()
        if not due_str:
            flash('È necessario specificare la data e ora di scadenza.', 'danger')
            return redirect(url_for('prenota', material_id=material_id))
        try:
            # ``datetime-local`` restituisce stringa in formato YYYY-MM-DDTHH:MM
            due_dt = datetime.fromisoformat(due_str)
        except ValueError:
            flash('Formato data/ora non valido.', 'danger')
            return redirect(url_for('prenota', material_id=material_id))
        # Leggi macchina selezionata, se presente
        macchina_id_raw = request.form.get('macchina_id', '').strip()
        macchina_id_val = None
        if macchina_id_raw:
            try:
                macchina_id_val = int(macchina_id_raw)
            except ValueError:
                macchina_id_val = None
        # Determina il tipo di prenotazione (ID specifico o generico).  Per le lastre
        # indipendenti o i bancali il campo non è presente e il valore predefinito
        # rimane "specific".  L'opzione ``generic`` consente di evadere la prenotazione
        # prelevando qualsiasi lastra del bancale, mentre ``specific`` richiede
        # l'identificativo esatto.  Convertiamo in un intero (0 o 1) da
        # memorizzare nel database.
        id_mode_raw = request.form.get('id_mode', 'specific').strip().lower()
        is_generic_val = 1 if id_mode_raw == 'generic' else 0
        # Inserisci prenotazione includendo la macchina e il flag generico
        created_str = datetime.now().isoformat(timespec='seconds')
        with get_db_connection() as conn:
            conn.execute(
                f"INSERT INTO {PRENOTAZIONI_TABLE} (material_id, due_time, created_at, macchina_id, is_generic) VALUES (?,?,?,?,?)",
                (material_id, due_dt.isoformat(timespec='seconds'), created_str, macchina_id_val, is_generic_val)
            )
            # Commit esplicito prima della registrazione nello storico.
            try:
                conn.commit()
            except Exception:
                pass
        # Registra l'evento di prenotazione nello storico per la lastra
        try:
            # Convert the sqlite3.Row to a dict so we can use .get safely
            try:
                matd = dict(materiale)
            except Exception:
                matd = {k: materiale[k] for k in materiale.keys()} if materiale else {}
            log_slab_events([
                {
                    'slab_id': material_id,
                    'event_type': 'prenotato',
                    'from_letter': matd.get('ubicazione_lettera'),
                    'from_number': matd.get('ubicazione_numero'),
                    'to_letter': matd.get('ubicazione_lettera'),
                    'to_number': matd.get('ubicazione_numero'),
                    'dimensione_x': matd.get('dimensione_x'),
                    'dimensione_y': matd.get('dimensione_y'),
                    'spessore': matd.get('spessore'),
                    'materiale': matd.get('materiale'),
                    'tipo': matd.get('tipo'),
                    'fornitore': matd.get('fornitore'),
                    'produttore': matd.get('produttore'),
                    'note': matd.get('note'),
                    'nesting_link': None,
                }
            ])
        except Exception:
            pass
        flash('Prenotazione registrata con successo!', 'success')
        return redirect(url_for('live'))
    # GET
    # Carica elenco macchine per la selezione (può essere vuoto)
    macchine = get_macchine_vocabolario()
    return render_template('prenota.html', title='Prenota materiale', materiale=materiale, macchine=macchine)


@app.route('/delete_prenotazione/<int:pren_id>', methods=['POST'])
def delete_prenotazione(pren_id: int):
    """Elimina una prenotazione dall'elenco (utilizzato nella pagina Live) e registra lo storico."""
    # Recupera l'ID della lastra associata e i suoi dettagli prima di cancellare la prenotazione
    material_to_log: int | None = None
    materiale_row = None
    try:
        with get_db_connection() as conn:
            row = conn.execute(
                f"SELECT material_id FROM {PRENOTAZIONI_TABLE} WHERE id=?",
                (pren_id,)
            ).fetchone()
            if row:
                material_to_log = row['material_id']
                materiale_row = conn.execute(
                    "SELECT * FROM materiali WHERE id=?",
                    (material_to_log,)
                ).fetchone()
            # Cancella la prenotazione
            conn.execute(
                f"DELETE FROM {PRENOTAZIONI_TABLE} WHERE id=?",
                (pren_id,)
            )
            # Commit immediato così la riga eliminata è visibile alla connessione dello storico.
            try:
                conn.commit()
            except Exception:
                pass
    except Exception:
        # ignora eventuali errori di lettura e cancellazione
        pass
    # Registra l'evento di cancellazione della prenotazione se sono disponibili i dati della lastra
    if material_to_log and materiale_row:
        try:
            matd = dict(materiale_row)
        except Exception:
            matd = {}
        try:
            log_slab_events([
                {
                    'slab_id': material_to_log,
                    'event_type': 'prenotazione_cancellata',
                    'from_letter': matd.get('ubicazione_lettera'),
                    'from_number': matd.get('ubicazione_numero'),
                    'to_letter': matd.get('ubicazione_lettera'),
                    'to_number': matd.get('ubicazione_numero'),
                    'dimensione_x': matd.get('dimensione_x'),
                    'dimensione_y': matd.get('dimensione_y'),
                    'spessore': matd.get('spessore'),
                    'materiale': matd.get('materiale'),
                    'tipo': matd.get('tipo'),
                    'fornitore': matd.get('fornitore'),
                    'produttore': matd.get('produttore'),
                    'note': matd.get('note'),
                    'nesting_link': None,
                }
            ])
        except Exception:
            pass
    flash('Prenotazione rimossa.', 'success')
    return redirect(url_for('live'))


# ---------------------------------------------------------------------------
# Gestione documenti associati ai materiali

@app.route('/upload_docs/<int:material_id>', methods=['POST'])
def upload_docs(material_id: int):
    """Gestisce il caricamento di documenti per un materiale o per un bancale.

    A differenza dell'implementazione originale, i documenti vengono salvati
    in una sottocartella di ``uploads`` il cui nome è basato sulla data
    corrente (formato ``aaaammgg``) seguita dall'ID della lastra.  Se il
    materiale selezionato è un bancale (``is_pallet=1``), il documento viene
    copiato in ogni lastra figlia e, per comodità, anche sul bancale stesso.
    Ogni copia viene rinominata con un identificativo univoco (UUID) per
    evitare conflitti.  Per ciascuna copia viene registrato un record
    nella tabella ``documenti`` con ``material_id`` impostato all'ID della
    lastra corrispondente.
    """
    # Raccogli la lista di file e la pagina di ritorno (next) dall'input del form
    files = request.files.getlist('documents')
    # La pagina a cui tornare dopo il caricamento. Se il form contiene un campo
    # nascosto 'next', usiamo quello; in caso contrario useremo il referer.
    next_page = request.form.get('next') or request.args.get('next')
    if not files:
        flash('Nessun file selezionato per il caricamento.', 'warning')
        # Resta sulla pagina indicata o sul referer se disponibile
        if next_page:
            return redirect(next_page)
        return redirect(request.referrer or url_for('dashboard'))

    # Determina l'elenco di ID destinatari: se si tratta di un bancale,
    # includi sia il bancale che tutte le lastre figlie; altrimenti solo
    # l'ID specificato.
    id_list: list[int] = []
    try:
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT id, is_pallet FROM materiali WHERE id=?", (material_id,)
            ).fetchone()
            if row:
                id_list.append(int(row['id']))
                try:
                    is_pallet_flag = int(row['is_pallet'] or 0)
                except Exception:
                    is_pallet_flag = 0
                if is_pallet_flag == 1:
                    # Recupera le lastre figlie del bancale
                    children = conn.execute(
                        "SELECT id FROM materiali WHERE parent_id=?",
                        (material_id,)
                    ).fetchall()
                    for ch in children:
                        try:
                            ch_id = int(ch['id']) if isinstance(ch, sqlite3.Row) else int(ch[0])
                            id_list.append(ch_id)
                        except Exception:
                            continue
    except Exception:
        # Se la query fallisce, procederemo con il solo ID fornito
        if not id_list:
            id_list = [material_id]
    # Elimina eventuali duplicati preservando l'ordine
    seen_ids = set()
    id_list_unique: list[int] = []
    for iid in id_list:
        if iid not in seen_ids:
            id_list_unique.append(iid)
            seen_ids.add(iid)
    id_list = id_list_unique
    # Salvataggio documenti riveduto: utilizziamo la struttura a cartelle
    # Documenti_pallet/Documenti_materiale e replichiamo sui figli se necessario.
    saved_any = False
    # Stabilisce il tipo di documento: se il materiale è un bancale (is_pallet_flag==1)
    # i documenti vanno replicati su tutte le lastre figlie con doc_type='pallet',
    # altrimenti vengono salvati come documenti specifici della singola lastra.
    doc_type = 'pallet' if is_pallet_flag == 1 else 'materiale'
    with get_db_connection() as conn:
        for f in files:
            if not f or f.filename == '':
                continue
            orig_name = f.filename
            # Estensione consentita
            if not allowed_file(orig_name):
                flash(f"Estensione non permessa per {orig_name}.", 'danger')
                continue
            # Leggi l'intero contenuto del file
            try:
                content_bytes = f.read()
            except Exception:
                flash(f"Errore nel salvataggio di {orig_name}.", 'danger')
                continue
            # Estrai l'estensione originale e normalizza in minuscolo
            _, ext = os.path.splitext(orig_name)
            ext = ext.lower()
            # Salva su ogni ID nella lista (padre + eventuali figli)
            for target_id in id_list:
                try:
                    rel_dest = save_file_to_id(content_bytes, ext, target_id, doc_type=doc_type)
                    conn.execute(
                        "INSERT INTO documenti (material_id, filename, original_name) VALUES (?, ?, ?)",
                        (target_id, rel_dest, orig_name)
                    )
                    saved_any = True
                except Exception:
                    # Ignora errori di salvataggio/inserimento e continua
                    continue
        if saved_any:
            try:
                conn.commit()
            except Exception:
                pass
    if saved_any:
        flash('Documento/i caricati con successo.', 'success')
    # Dopo il caricamento, torna alla pagina indicata nel campo 'next' se presente,
    # altrimenti usa il referer. In mancanza di entrambi, rimanda alla dashboard.
    if next_page:
        return redirect(next_page)
    referrer = request.referrer
    if referrer:
        return redirect(referrer)
    return redirect(url_for('dashboard'))


@app.route('/download_doc/<int:doc_id>')
def download_doc(doc_id: int):
    """Consente di scaricare un documento associato a un materiale.

    Recupera le informazioni del documento dal database e utilizza
    ``send_file`` per restituire il file al client.  In caso di
    assenza del documento viene visualizzato un messaggio di errore.
    """
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT filename, original_name FROM documenti WHERE id=?",
            (doc_id,)
        ).fetchone()
    if not row:
        flash('Documento non trovato.', 'danger')
        return redirect(url_for('dashboard'))
    file_path = os.path.join(UPLOAD_FOLDER, row['filename'])
    if not os.path.exists(file_path):
        flash('File non presente sul server.', 'danger')
        return redirect(url_for('dashboard'))
    return send_file(file_path, as_attachment=True, download_name=row['original_name'])


@app.route('/delete_doc/<int:doc_id>', methods=['POST'])
def delete_doc(doc_id: int):
    """Elimina un documento sia dal filesystem che dal database.

    Questa operazione richiede una richiesta POST per evitare cancellazioni
    accidentali via GET.  Se il file esiste, viene rimosso dal
    filesystem; successivamente il record viene eliminato dalla tabella
    ``documenti``.  Al termine l'utente viene reindirizzato alla
    dashboard.
    """
    # Rimuove un documento associato a un materiale.
    # Per consentire all'utente di continuare a gestire i documenti senza
    # essere reindirizzato alla dashboard, reindirizziamo sempre alla
    # pagina che ha effettuato la richiesta (request.referrer), se
    # disponibile.  Solo in assenza di referer torniamo alla dashboard.
    # Determina la pagina a cui tornare dopo la cancellazione. Si cerca innanzitutto
    # un parametro ``next`` nel corpo della richiesta o nella query string.
    next_page = request.form.get('next') or request.args.get('next')
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT filename FROM documenti WHERE id=?",
            (doc_id,)
        ).fetchone()
        if not row:
            flash('Documento non trovato.', 'danger')
            # Torna alla pagina indicata se presente o al referer
            if next_page:
                return redirect(next_page)
            return redirect(request.referrer or url_for('dashboard'))
        file_path = os.path.join(UPLOAD_FOLDER, row['filename'])
        try:
            # Se il file esiste rimuovilo dal filesystem
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            # Ignora eventuali errori nella cancellazione del file per non interrompere la logica
            pass
        # Rimuovi la riga dal database
        conn.execute("DELETE FROM documenti WHERE id=?", (doc_id,))
        conn.commit()
    flash('Documento eliminato.', 'success')
    # Dopo la cancellazione, torna alla pagina specificata nel parametro 'next' se presente,
    # oppure al referer. Come ultima risorsa, torna alla dashboard.
    if next_page:
        return redirect(next_page)
    return redirect(request.referrer or url_for('dashboard'))


# ---------------------------------------------------------------------------
# Visualizzazione documenti per un materiale

@app.route('/docs/<int:material_id>')
@app.route('/view_docs/<int:material_id>')
def view_docs(material_id: int):
    """
    Mostra l'elenco dei documenti associati a un materiale in una pagina dedicata.

    Utilizza la funzione ``get_attachments`` per recuperare tutti i
    documenti legati a ``material_id``.  Per ogni documento viene
    visualizzato il nome originale come link per il download tramite
    l'endpoint ``download_doc``.  Se non esistono documenti per
    l'ID specificato viene mostrato un messaggio informativo.
    """
    try:
        attachments = get_attachments(material_id)
    except Exception:
        attachments = []
    return render_template('view_docs.html', material_id=material_id, docs=attachments)


@app.route('/download_qr_pdf')
def download_qr_pdf():
    """Genera un archivio ZIP contenente un'immagine PNG per ciascun QR specificato.

    L'elenco degli ID deve essere passato tramite la query string
    separandoli con la virgola (es. ``/download_qr_pdf?ids=1,2,3``). Per
    ogni ID viene generata un'immagine A4 (72 DPI) suddivisa in due: la
    metà superiore visualizza il QR code centrato, mentre la metà inferiore
    contiene soltanto il codice ID del materiale in formato molto grande.
    Tutte le immagini PNG vengono poi compresse in un file ZIP che
    l'utente può scaricare.
    """
    ids_param = request.args.get('ids', '')
    if not ids_param:
        flash('Nessun QR da esportare in PDF.', 'warning')
        return redirect(url_for('dashboard'))
    # Filtra solo numeri interi validi
    try:
        ids = [int(x) for x in ids_param.split(',') if x.strip().isdigit()]
    except Exception:
        ids = []
    if not ids:
        flash('ID non validi per il PDF.', 'warning')
        return redirect(url_for('dashboard'))
    # Recupera dati dal database
    with get_db_connection() as conn:
        placeholders = ','.join(['?'] * len(ids))
        query = f"SELECT id, ubicazione_lettera, ubicazione_numero FROM materiali WHERE id IN ({placeholders})"
        rows = conn.execute(query, ids).fetchall()
    if not rows:
        flash('Nessun materiale trovato per i QR specificati.', 'warning')
        return redirect(url_for('dashboard'))
    pages: list[Image.Image] = []
    # Dimensioni della pagina A4 a 72 DPI
    page_width, page_height = 595, 842
    half_height = page_height // 2
    margin = 20  # Margine generale per il contenuto
    # Genera una pagina per ciascun materiale, identica alla versione singola,
    # e aggiungila alla lista delle pagine.
    for row in rows:
        page = Image.new('RGB', (page_width, page_height), 'white')
        draw_page = ImageDraw.Draw(page)
        # Genera il codice QR e ridimensionalo per occupare la metà superiore
        qr = qrcode.QRCode(box_size=10, border=4)
        qr.add_data(str(row['id']))
        qr_img = qr.make_image(fill_color='black', back_color='white').convert('RGB')
        qr_max_w = page_width - 2 * margin
        qr_max_h = half_height - 2 * margin
        orig_w, orig_h = qr_img.size
        scale_factor = min(qr_max_w / orig_w, qr_max_h / orig_h)
        scaled_w = int(orig_w * scale_factor)
        scaled_h = int(orig_h * scale_factor)
        qr_scaled = qr_img.resize((scaled_w, scaled_h))
        qr_x = (page_width - scaled_w) // 2
        qr_y = margin + (half_height - 2 * margin - scaled_h) // 2
        page.paste(qr_scaled, (qr_x, qr_y))
        # Testo del codice ID nella metà inferiore
        id_text = str(row['id'])
        id_max_w = page_width - 2 * margin
        id_max_h = half_height - margin
        font_size = 600
        id_font = None
        id_text_w = id_text_h = 0
        while font_size >= 10:
            try:
                candidate = ImageFont.truetype('DejaVuSans-Bold.ttf', font_size)
            except Exception:
                try:
                    candidate = ImageFont.truetype('DejaVuSans.ttf', font_size)
                except Exception:
                    candidate = ImageFont.load_default()
            tw, th = draw_page.textsize(id_text, font=candidate)
            if tw <= id_max_w and th <= id_max_h:
                id_font = candidate
                id_text_w, id_text_h = tw, th
                break
            font_size -= 2
        if not id_font:
            try:
                id_font = ImageFont.truetype('DejaVuSans-Bold.ttf', 12)
            except Exception:
                try:
                    id_font = ImageFont.truetype('DejaVuSans.ttf', 12)
                except Exception:
                    id_font = ImageFont.load_default()
            id_text_w, id_text_h = draw_page.textsize(id_text, font=id_font)
        id_x = (page_width - id_text_w) // 2
        id_y = half_height + margin
        draw_page.text((id_x, id_y), id_text, fill='black', font=id_font)
        pages.append(page)
    # Se c'è almeno una pagina, creiamo un PDF multipagina salvando la prima
    # immagine e appendendo le restanti.  Utilizziamo un buffer in memoria.
    pdf_buffer = BytesIO()
    if pages:
        first_page, *rest = pages
        try:
            first_page.save(pdf_buffer, format='PDF', save_all=True, append_images=rest)
        except Exception:
            # Fallback: salva solo la prima pagina se la generazione multipagina fallisce
            first_page.save(pdf_buffer, format='PDF')
    pdf_buffer.seek(0)
    return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name='qr_codes.pdf')


@app.route('/print_qr/<int:material_id>')
def print_qr(material_id: int):
    """Genera e restituisce un PDF contenente un'unica etichetta QR per il materiale.

    Il PDF è composto direttamente in memoria senza ricorrere a un
    template HTML. La pagina A4 viene suddivisa a metà: nella parte
    superiore è centrato il QR code; nella parte inferiore è visualizzato
    esclusivamente il codice ID del materiale in modo molto grande.
    """
    # Recupera le informazioni del materiale dal database
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT id, ubicazione_lettera, ubicazione_numero FROM materiali WHERE id=?",
            (material_id,)
        ).fetchone()
    if not row:
        flash('Materiale non trovato per la stampa del QR.', 'danger')
        return redirect(url_for('dashboard'))
    # Imposta dimensioni della pagina (A4 a 72 DPI)
    page_width, page_height = 595, 842
    half_height = page_height // 2
    margin = 20
    # Genera il codice QR come immagine RGB
    qr = qrcode.QRCode(box_size=10, border=4)
    qr.add_data(str(row['id']))
    qr_img = qr.make_image(fill_color='black', back_color='white').convert('RGB')
    # Calcola dimensioni massime per il QR nella metà superiore
    qr_max_w = page_width - 2 * margin
    qr_max_h = half_height - 2 * margin
    q_w, q_h = qr_img.size
    scale = min(qr_max_w / q_w, qr_max_h / q_h)
    new_w = int(q_w * scale)
    new_h = int(q_h * scale)
    qr_scaled = qr_img.resize((new_w, new_h))
    # Crea una pagina bianca
    page = Image.new('RGB', (page_width, page_height), 'white')
    draw_page = ImageDraw.Draw(page)
    # Posizione del QR centrato nella metà superiore
    qr_x = (page_width - new_w) // 2
    qr_y = margin + (half_height - 2 * margin - new_h) // 2
    page.paste(qr_scaled, (qr_x, qr_y))
    # Predisponi il testo dell'ID nella metà inferiore
    id_text = str(row['id'])
    # Per il testo dell'ID vogliamo occupare quasi tutta la metà inferiore.
    # Lasciamo solo un margine superiore per separare dal QR e nessun margine inferiore
    id_max_w = page_width - 2 * margin
    id_max_h = half_height - margin  # utilizza quasi tutta la metà inferiore
    # Seleziona la dimensione del font per l'ID per riempire l'area disponibile
    font_size = 600  # partiamo da un valore più grande per massimizzare l'altezza
    id_font = None
    id_w = id_h = 0
    while font_size >= 10:
        try:
            # Usa un font bold se disponibile per una migliore resa
            candidate = ImageFont.truetype('DejaVuSans-Bold.ttf', font_size)
        except Exception:
            try:
                candidate = ImageFont.truetype('DejaVuSans.ttf', font_size)
            except Exception:
                candidate = ImageFont.load_default()
        tw, th = draw_page.textsize(id_text, font=candidate)
        if tw <= id_max_w and th <= id_max_h:
            id_font = candidate
            id_w, id_h = tw, th
            break
        font_size -= 2
    if not id_font:
        try:
            id_font = ImageFont.truetype('DejaVuSans-Bold.ttf', 12)
        except Exception:
            try:
                id_font = ImageFont.truetype('DejaVuSans.ttf', 12)
            except Exception:
                id_font = ImageFont.load_default()
        id_w, id_h = draw_page.textsize(id_text, font=id_font)
    # Calcola posizione per il testo ID
    id_x = (page_width - id_w) // 2
    # Allinea il testo ID in alto nella metà inferiore per usare più spazio verticale
    id_y = half_height + margin
    draw_page.text((id_x, id_y), id_text, fill='black', font=id_font)
    # Salva su buffer come immagine PNG
    buf = BytesIO()
    page.save(buf, format='PNG')
    buf.seek(0)
    filename = f"qr_{row['id']}.png"
    return send_file(buf, mimetype='image/png', as_attachment=True, download_name=filename)

# ---------------------------------------------------------------------------
# Pagina di stampa per i QR code
#
# Per consentire la stampa rapida dei codici QR direttamente dal browser,
# aggiungiamo un endpoint dedicato che rende un template HTML con il
# codice QR. Alla visualizzazione, la pagina invoca automaticamente
# ``window.print()`` tramite il template ``print_qr.html``. In questo modo
# l'utente può stampare l'etichetta con un solo click, senza dover
# scaricare manualmente il PNG.
@app.route('/print_qr_page/<int:material_id>')
def print_qr_page(material_id: int):
    """Visualizza una pagina HTML con il QR code del materiale e avvia la stampa.

    Quando questo endpoint viene aperto in una nuova scheda, il template
    ``print_qr.html`` caricherà il codice QR e le informazioni
    essenziali del materiale. All'onload della pagina il browser
    richiamerà automaticamente la finestra di stampa per consentire
    all'utente di stampare l'etichetta.

    :param material_id: identificatore del materiale
    :return: una risposta HTML renderizzata
    """
    # Recupera la riga del materiale dal database. Se non esiste, si
    # reindirizza l'utente alla dashboard con un messaggio di errore.
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT * FROM materiali WHERE id=?",
            (material_id,),
        ).fetchone()
    if not row:
        flash('Materiale non trovato per la stampa del QR.', 'danger')
        return redirect(url_for('dashboard'))
    # Converti la riga SQLite Row in un dizionario per un accesso
    # affidabile nei template Jinja.
    materiale_dict = dict(row)
    return render_template('print_qr.html', materiale=materiale_dict)

# ---------------------------------------------------------------------------
# Spostamento massivo dell'ubicazione dei materiali
#
# Questo endpoint consente di spostare l'ubicazione di uno o più materiali selezionati.
# Accetta una lista di ID (separati da virgola) tramite il campo form ``selected_ids`` e
# una stringa di destinazione ``target_location``.  Per ciascun ID selezionato viene
# aggiornata la lettera e il numero di ubicazione.  Se l'ID corrisponde a un bancale,
# anche tutte le lastre figlie (righe con ``parent_id`` uguale all'ID del bancale) vengono
# spostate.  L'endpoint restituisce un oggetto JSON con la conferma dell'operazione.
@app.route('/bulk_move_location', methods=['POST'])
def bulk_move_location():
    """
    Sposta l'ubicazione di più materiali selezionati, con merge se necessario.

    Legge gli identificativi selezionati e una nuova ubicazione dal corpo della richiesta.
    Esegue la convalida dei parametri e utilizza la funzione di parsing
    ``_parse_location_string`` per estrarre lettera e numero dall'ubicazione.  Per ogni
    bancale selezionato vengono inclusi anche i relativi figli nella lista degli ID da
    aggiornare.  Dopo aver aggiornato l'ubicazione, se nella destinazione esiste già un bancale
    con la stessa combinazione (materiale, tipo, fornitore, produttore, spessore), le lastre
    vengono unite a quel bancale e il bancale sorgente viene eliminato.  In caso contrario,
    il bancale viene semplicemente spostato.
    """
    ids_csv = request.form.get('selected_ids', '').strip()
    target = request.form.get('target_location', '').strip()
    # Estrae lettera e numero dalla stringa di ubicazione
    lettera, numero = _parse_location_string(target)
    if not ids_csv or lettera is None or numero is None:
        return jsonify({'ok': False, 'message': 'Parametri non validi.'}), 400
    # Converte gli ID in interi filtrando i valori non numerici
    try:
        ids = [int(x) for x in ids_csv.split(',') if x.strip().isdigit()]
    except Exception:
        ids = []
    if not ids:
        return jsonify({'ok': False, 'message': 'Nessun ID selezionato.'}), 400
    merged = 0
    updated = 0
    # Dizionario per salvare lo stato pre-spostamento di ogni ID.  Verrà
    # utilizzato dopo l'aggiornamento per registrare gli eventi nello storico.
    pre_move_info: dict[int, dict] = {}
    with get_db_connection() as conn:
        # Recupera i record selezionati per determinare quali sono bancali (e per il merge)
        placeholders = ','.join(['?'] * len(ids))
        selected_rows = conn.execute(
            f"SELECT * FROM materiali WHERE id IN ({placeholders})",
            ids
        ).fetchall()
        # Insieme di tutti gli ID da aggiornare (inclusi quelli figli dei bancali)
        all_ids = set(ids)
        pallet_rows = []
        for row in selected_rows:
            row_dict = dict(row)
            if row_dict.get('is_pallet'):
                pallet_rows.append(row_dict)
                # Aggiungi figli per update di ubicazione
                child_rows = conn.execute(
                    "SELECT id FROM materiali WHERE parent_id=?",
                    (row_dict['id'],)
                ).fetchall()
                for child in child_rows:
                    all_ids.add(child['id'])
        # Prima di effettuare l'aggiornamento salviamo lo stato corrente di tutte le lastre interessate
        if all_ids:
            pl = ','.join(['?'] * len(all_ids))
            rows_pre = conn.execute(
                f"SELECT id, ubicazione_lettera, ubicazione_numero, dimensione_x, dimensione_y, spessore, materiale, tipo, fornitore, produttore, is_pallet, parent_id, note FROM materiali WHERE id IN ({pl})",
                list(all_ids)
            ).fetchall()
            for rr in rows_pre:
                pre_move_info[rr['id']] = dict(rr)

        # Aggiorna la lettera e il numero di ubicazione per tutti gli ID
        placeholders2 = ','.join(['?'] * len(all_ids))
        conn.execute(
            f"UPDATE materiali SET ubicazione_lettera=?, ubicazione_numero=? WHERE id IN ({placeholders2})",
            (lettera, numero, *list(all_ids))
        )
        updated = conn.total_changes
        # Per ogni bancale selezionato, verifica se esiste già un bancale equivalente nella destinazione
        for src in pallet_rows:
            eq = conn.execute(
                "SELECT id, quantita FROM materiali WHERE is_pallet=1 AND parent_id IS NULL "
                "AND COALESCE(materiale,'')=COALESCE(?, '') "
                "AND COALESCE(tipo,'')=COALESCE(?, '') "
                "AND COALESCE(fornitore,'')=COALESCE(?, '') "
                "AND COALESCE(produttore,'')=COALESCE(?, '') "
                "AND COALESCE(spessore,'')=COALESCE(?, '') "
                "AND COALESCE(ubicazione_lettera,'')=? AND COALESCE(ubicazione_numero,0)=? "
                "AND id != ? "
                "LIMIT 1",
                (
                    src.get('materiale') or '',
                    src.get('tipo') or '',
                    src.get('fornitore') or '',
                    src.get('produttore') or '',
                    src.get('spessore') or '',
                    lettera,
                    numero,
                    src['id']
                )
            ).fetchone()
            if not eq:
                continue
            target_id = eq['id']
            # Riassegna i figli al target
            conn.execute("UPDATE materiali SET parent_id=? WHERE parent_id=?", (target_id, src['id']))
            # Somma le quantità sul target
            try:
                src_q = int(src.get('quantita') or 0)
            except Exception:
                src_q = 0
            try:
                tgt_q = int(eq['quantita'] or 0)
            except Exception:
                tgt_q = 0
            conn.execute("UPDATE materiali SET quantita=? WHERE id=?", (tgt_q + src_q, target_id))
            # Ricollega documenti, se la tabella documenti esiste
            try:
                conn.execute("UPDATE documenti SET material_id=? WHERE material_id=?", (target_id, src['id']))
            except Exception:
                pass
            # Elimina il bancale sorgente
            conn.execute("DELETE FROM materiali WHERE id=?", (src['id'],))
            merged += 1
        # Registriamo gli eventi di spostamento nello storico per ogni lastra non bancale
        # Prepara gli eventi di spostamento in batch per tutte le lastre non bancali
        events_move: list[dict] = []
        try:
            for sid, info in pre_move_info.items():
                try:
                    is_pallet_val = int(info.get('is_pallet') or 0)
                except Exception:
                    is_pallet_val = 0
                if is_pallet_val == 1:
                    continue
                events_move.append({
                    'slab_id': sid,
                    'event_type': 'spostato',
                    'from_letter': info.get('ubicazione_lettera'),
                    'from_number': info.get('ubicazione_numero'),
                    'to_letter': lettera,
                    'to_number': numero,
                    'dimensione_x': info.get('dimensione_x'),
                    'dimensione_y': info.get('dimensione_y'),
                    'spessore': info.get('spessore'),
                    'materiale': info.get('materiale'),
                    'tipo': info.get('tipo'),
                    'fornitore': info.get('fornitore'),
                    'produttore': info.get('produttore'),
                    'note': info.get('note'),
                    'nesting_link': None,
                })
        except Exception:
            # Ignora errori di preparazione
            pass
        if events_move:
            # Commit prima di registrare gli eventi di spostamento. Le modifiche
            # effettuate (update dell'ubicazione e merge) devono essere persistite
            # affinché la connessione di ``log_slab_events`` possa vedere lo
            # stato aggiornato.
            try:
                conn.commit()
            except Exception:
                pass
            log_slab_events(events_move)
    return jsonify({'ok': True, 'updated': updated, 'merged': merged, 'target': f"{lettera}-{numero}"})

# ---------------------------------------------------------------------------
# Stampa diretta su stampante Zebra
#
# Questi endpoint consentono di inviare direttamente alla stampante Zebra
# l'etichetta di un singolo materiale oppure di una lista di materiali.
# Utilizzano le funzioni ausiliarie definite nella parte superiore del file
# per costruire le stringhe ZPL e inviarle tramite socket.  Dopo la
# stampa l'utente viene reindirizzato alla pagina precedente (se
# disponibile) o alla dashboard.

@app.route('/print_label/<int:material_id>')
def print_label(material_id: int):
    """Stampa l'etichetta di un singolo materiale su stampante Zebra.

    Verifica che il materiale esista nel database, poi costruisce e invia
    l'etichetta alla stampante.  In caso di successo viene mostrato un
    messaggio di conferma, altrimenti un avviso di errore.  Al termine
    si reindirizza l'utente alla pagina da cui è arrivato (referrer) o
    alla dashboard se l'informazione non è disponibile.

    :param material_id: identificatore del materiale da stampare
    :return: una redirezione HTTP
    """
    # Verifica l'esistenza del materiale
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT id FROM materiali WHERE id=?",
            (material_id,)
        ).fetchone()
    if not row:
        flash('Materiale non trovato per la stampa.', 'danger')
        return redirect(url_for('dashboard'))
    # Effettua la stampa
    if _print_label_to_zebra(material_id):
        flash(f'Etichetta per il materiale {material_id} inviata alla stampante.', 'success')
    else:
        flash('Errore durante la stampa dell\'etichetta.', 'danger')
    # Torna alla pagina precedente oppure alla dashboard
    ref = request.referrer or url_for('dashboard')
    return redirect(ref)


@app.route('/print_labels')
def print_labels():
    """Stampa una serie di etichette su stampante Zebra.

    Gli identificativi dei materiali da stampare devono essere forniti
    tramite la query string ``ids`` separati da virgola (es.
    ``/print_labels?ids=3,4,5``).  La funzione tenterà di stampare
    tutte le etichette in successione.  Alla fine verrà mostrato un
    messaggio indicante quante etichette sono state inviate
    correttamente.  In assenza di ID validi l'utente viene avvisato.

    :return: una redirezione HTTP
    """
    ids_param = request.args.get('ids', '')
    if not ids_param:
        flash('Nessun ID da stampare.', 'warning')
        return redirect(url_for('dashboard'))
    # Estrae solo numeri interi validi
    try:
        ids = [int(x) for x in ids_param.split(',') if x.strip().isdigit()]
    except Exception:
        ids = []
    if not ids:
        flash('Nessun ID valido da stampare.', 'warning')
        return redirect(url_for('dashboard'))
    # Stampa ciascuna etichetta
    success_count = 0
    for mid in ids:
        if _print_label_to_zebra(mid):
            success_count += 1
    if success_count:
        flash(f'Stampate {success_count} etichette.', 'success')
    else:
        flash('Nessuna etichetta è stata stampata.', 'danger')
    ref = request.referrer or url_for('dashboard')
    return redirect(ref)

# ---------------------------------------------------------------------------
# Gestione del dizionario materiali
#
# Queste route permettono di visualizzare, aggiungere e rimuovere
# materiali dal vocabolario utilizzato nelle schermate di inserimento
# e modifica.  Aggiungendo nuovi materiali qui, l'utente può
# selezionarli dal menu a tendina senza dover modificare il codice.

@app.route('/dizionario', methods=['GET', 'POST'])
def dizionario():
    """Pagina per la gestione del vocabolario dei materiali.

    In GET mostra l'elenco dei materiali attualmente disponibili. In
    POST aggiunge un nuovo materiale, se non vuoto. Se il materiale
    esiste già nel vocabolario, viene mostrato un avviso.
    """
    if request.method == 'POST':
        # Gestisce l'aggiunta di nuovi materiali al dizionario.  Il campo
        # "nome" è obbligatorio per l'inserimento.  I tipi sono
        # gestiti da un'altra route dedicata.
        nome = request.form.get('nome', '').strip()
        if nome:
            with get_db_connection() as conn:
                try:
                    conn.execute(f"INSERT INTO {VOCAB_TABLE} (nome) VALUES (?)", (nome,))
                    flash('Materiale aggiunto al dizionario!', 'success')
                except sqlite3.IntegrityError:
                    # Nomi duplicati sollevano un errore di integrità; avvisa l'utente
                    flash('Il materiale esiste già nel dizionario.', 'warning')
        return redirect(url_for('dizionario'))
    # GET
    with get_db_connection() as conn:
        # Recupera l'elenco dei materiali presenti nel vocabolario
        materials = conn.execute(f"SELECT id, nome FROM {VOCAB_TABLE} ORDER BY nome").fetchall()
        # Recupera l'elenco dei tipi di lavorazione
        tipi = conn.execute(f"SELECT id, nome FROM {TIPO_TABLE} ORDER BY nome").fetchall()
        # Preleva la lista delle macchine per visualizzarle nella pagina del dizionario
        try:
            macchine = conn.execute(f"SELECT id, nome FROM {MACCHINE_TABLE} ORDER BY nome").fetchall()
        except sqlite3.Error:
            macchine = []
        # Recupera la lista dei produttori per mostrarla nella pagina del dizionario
        try:
            produttori = conn.execute(f"SELECT id, nome FROM {PRODUTTORE_TABLE} ORDER BY nome").fetchall()
        except sqlite3.Error:
            produttori = []
    # Includi anche i produttori nella pagina dizionario
    return render_template(
        'dizionario.html',
        title='Dizionario',
        materials=materials,
        tipi=tipi,
        macchine=macchine,
        produttori=produttori,
    )


@app.route('/delete_materiale_vocab/<int:materiale_id>', methods=['POST'])
def delete_materiale_vocab(materiale_id: int):
    """Elimina un materiale dal dizionario.

    L'eliminazione viene eseguita tramite POST per prevenire
    cancellazioni accidentali via URL. Dopo la rimozione viene
    reindirizzato l'utente alla pagina del dizionario.
    """
    with get_db_connection() as conn:
        conn.execute(f"DELETE FROM {VOCAB_TABLE} WHERE id=?", (materiale_id,))
    flash('Materiale rimosso dal dizionario!', 'success')
    return redirect(url_for('dizionario'))


# ---------------------------------------------------------------------------
# Gestione del dizionario dei tipi di lavorazione/materiali

@app.route('/add_tipo_vocab', methods=['POST'])
def add_tipo_vocab():
    """Aggiunge un nuovo tipo al dizionario tipi.

    Il parametro "nome" deve essere presente nel corpo della richiesta.  In
    caso di duplicato viene visualizzato un messaggio di avviso.
    """
    nome = request.form.get('nome', '').strip()
    if nome:
        with get_db_connection() as conn:
            try:
                conn.execute(f"INSERT INTO {TIPO_TABLE} (nome) VALUES (?)", (nome,))
                flash('Tipo aggiunto al dizionario!', 'success')
            except sqlite3.IntegrityError:
                flash('Il tipo esiste già nel dizionario.', 'warning')
    return redirect(url_for('dizionario'))


@app.route('/delete_tipo_vocab/<int:tipo_id>', methods=['POST'])
def delete_tipo_vocab(tipo_id: int):
    """Elimina un tipo dal dizionario.

    L'eliminazione avviene tramite POST per evitare cancellazioni
    accidentali.  Se il tipo è in uso nei materiali, non viene
    automaticamente rimosso dalle lastre esistenti, tuttavia sarà
    semplicemente una stringa orfana nella tabella ``materiali``.
    """
    with get_db_connection() as conn:
        conn.execute(f"DELETE FROM {TIPO_TABLE} WHERE id=?", (tipo_id,))
    flash('Tipo rimosso dal dizionario!', 'success')
    return redirect(url_for('dizionario'))


# ---------------------------------------------------------------------------
# Gestione del dizionario delle macchine

@app.route('/dizionario_macchine', methods=['GET', 'POST'])
def dizionario_macchine():
    """Gestione del vocabolario delle macchine.

    Questa route viene mantenuta per retrocompatibilità ma reindirizza
    alla pagina principale del dizionario. In caso di POST aggiunge la
    macchina e poi torna al dizionario. In caso di GET mostra lo
    stesso contenuto della pagina del dizionario.
    """
    if request.method == 'POST':
        nome = request.form.get('nome', '').strip()
        if nome:
            with get_db_connection() as conn:
                try:
                    conn.execute(f"INSERT INTO {MACCHINE_TABLE} (nome) VALUES (?)", (nome,))
                    flash('Macchina aggiunta al dizionario!', 'success')
                except sqlite3.IntegrityError:
                    flash('La macchina esiste già nel dizionario.', 'warning')
        # Torna alla pagina principale del dizionario
        return redirect(url_for('dizionario'))
    # GET: reindirizza alla pagina dizionario che ora include anche la gestione delle macchine
    return redirect(url_for('dizionario'))


@app.route('/delete_macchina_vocab/<int:macchina_id>', methods=['POST'])
def delete_macchina_vocab(macchina_id: int):
    """Elimina una macchina dal dizionario.

    L'eliminazione avviene tramite POST per evitare cancellazioni
    accidentali tramite URL.  Se la macchina è referenziata in
    prenotazioni, la colonna ``macchina_id`` rimarrà con un riferimento
    orfano ma non influenzerà il funzionamento dell'applicazione.
    """
    with get_db_connection() as conn:
        conn.execute(f"DELETE FROM {MACCHINE_TABLE} WHERE id=?", (macchina_id,))
    flash('Macchina rimossa dal dizionario!', 'success')
    # Torna alla pagina principale del dizionario poiché la gestione macchine è stata accorpata
    return redirect(url_for('dizionario'))


# ---------------------------------------------------------------------------
# Gestione del dizionario fornitori

@app.route('/dizionario_fornitori', methods=['GET', 'POST'])
def dizionario_fornitori():
    """Gestione del vocabolario dei fornitori.

    Questa route rimane per retrocompatibilità ma reindirizza alla
    pagina "Anagrafiche articoli" (config) dove ora è presente la
    sezione per la gestione dei fornitori. In caso di POST aggiunge il
    fornitore e torna a tale pagina. In caso di GET reindirizza
    semplicemente alla pagina di configurazione.
    """
    if request.method == 'POST':
        nome = request.form.get('nome', '').strip()
        # Legge il campo email se presente nel form.  L'email è opzionale.
        email = request.form.get('email', '').strip()
        if nome:
            with get_db_connection() as conn:
                try:
                    # Determina se la colonna email è presente nella tabella fornitori.
                    try:
                        cols = conn.execute(f"PRAGMA table_info({SUPPLIER_TABLE})").fetchall()
                        col_names = {row['name'] for row in cols}
                    except sqlite3.Error:
                        col_names = set()
                    # Inserisce sia il nome che l'email se la colonna email è disponibile;
                    # altrimenti inserisce solo il nome (compatibilità con database legacy).
                    if 'email' in col_names:
                        conn.execute(f"INSERT INTO {SUPPLIER_TABLE} (nome, email) VALUES (?, ?)", (nome, email if email else None))
                    else:
                        conn.execute(f"INSERT INTO {SUPPLIER_TABLE} (nome) VALUES (?)", (nome,))
                    flash('Fornitore aggiunto al dizionario!', 'success')
                except sqlite3.IntegrityError:
                    flash('Il fornitore esiste già nel dizionario.', 'warning')
        return redirect(url_for('config'))
    # GET: reindirizza alla pagina di configurazione (anagrafiche articoli)
    return redirect(url_for('config'))


@app.route('/delete_fornitore_vocab/<int:fornitore_id>', methods=['POST'])
def delete_fornitore_vocab(fornitore_id: int):
    """Elimina un fornitore dal dizionario.

    L'eliminazione viene eseguita tramite POST per prevenire
    cancellazioni accidentali via URL. Dopo la rimozione viene
    reindirizzato l'utente alla pagina del dizionario fornitori.
    """
    with get_db_connection() as conn:
        conn.execute(f"DELETE FROM {SUPPLIER_TABLE} WHERE id=?", (fornitore_id,))
    flash('Fornitore rimosso dal dizionario!', 'success')
    # Torna alla pagina di configurazione dove è stata spostata la gestione fornitori
    return redirect(url_for('config'))

# ---------------------------------------------------------------------------
# Aggiornamento dei fornitori

@app.route('/update_fornitore/<int:fornitore_id>', methods=['POST'])
def update_fornitore(fornitore_id: int):
    """Aggiorna il nome e/o l'email di un fornitore nel dizionario.

    Questa route viene invocata dalla pagina di configurazione quando
    l'utente modifica l'indirizzo email associato a un fornitore.  Se
    la colonna email non è presente nel database (versioni legacy),
    viene aggiornata solamente la colonna nome.  Dopo l'aggiornamento
    l'utente viene reindirizzato alla pagina di configurazione.

    :param fornitore_id: l'identificativo del fornitore da modificare
    :return: un reindirizzamento alla vista di configurazione
    """
    nome = request.form.get('nome', '').strip()
    email = request.form.get('email', '').strip()
    # Se non viene fornito un nome mantieni quello esistente (lo recuperiamo)
    with get_db_connection() as conn:
        try:
            # Recupera l'email esistente per preservare il valore se l'utente lascia il campo vuoto
            existing = conn.execute(f"SELECT nome, email FROM {SUPPLIER_TABLE} WHERE id=?", (fornitore_id,)).fetchone()
            if existing:
                if not nome:
                    nome = existing['nome']
                if not email and 'email' in existing.keys():
                    email = existing['email'] or ''
            # Verifica se la colonna email esiste nella tabella
            try:
                cols = conn.execute(f"PRAGMA table_info({SUPPLIER_TABLE})").fetchall()
                col_names = {row['name'] for row in cols}
            except sqlite3.Error:
                col_names = set()
            if 'email' in col_names:
                conn.execute(f"UPDATE {SUPPLIER_TABLE} SET nome=?, email=? WHERE id=?", (nome, email if email else None, fornitore_id))
            else:
                conn.execute(f"UPDATE {SUPPLIER_TABLE} SET nome=? WHERE id=?", (nome, fornitore_id))
            conn.commit()
            flash('Fornitore aggiornato con successo!', 'success')
        except sqlite3.Error:
            flash('Errore durante l\'aggiornamento del fornitore.', 'danger')
    return redirect(url_for('config'))

# ---------------------------------------------------------------------------
# Gestione del dizionario produttori

@app.route('/dizionario_produttori', methods=['GET', 'POST'])
def dizionario_produttori():
    """Gestione del vocabolario dei produttori.

    Questa route permette di aggiungere o rimuovere i produttori
    disponibili per l'inserimento e i filtri del magazzino.  Per
    uniformità con la gestione dei fornitori, questa vista non
    presenta un template dedicato ma reindirizza alla pagina
    "Anagrafiche articoli" (config) dove è presente la sezione per
    gestire il dizionario dei produttori.  In caso di POST viene
    inserito il produttore nel vocabolario e si torna a tale pagina.
    In caso di GET si effettua solo il reindirizzamento.
    """
    if request.method == 'POST':
        nome = request.form.get('nome', '').strip()
        if nome:
            with get_db_connection() as conn:
                try:
                    conn.execute(f"INSERT INTO {PRODUTTORE_TABLE} (nome) VALUES (?)", (nome,))
                    flash('Produttore aggiunto al dizionario!', 'success')
                except sqlite3.IntegrityError:
                    flash('Il produttore esiste già nel dizionario.', 'warning')
        # Dopo l'aggiunta reindirizza alla pagina principale del dizionario dove ora si gestiscono i produttori
        return redirect(url_for('dizionario'))
    # GET: reindirizza alla pagina di configurazione (anagrafiche articoli)
    # Visualizza la pagina principale del dizionario anziché la pagina di configurazione
    return redirect(url_for('dizionario'))


@app.route('/delete_produttore_vocab/<int:produttore_id>', methods=['POST'])
def delete_produttore_vocab(produttore_id: int):
    """Elimina un produttore dal dizionario.

    L'eliminazione viene eseguita tramite POST per prevenire
    cancellazioni accidentali via URL.  Dopo la rimozione viene
    reindirizzato l'utente alla pagina del dizionario produttori
    nella sezione anagrafiche articoli.
    """
    with get_db_connection() as conn:
        conn.execute(f"DELETE FROM {PRODUTTORE_TABLE} WHERE id=?", (produttore_id,))
    flash('Produttore rimosso dal dizionario!', 'success')
    # Dopo la rimozione torna alla pagina principale del dizionario, dove è stata spostata la gestione produttori
    return redirect(url_for('dizionario'))


# ---------------------------------------------------------------------------
# Pagina di configurazione delle soglie
@app.route('/config', methods=['GET', 'POST'])
def config():
    """Pagina per configurare le soglie di riordino per ogni materiale, tipo e spessore.

    Questa vista mostra l'elenco delle combinazioni di ``materiale``, ``tipo`` e
    ``spessore`` presenti nel database. Per ciascuna è possibile specificare
    un valore di soglia personalizzato. In caso di POST le soglie vengono
    aggiornate (o inserite) nella tabella ``riordino_soglie``. L'alert
    threshold globale continua ad essere letto dal file ``thresholds.txt`` ma
    non è modificabile da questa pagina.
    """
    if request.method == 'POST':
        # Riceviamo le liste dei campi dal form (array paralleli).  Oltre a materiale,
        # tipo e spessore, il template invia anche le dimensioni e il produttore della
        # combinazione.  Questi valori vengono letti qui per mantenere gli array
        # allineati, anche se la logica corrente delle soglie continua a basarsi
        # sulla terna (materiale, tipo, spessore).  In futuro si potrà estendere
        # l'utilizzo di dimensione_x, dimensione_y e produttore per definire soglie
        # più granulari.
        materiali = request.form.getlist('materiale[]')
        tipi = request.form.getlist('tipo[]')
        spessori = request.form.getlist('spessore[]')
        soglie = request.form.getlist('soglia[]')
        # Nuovi campi aggiunti al form per ciascuna riga dell'anagrafica.  Vengono letti per
        # mantenere le liste sincronizzate con gli altri campi e per consentire
        # l'aggiornamento granulare sulla tabella estesa.
        dimensioni_x = request.form.getlist('dimensione_x[]')
        dimensioni_y = request.form.getlist('dimensione_y[]')
        produttori_post = request.form.getlist('produttore[]')
        # Identificatore univoco dell'articolo (facoltativo).  È usato solo a
        # scopo informativo; la logica di aggiornamento delle soglie non fa
        # affidamento su questo valore.
        articolo_ids = request.form.getlist('articolo_id[]')
        # Nuova colonna: quantità di riordino. Potrebbe mancare se il client non la invia.
        quantita_riordino_vals = request.form.getlist('quantita_riordino[]')
        # Allinea la lunghezza della lista quantita_riordino a quella degli altri campi
        # Se la lista è più corta o mancante riempiamo con stringhe vuote
        if not quantita_riordino_vals:
            quantita_riordino_vals = [''] * len(materiali)
        elif len(quantita_riordino_vals) < len(materiali):
            quantita_riordino_vals += [''] * (len(materiali) - len(quantita_riordino_vals))
        # Allinea anche le liste delle dimensioni e dei produttori alla lunghezza dei materiali
        if not dimensioni_x or len(dimensioni_x) < len(materiali):
            dimensioni_x = (dimensioni_x or []) + [''] * (len(materiali) - len(dimensioni_x or []))
        if not dimensioni_y or len(dimensioni_y) < len(materiali):
            dimensioni_y = (dimensioni_y or []) + [''] * (len(materiali) - len(dimensioni_y or []))
        if not produttori_post or len(produttori_post) < len(materiali):
            produttori_post = (produttori_post or []) + [''] * (len(materiali) - len(produttori_post or []))
        with get_db_connection() as conn:
            # Verifica e aggiorna lo schema della tabella estesa prima di utilizzarla
            ensure_riordino_soglie_ext_schema(conn)
            for m, t, s, dx, dy, prod, th, qr in zip(materiali, tipi, spessori, dimensioni_x, dimensioni_y, produttori_post, soglie, quantita_riordino_vals):
                # Normalizza i valori vuoti o None in stringa vuota
                mat = (m or '').strip()
                tp = (t or '').strip()
                sp = (s or '').strip()
                dx_norm = (dx or '').strip()
                dy_norm = (dy or '').strip()
                prod_norm = (prod or '').strip()
                # Calcola la soglia; se non valida usa la costante di default
                try:
                    soglia_val = int(th)
                except (ValueError, TypeError):
                    soglia_val = DEFAULT_REORDER_THRESHOLD
                if soglia_val < 0:
                    soglia_val = DEFAULT_REORDER_THRESHOLD
                # Calcola la quantità di riordino; se non valida o <=0 usa 1
                try:
                    qr_val = int(qr) if qr not in (None, '', 'None') else 1
                    if qr_val <= 0:
                        qr_val = 1
                except (ValueError, TypeError):
                    qr_val = 1
                # Aggiorna la tabella estesa per la combinazione completa
                conn.execute(
                    "INSERT OR REPLACE INTO riordino_soglie_ext (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore, threshold, quantita_riordino) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (mat, tp, sp, dx_norm, dy_norm, prod_norm, soglia_val, qr_val)
                )
                # Aggiorna anche la tabella legacy per retro‑compatibilità
                conn.execute(
                    f"INSERT OR REPLACE INTO {RIORDINO_SOGGIE_TABLE} (materiale, tipo, spessore, threshold, quantita_riordino) VALUES (?,?,?,?,?)",
                    (mat, tp, sp, soglia_val, qr_val)
                )
        flash('Soglie salvate con successo!', 'success')
        return redirect(url_for('config'))
    # GET: prepara l'elenco delle combinazioni presenti nel magazzino o nel catalogo con le soglie correnti
    # La tabella di configurazione mostra anche le dimensioni X e Y per ciascuna combinazione.
    # Oltre alle combinazioni derivate dalla tabella ``materiali``, includiamo quelle
    # definite manualmente nella tabella ``articoli_catalogo``.
    pairs: list[dict] = []
    # Mappa soglie legacy: chiave (materiale, tipo, spessore) -> threshold
    threshold_map: dict[tuple[str, str, str], int] = {}
    # Mappa quantità di riordino legacy: chiave (materiale, tipo, spessore) -> quantità impostata o None
    quant_map: dict[tuple[str, str, str], int | None] = {}
    # Mappa soglie estese: chiave (materiale, tipo, spessore, dx, dy, produttore) -> threshold
    threshold_map_ext: dict[tuple[str, str, str, str, str, str], int] = {}
    # Mappa quantità di riordino estesa: chiave (materiale, tipo, spessore, dx, dy, produttore) -> valore o None
    quant_map_ext: dict[tuple[str, str, str, str, str, str], int | None] = {}
    with get_db_connection() as conn:
        try:
            cur = conn.execute(
                f"SELECT materiale, tipo, spessore, threshold, quantita_riordino FROM {RIORDINO_SOGGIE_TABLE}"
            ).fetchall()
            for row in cur:
                key = (row['materiale'], row['tipo'] or '', row['spessore'] or '')
                # Soglia di riordino
                try:
                    threshold_map[key] = int(row['threshold'])
                except (ValueError, TypeError):
                    threshold_map[key] = DEFAULT_REORDER_THRESHOLD
                # Quantità di riordino (può essere None)
                
                try:
                    qr_raw = row['quantita_riordino']
                    qr_val = int(qr_raw) if qr_raw not in (None, '') else None
                except (ValueError, TypeError):
                    qr_val = None
                # Se la quantità è non positiva la consideriamo nulla
                if qr_val is not None and qr_val <= 0:
                    qr_val = None
                quant_map[key] = qr_val
        except sqlite3.Error:
            threshold_map = {}
            quant_map = {}
        # Carica soglie e quantità di riordino dalla tabella estesa
        try:
            cur2 = conn.execute(
                "SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy, COALESCE(produttore,'') AS prod, "
                "threshold, quantita_riordino FROM riordino_soglie_ext"
            ).fetchall()
            for row in cur2:
                key_ext = (
                    row['materiale'],
                    row['tipo'] or '',
                    row['spessore'] or '',
                    (row['dx'] or '').strip(),
                    (row['dy'] or '').strip(),
                    (row['prod'] or '').strip(),
                )
                # soglia di riordino estesa
                try:
                    threshold_map_ext[key_ext] = int(row['threshold'])
                except (ValueError, TypeError):
                    threshold_map_ext[key_ext] = DEFAULT_REORDER_THRESHOLD
                # quantità di riordino estesa
                try:
                    qr_raw = row['quantita_riordino']
                    qr_val = int(qr_raw) if qr_raw not in (None, '') else None
                except (ValueError, TypeError):
                    qr_val = None
                if qr_val is not None and qr_val <= 0:
                    qr_val = None
                quant_map_ext[key_ext] = qr_val
        except sqlite3.Error:
            threshold_map_ext = {}
            quant_map_ext = {}
        # Non estraiamo più le combinazioni distinte dal magazzino da visualizzare
        # nella tabella di configurazione.  La pagina "Anagrafiche articoli"
        # deve infatti mostrare solo le combinazioni definite manualmente
        # dall'utente (articoli catalogo) e non quelle derivate
        # automaticamente dalle lastre presenti nel magazzino.
        combo_rows = []
        # Recupera combinazioni dal catalogo articoli; se la tabella non
        # esiste o non contiene valori restituiamo una lista vuota.
        try:
            # Includiamo anche l'ID e il flag di preferito per ciascun articolo del catalogo
            art_rows = conn.execute(
                "SELECT id, materiale, tipo, spessore, dimensione_x, dimensione_y, preferito FROM articoli_catalogo"
            ).fetchall()
        except sqlite3.Error:
            art_rows = []
    # Crea mappa per ID e preferito delle combinazioni da articoli_catalogo
    id_pref_map: dict[tuple[str, str, str, str, str], tuple[int | None, int]] = {}
    try:
        for r in art_rows:
            tp_norm = (r['tipo'] or '')
            sp_norm = (r['spessore'] or '')
            dx_norm = (r['dimensione_x'] or '').strip()
            dy_norm = (r['dimensione_y'] or '').strip()
            key = (r['materiale'], tp_norm, sp_norm, dx_norm, dy_norm)
            try:
                ident = int(r['id']) if r['id'] is not None else None
            except Exception:
                ident = None
            try:
                pref_flag = int(r['preferito'] or 0)
            except Exception:
                pref_flag = 0
            id_pref_map[key] = (ident, pref_flag)
    except Exception:
        id_pref_map = {}

    # Utilizza un dizionario per evitare duplicati tra magazzino e catalogo
    combos_dict: dict[tuple[str, str, str, str, str], dict] = {}
    def add_combo(mat, tp, sp, dx, dy):
        # Normalizza i campi None -> stringa vuota
        tp_norm = tp or ''
        sp_norm = sp or ''
        dx_norm = (dx or '').strip()
        dy_norm = (dy or '').strip()
        key = (mat, tp_norm, sp_norm, dx_norm, dy_norm)
        if key not in combos_dict:
            # La soglia e la quantità di riordino vengono assegnate successivamente
            # quando si conosce anche il produttore.  Per ora inizializza con i
            # valori legacy; verranno eventualmente sovrascritti nell'iterazione
            # finale durante la costruzione di ``pairs``.
            threshold_val_tmp = threshold_map.get((mat, tp_norm, sp_norm), DEFAULT_REORDER_THRESHOLD)
            quant_tmp = quant_map.get((mat, tp_norm, sp_norm))
            combos_dict[key] = {
                'materiale': mat,
                'tipo': tp_norm,
                'spessore': sp_norm,
                'dimensione_x': dx_norm,
                'dimensione_y': dy_norm,
                'soglia': threshold_val_tmp,
                'quantita_riordino': quant_tmp,
                'articolo_id': id_pref_map.get(key, (None, 0))[0],
                'preferito': id_pref_map.get(key, (None, 0))[1],
            }
    # Aggiungi solo le combinazioni definite manualmente nel catalogo.
    for combo in art_rows:
        add_combo(
            combo['materiale'],
            combo['tipo'],
            combo['spessore'],
            combo['dimensione_x'],
            combo['dimensione_y'],
        )
    # ------------------------------------------------------------------
    # Calcola la quantità attuale e i produttori per ciascuna combinazione di
    # materiale/tipo/spessore/dimensioni e produttore presente nel catalogo.
    # In precedenza si aggregavano le quantità tra tutti i produttori e si
    # visualizzava l'elenco dei produttori associati a ogni combinazione.  Con
    # questa revisione, la logica opera su un singolo produttore: per ogni
    # combinazione (materiale, tipo, spessore, dimensione_x, dimensione_y) viene
    # creata una riga distinta per ciascun produttore presente nel magazzino.
    # Pertanto la chiave include il produttore e la quantità è calcolata
    # separatamente per ogni produttore.
    qty_map: dict[tuple[str, str, str, str, str, str], int] = {}
    # Mappa dei produttori per ogni combinazione senza produttore.  La chiave è
    # (mat, tipo, spessore, dx, dy) e il valore è l'insieme dei produttori
    # associati alla combinazione.  Serve per generare righe separate.
    prod_map: dict[tuple[str, str, str, str, str], set[str]] = {}
    with get_db_connection() as conn:
        try:
            # Calcola la somma delle quantità per ogni combinazione includendo il produttore.
            # Usiamo COALESCE per normalizzare i None a stringa vuota.  Raggruppiamo
            # anche per produttore per poter sommare la quantità separatamente.
            cur_qty = conn.execute(
                "SELECT materiale, COALESCE(tipo,'') AS tipo, COALESCE(spessore,'') AS spessore, "
                "COALESCE(dimensione_x,'') AS dx, COALESCE(dimensione_y,'') AS dy, "
                "COALESCE(produttore,'') AS produttore, SUM(quantita) AS tot_qty "
                "FROM materiali GROUP BY materiale, tipo, spessore, dimensione_x, dimensione_y, produttore"
            ).fetchall()
            for row in cur_qty:
                mat = row['materiale']
                tp_norm = row['tipo']
                sp_norm = row['spessore']
                dx_norm = (row['dx'] or '').strip()
                dy_norm = (row['dy'] or '').strip()
                prod_norm = (row['produttore'] or '').strip()
                qty_map[(mat, tp_norm, sp_norm, dx_norm, dy_norm, prod_norm)] = int(row['tot_qty'] or 0)
                # Aggiorna anche la mappa dei produttori per la combinazione senza produttore
                key_no_prod = (mat, tp_norm, sp_norm, dx_norm, dy_norm)
                prod_set = prod_map.setdefault(key_no_prod, set())
                # Aggiungiamo anche produttori vuoti (stringa vuota) per combinazioni senza produttore
                prod_set.add(prod_norm)
        except sqlite3.Error:
            # In caso di errore, lascia le mappe vuote
            qty_map = {}
            prod_map = {}
    # Associa a ciascuna combinazione del catalogo (senza produttore) le righe
    # derivanti dai produttori.  La lista finale ``pairs`` conterrà una
    # riga per ogni combinazione e produttore.
    pairs = []
    for key_no_prod, base_item in combos_dict.items():
        mat, tp_norm, sp_norm, dx_norm, dy_norm = key_no_prod
        # Elenco dei produttori per questa combinazione.  Se vuoto, includi
        # comunque una riga con produttore stringa vuota per rappresentare la
        # combinazione priva di produttore.
        prods = sorted(prod_map.get(key_no_prod, {''}))
        for prod in prods:
            prod_norm = prod or ''
            # Copia l'elemento base e arricchisci con il produttore e la quantità
            new_item = base_item.copy()
            new_item['produttore'] = prod_norm
            # Quantità attuale specifica per questo produttore
            new_item['quantita_attuale'] = qty_map.get((mat, tp_norm, sp_norm, dx_norm, dy_norm, prod_norm), 0)
            # Sovrascrivi soglia e quantità di riordino con i valori estesi se disponibili
            ext_key = (mat, tp_norm, sp_norm, dx_norm, dy_norm, prod_norm)
            if ext_key in threshold_map_ext:
                new_item['soglia'] = threshold_map_ext.get(ext_key, DEFAULT_REORDER_THRESHOLD)
            else:
                new_item['soglia'] = threshold_map.get((mat, tp_norm, sp_norm), DEFAULT_REORDER_THRESHOLD)
            if ext_key in quant_map_ext:
                new_item['quantita_riordino'] = quant_map_ext.get(ext_key)
            else:
                new_item['quantita_riordino'] = quant_map.get((mat, tp_norm, sp_norm))
            # Rimuovi la chiave aggregata "produttori" se presente; non serve più
            if 'produttori' in new_item:
                del new_item['produttori']
            pairs.append(new_item)
    # Ordina la lista completa per materiale, tipo, spessore, dimensioni e produttore
    pairs.sort(
        key=lambda c: (
            c['materiale'],
            c['tipo'],
            c['spessore'],
            c['dimensione_x'],
            c['dimensione_y'],
            c.get('produttore', '')
        )
    )
    # Leggi l'alert threshold globale per visualizzarlo (ma non modificarlo)
    _, alert = load_thresholds()
    # Recupera elenco fornitori per la gestione nell'anagrafica articoli (nome ed email)
    with get_db_connection() as conn:
        try:
            suppliers = conn.execute(f"SELECT id, nome, email FROM {SUPPLIER_TABLE} ORDER BY nome").fetchall()
        except sqlite3.Error:
            suppliers = []
    # Recupera elenco produttori per la gestione nell'anagrafica articoli
    with get_db_connection() as conn:
        try:
            producers = conn.execute(f"SELECT id, nome FROM {PRODUTTORE_TABLE} ORDER BY nome").fetchall()
        except sqlite3.Error:
            producers = []
    # Recupera l'anagrafica articoli per mostrarla nella sezione di gestione
    articoli_catalogo = get_articoli_catalogo()
    # Recupera liste per i campi del modulo di aggiunta articoli
    materiali_list = get_materiali_vocabolario()
    # get_tipi_vocabolario restituisce dizionari con id/nome; estrai solo il nome per la lista
    try:
        tipi_list_res = get_tipi_vocabolario()
        tipi_list = [t['nome'] for t in tipi_list_res]
    except Exception:
        tipi_list = []
    # Recupera valori distinti per spessore, dimensione_x e dimensione_y dalle tabelle
    spessori_set = set()
    dimx_set = set()
    dimy_set = set()
    with get_db_connection() as conn:
        try:
            rows_sp = conn.execute(
                "SELECT DISTINCT spessore FROM materiali WHERE spessore IS NOT NULL AND TRIM(spessore) != ''"
            ).fetchall()
            spessori_set.update([row['spessore'] for row in rows_sp if row['spessore'] is not None])
        except sqlite3.Error:
            pass
        try:
            rows_sp2 = conn.execute(
                "SELECT DISTINCT spessore FROM articoli_catalogo WHERE spessore IS NOT NULL AND TRIM(spessore) != ''"
            ).fetchall()
            spessori_set.update([row['spessore'] for row in rows_sp2 if row['spessore'] is not None])
        except sqlite3.Error:
            pass
        try:
            rows_dx = conn.execute(
                "SELECT DISTINCT dimensione_x FROM materiali WHERE dimensione_x IS NOT NULL AND TRIM(dimensione_x) != ''"
            ).fetchall()
            dimx_set.update([row['dimensione_x'] for row in rows_dx if row['dimensione_x'] is not None])
        except sqlite3.Error:
            pass
        try:
            rows_dx2 = conn.execute(
                "SELECT DISTINCT dimensione_x FROM articoli_catalogo WHERE dimensione_x IS NOT NULL AND TRIM(dimensione_x) != ''"
            ).fetchall()
            dimx_set.update([row['dimensione_x'] for row in rows_dx2 if row['dimensione_x'] is not None])
        except sqlite3.Error:
            pass
        try:
            rows_dy = conn.execute(
                "SELECT DISTINCT dimensione_y FROM materiali WHERE dimensione_y IS NOT NULL AND TRIM(dimensione_y) != ''"
            ).fetchall()
            dimy_set.update([row['dimensione_y'] for row in rows_dy if row['dimensione_y'] is not None])
        except sqlite3.Error:
            pass
        try:
            rows_dy2 = conn.execute(
                "SELECT DISTINCT dimensione_y FROM articoli_catalogo WHERE dimensione_y IS NOT NULL AND TRIM(dimensione_y) != ''"
            ).fetchall()
            dimy_set.update([row['dimensione_y'] for row in rows_dy2 if row['dimensione_y'] is not None])
        except sqlite3.Error:
            pass
    spessori_list = sorted(spessori_set, key=lambda v: (float(v) if v.replace('.', '', 1).isdigit() else v))
    dimensione_x_list = sorted(dimx_set, key=lambda v: (float(v) if v.replace('.', '', 1).isdigit() else v))
    dimensione_y_list = sorted(dimy_set, key=lambda v: (float(v) if v.replace('.', '', 1).isdigit() else v))
    return render_template(
        'config.html',
        title='Anagrafiche articoli',
        pairs=pairs,
        alert=alert,
        suppliers=suppliers,
        produttori=producers,
        articoli_catalogo=articoli_catalogo,
        materiali_list=materiali_list,
        tipi_list=tipi_list,
        spessori_list=spessori_list,
        dimensione_x_list=dimensione_x_list,
        dimensione_y_list=dimensione_y_list,
    )


# ------------------------------------------------------------------
# API endpoints per aggiornare soglia e quantità di riordino on-the-fly
#
# Le soglie di riordino e le quantità di riordino possono essere
# modificate direttamente dalla pagina delle anagrafiche articoli. Per
# evitare il refresh dell'intera pagina e la necessità di premere un
# pulsante di salvataggio, definiamo due endpoint che ricevono i
# parametri via POST e aggiornano la tabella ``riordino_soglie`` di
# conseguenza. Entrambi gli endpoint restituiscono un JSON con lo
# stato dell'operazione.

@app.route('/update_soglia', methods=['POST'])
def update_soglia():
    """
    Aggiorna la soglia di riordino per una specifica combinazione di articolo.

    A partire da questa revisione, la soglia viene salvata su una tabella
    estesa ``riordino_soglie_ext`` che include, oltre a ``materiale``,
    ``tipo`` e ``spessore``, anche ``dimensione_x``, ``dimensione_y`` e
    ``produttore``.  In questo modo combinazioni uguali ma con
    produttori diversi non condividono più la stessa soglia.

    Il client può inviare opzionalmente i campi ``dimensione_x``,
    ``dimensione_y`` e ``produttore``.  Se omessi, vengono
    normalizzati a stringa vuota.  La quantità di riordino esistente
    per la stessa combinazione (se presente) viene mantenuta.  In
    assenza di un record precedente, viene utilizzata la quantità
    predefinita (1).
    """
    materiale = (request.form.get('materiale') or '').strip()
    tipo = (request.form.get('tipo') or '').strip()
    spessore = (request.form.get('spessore') or '').strip()
    dx = (request.form.get('dimensione_x') or '').strip()
    dy = (request.form.get('dimensione_y') or '').strip()
    produttore = (request.form.get('produttore') or '').strip()
    soglia_raw = request.form.get('soglia')
    try:
        soglia_val = int(soglia_raw)
    except (ValueError, TypeError):
        soglia_val = DEFAULT_REORDER_THRESHOLD
    # Evita soglie negative
    if soglia_val < 0:
        soglia_val = DEFAULT_REORDER_THRESHOLD
    with get_db_connection() as conn:
        # Verifica e aggiorna lo schema della tabella estesa prima di utilizzarla
        ensure_riordino_soglie_ext_schema(conn)
        # Recupera la quantità di riordino esistente dalla tabella estesa se presente.
        qr_val = None
        try:
            qr_row_ext = conn.execute(
                "SELECT quantita_riordino FROM riordino_soglie_ext WHERE materiale=? AND tipo=? AND spessore=? AND dimensione_x=? AND dimensione_y=? AND produttore=?",
                (materiale, tipo, spessore, dx, dy, produttore),
            ).fetchone()
            if qr_row_ext and qr_row_ext['quantita_riordino'] not in (None, ''):
                qr_tmp = int(qr_row_ext['quantita_riordino'])
                if qr_tmp > 0:
                    qr_val = qr_tmp
        except Exception:
            qr_val = None
        # Se non trovata nella tabella estesa, recupera dalla tabella legacy
        if qr_val is None:
            try:
                qr_row = conn.execute(
                    f"SELECT quantita_riordino FROM {RIORDINO_SOGGIE_TABLE} WHERE materiale=? AND tipo=? AND spessore=?",
                    (materiale, tipo, spessore),
                ).fetchone()
                if qr_row and qr_row['quantita_riordino'] not in (None, ''):
                    qr_tmp2 = int(qr_row['quantita_riordino'])
                    if qr_tmp2 > 0:
                        qr_val = qr_tmp2
            except Exception:
                qr_val = None
        # Fallback quantita_riordino
        if qr_val is None or qr_val <= 0:
            qr_val = 1
        # Inserisci o aggiorna la combinazione nella tabella estesa
        try:
            conn.execute(
                "INSERT OR REPLACE INTO riordino_soglie_ext (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore, threshold, quantita_riordino) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (materiale, tipo, spessore, dx, dy, produttore, soglia_val, qr_val),
            )
        except sqlite3.Error:
            pass
    return {
        'status': 'ok',
        'materiale': materiale,
        'tipo': tipo,
        'spessore': spessore,
        'dimensione_x': dx,
        'dimensione_y': dy,
        'produttore': produttore,
        'soglia': soglia_val
    }


@app.route('/update_quantita_riordino', methods=['POST'])
def update_quantita_riordino():
    """
    Aggiorna la quantità di riordino per una specifica combinazione di articolo.

    A differenza della versione precedente, questa implementazione scrive
    sempre sulla tabella estesa ``riordino_soglie_ext``, la quale
    identifica in modo univoco la combinazione mediante materiale, tipo,
    spessore, dimensioni e produttore.  Se la riga esiste, la
    quantità viene aggiornata mantenendo la soglia invariata; se non
    esiste, viene creata con la soglia di default.
    """
    materiale = (request.form.get('materiale') or '').strip()
    tipo = (request.form.get('tipo') or '').strip()
    spessore = (request.form.get('spessore') or '').strip()
    dx = (request.form.get('dimensione_x') or '').strip()
    dy = (request.form.get('dimensione_y') or '').strip()
    produttore = (request.form.get('produttore') or '').strip()
    quant_raw = request.form.get('quantita')
    # Normalizziamo la quantità; se non valida o <=0 usiamo 1
    qr_val = None
    try:
        tmp = int(quant_raw) if quant_raw not in (None, '', 'None') else None
        if tmp is not None and tmp > 0:
            qr_val = tmp
    except (ValueError, TypeError):
        qr_val = None
    if qr_val is None or qr_val <= 0:
        qr_val = 1
    with get_db_connection() as conn:
        # Verifica e aggiorna lo schema della tabella estesa prima di utilizzarla
        ensure_riordino_soglie_ext_schema(conn)
        # Recupera la soglia esistente dalla tabella estesa; se assente, prova la tabella legacy
        threshold_val = None
        try:
            row_ext = conn.execute(
                "SELECT threshold FROM riordino_soglie_ext WHERE materiale=? AND tipo=? AND spessore=? AND dimensione_x=? AND dimensione_y=? AND produttore=?",
                (materiale, tipo, spessore, dx, dy, produttore),
            ).fetchone()
            if row_ext and row_ext['threshold'] not in (None, ''):
                threshold_val = int(row_ext['threshold'])
        except Exception:
            threshold_val = None
        if threshold_val is None:
            try:
                row = conn.execute(
                    f"SELECT threshold FROM {RIORDINO_SOGGIE_TABLE} WHERE materiale=? AND tipo=? AND spessore=?",
                    (materiale, tipo, spessore),
                ).fetchone()
                if row and row['threshold'] not in (None, ''):
                    threshold_val = int(row['threshold'])
            except Exception:
                threshold_val = None
        if threshold_val is None or threshold_val < 0:
            threshold_val = DEFAULT_REORDER_THRESHOLD
        # Inserisci o aggiorna il record esteso
        try:
            conn.execute(
                "INSERT OR REPLACE INTO riordino_soglie_ext (materiale, tipo, spessore, dimensione_x, dimensione_y, produttore, threshold, quantita_riordino) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (materiale, tipo, spessore, dx, dy, produttore, threshold_val, qr_val),
            )
        except sqlite3.Error:
            pass
    return {
        'status': 'ok',
        'materiale': materiale,
        'tipo': tipo,
        'spessore': spessore,
        'dimensione_x': dx,
        'dimensione_y': dy,
        'produttore': produttore,
        'quantita_riordino': qr_val
    }


# ---------------------------------------------------------------------------
# Storico Lastre
#
# Questa vista mostra la cronologia delle operazioni effettuate su ciascuna
# lastra.  Le operazioni sono raggruppate per ID lastra (padre) e mostrate
# in ordine cronologico.  È necessario il permesso "storico" per
# accedervi.

@app.route('/storico')
def storico():
    """Visualizza lo storico delle lastre (movimenti, aggiunte, rimozioni, sfridi)."""
    allowed = session.get('allowed_tabs', [])
    if 'storico' not in allowed:
        flash('Accesso non autorizzato allo storico.', 'danger')
        return redirect(url_for('dashboard'))
    # Recuperiamo tutti gli eventi dal database e li raggruppiamo per lastra
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT h.*, m.materiale AS m_materiale, m.tipo AS m_tipo, m.fornitore AS m_fornitore, m.produttore AS m_produttore, "
            "m.dimensione_x AS m_dimensione_x, m.dimensione_y AS m_dimensione_y, m.spessore AS m_spessore "
            "FROM slab_history h LEFT JOIN materiali m ON h.slab_id = m.id ORDER BY h.timestamp DESC"
        ).fetchall()
    history: dict[int, dict] = {}
    for r in rows:
        sid = r['slab_id']
        if sid not in history:
            history[sid] = {
                'id': sid,
                'materiale': r['m_materiale'],
                'tipo': r['m_tipo'],
                'fornitore': r['m_fornitore'],
                'produttore': r['m_produttore'],
                'dimensione_x': r['m_dimensione_x'],
                'dimensione_y': r['m_dimensione_y'],
                'spessore': r['m_spessore'],
                'events': []
            }
        # Costruisci stringhe di ubicazione
        from_loc = None
        if r['from_letter'] or r['from_number']:
            fl = r['from_letter'] or ''
            fn = str(r['from_number']) if r['from_number'] is not None else ''
            from_loc = f"{fl}-{fn}" if fl or fn else None
        to_loc = None
        if r['to_letter'] or r['to_number']:
            tl = r['to_letter'] or ''
            tn = str(r['to_number']) if r['to_number'] is not None else ''
            to_loc = f"{tl}-{tn}" if tl or tn else None
        ev = {
            'timestamp': r['timestamp'],
            'event_type': r['event_type'],
            'from_location': from_loc,
            'to_location': to_loc,
            'user': r['user'],
            'dimensione_x': r['dimensione_x'] or history[sid]['dimensione_x'],
            'dimensione_y': r['dimensione_y'] or history[sid]['dimensione_y'],
            'spessore': r['spessore'] or history[sid]['spessore'],
            'materiale': r['materiale'] or history[sid]['materiale'],
            'tipo': r['tipo'] or history[sid]['tipo'],
            'fornitore': r['fornitore'] or history[sid]['fornitore'],
            'produttore': r['produttore'] or history[sid]['produttore'],
            'nesting_link': r['nesting_link']
        }
        # Aggiorna i valori di base del gruppo (materiale, tipo, dimensioni, spessore, fornitore, produttore) se mancanti.
        # Quando una lastra viene rimossa dal magazzino, il join con la tabella materiali restituisce valori NULL
        # per queste colonne. Utilizziamo quindi i valori presenti nell'evento per popolare il gruppo affinché
        # l'intestazione dell'accordion mostri informazioni corrette anche per lastre rimosse.
        for key in ['dimensione_x', 'dimensione_y', 'spessore', 'materiale', 'tipo', 'fornitore', 'produttore']:
            try:
                if not history[sid].get(key) and ev.get(key):
                    history[sid][key] = ev[key]
            except Exception:
                pass
        history[sid]['events'].append(ev)
    # Converti il dizionario in lista per poter applicare filtri e ordinamenti.
    slabs = list(history.values())
    # Applica i filtri richiesti via query string.  Il filtro ID accetta
    # sia valori numerici esatti sia una stringa vuota (nessun filtro).
    id_filter_raw = (request.args.get('id_filter') or '').strip()
    try:
        id_filter = int(id_filter_raw) if id_filter_raw else None
    except Exception:
        id_filter = None
    event_filter = (request.args.get('event_type') or '').strip()
    materiale_filter = (request.args.get('materiale') or '').strip()
    tipo_filter = (request.args.get('tipo') or '').strip()
    fornitore_filter = (request.args.get('fornitore') or '').strip()
    produttore_filter = (request.args.get('produttore') or '').strip()
    spessore_filter = (request.args.get('spessore') or '').strip()
    filtered_slabs: list[dict] = []
    for slab in slabs:
        # Filtro per ID: se presente deve corrispondere esattamente al padre
        if id_filter is not None and slab.get('id') != id_filter:
            continue
        # Filtro per evento: almeno un evento deve avere il tipo richiesto
        if event_filter:
            has_evt = False
            for ev in slab.get('events', []):
                if ev.get('event_type') == event_filter:
                    has_evt = True
                    break
            if not has_evt:
                continue
        # Filtro per materiale (prefix o match parziale case insensitive)
        if materiale_filter:
            m_val = (slab.get('materiale') or '').lower()
            if materiale_filter.lower() not in m_val:
                continue
        # Filtro per tipo
        if tipo_filter:
            t_val = (slab.get('tipo') or '').lower()
            if tipo_filter.lower() not in t_val:
                continue
        # Filtro per fornitore
        if fornitore_filter:
            f_val = (slab.get('fornitore') or '').lower()
            if fornitore_filter.lower() not in f_val:
                continue
        # Filtro per produttore
        if produttore_filter:
            p_val = (slab.get('produttore') or '').lower()
            if produttore_filter.lower() not in p_val:
                continue
        # Filtro per spessore (match esatto stringa)
        if spessore_filter:
            if (slab.get('spessore') or '') != spessore_filter:
                continue
        filtered_slabs.append(slab)
    # Ordina gli ID padre in base al timestamp più recente degli eventi: i più recenti primi.
    def _latest_ts(s):
        try:
            return max(ev.get('timestamp') for ev in s.get('events', []) if ev.get('timestamp'))
        except Exception:
            return ''
    filtered_slabs.sort(key=_latest_ts, reverse=True)
    # Ordina gli eventi all'interno di ciascun gruppo dal più vecchio al più recente
    for slab in filtered_slabs:
        slab['events'].sort(key=lambda ev: ev.get('timestamp') or '')
    # Pagina i risultati: mostra 10 lastre per pagina
    per_page = 10
    try:
        page = int(request.args.get('page', '1'))
    except Exception:
        page = 1
    if page < 1:
        page = 1
    total_items = len(filtered_slabs)
    total_pages = (total_items + per_page - 1) // per_page if per_page > 0 else 1
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    # Slice in bounds
    display_slabs = filtered_slabs[start_idx:end_idx]
    return render_template(
        'storico.html',
        title='Storico Lastre',
        history=display_slabs,
        id_filter=id_filter_raw,
        event_filter=event_filter,
        materiale_filter=materiale_filter,
        tipo_filter=tipo_filter,
        fornitore_filter=fornitore_filter,
        produttore_filter=produttore_filter,
        spessore_filter=spessore_filter,
        page=page,
        total_pages=total_pages
    )


# ---------------------------------------------------------------------------
# Funzioni di amministrazione: esportazione e reset del database
#
# L'export consente di scaricare l'intero contenuto del database in vari
# formati (Excel, CSV, JSON, SQLite o un pacchetto "All" con tutti i
# formati).  L'implementazione evita l'uso di buffer binari per i CSV
# (che provocherebbe un errore ``TypeError: a bytes-like object is required``)
# utilizzando ``io.StringIO`` per scrivere i dati come testo e quindi
# convertendoli in UTF‑8 prima di inserirli negli archivi ZIP.  Per il
# pacchetto completo e la copia SQLite evitiamo l'uso di
# ``NamedTemporaryFile`` su Windows per scongiurare errori di file lock
# (WinError 32).

@app.route('/export_database')
def export_database() -> Response:
    """Esporta l'intero database in vari formati.

    Specificare il formato desiderato tramite il parametro di query
    ``fmt``. I formati supportati sono:

    - ``excel`` (default): un file XLSX con un foglio per ogni tabella (richiede
      ``pandas`` e ``openpyxl``; se non disponibili ricade in CSV).
    - ``csv``: un archivio ZIP contenente un file CSV per ciascuna tabella.
    - ``json``: un singolo file JSON con un oggetto per tabella.
    - ``sqlite``: una copia del file SQLite attualmente in uso.
    - ``all``: un archivio ZIP con il database SQLite, i file JSON, CSV e,
      se disponibili le librerie necessarie, anche un file Excel.
    """
    fmt = (request.args.get('fmt') or 'excel').lower().strip()

    # Determina il percorso del database corrente
    db_path = os.path.join(os.path.dirname(__file__), DATABASE)

    # Recupera l'elenco delle tabelle utente (escludiamo quelle interne SQLite)
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        tables: list[str] = []
        for r in rows:
            # I risultati possono essere tuple o sqlite.Row
            if isinstance(r, (list, tuple)):
                tables.append(r[0])
            else:
                tables.append(r['name'])
    finally:
        conn.close()

    def read_table(name: str):
        """Restituisce (columns, rows) per la tabella specificata.

        ``columns`` è una lista di nomi colonna.  ``rows`` è una lista di
        dizionari per ciascuna riga.  La connessione viene aperta e chiusa
        all'interno della funzione per evitare conflitti.
        """
        c = get_db_connection()
        try:
            # Preleva le informazioni sulle colonne
            col_rows = c.execute(f"PRAGMA table_info({name})").fetchall()
            cols: list[str] = []
            for row in col_rows:
                if isinstance(row, (list, tuple)):
                    cols.append(row[1])
                else:
                    # sqlite.Row: row['name'] è il nome della colonna
                    cols.append(row['name'])
            data_rows = c.execute(f"SELECT * FROM {name}").fetchall()
            data: list[dict] = []
            for item in data_rows:
                data.append(dict(item))
            return cols, data
        finally:
            c.close()

    # Esporta in formato Excel (XLSX) se possibile; se non ci sono
    # dipendenze adeguate ricade su CSV
    if fmt == 'excel':
        try:
            import pandas as pd  # type: ignore
            from openpyxl import Workbook  # type: ignore
            xbuf = io.BytesIO()
            with pd.ExcelWriter(xbuf, engine='openpyxl') as writer:
                for table in tables:
                    cols, data = read_table(table)
                    df = pd.DataFrame(data, columns=cols) if data else pd.DataFrame(columns=cols)
                    sheet_name = table[:31] if len(table) > 31 else table
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            xbuf.seek(0)
            return Response(
                xbuf.getvalue(),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers={'Content-Disposition': 'attachment; filename="magazzino.xlsx"'}
            )
        except Exception:
            # Se l'export Excel fallisce, prosegui come CSV
            fmt = 'csv'

    # Esporta in CSV: un archivio ZIP contenente un file CSV per ogni tabella
    if fmt == 'csv':
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for table in tables:
                cols, data = read_table(table)
                sio = io.StringIO(newline='')
                writer = csv.writer(sio)
                # Scrivi intestazioni
                writer.writerow(cols)
                # Scrivi dati
                for row in data:
                    writer.writerow([row.get(c) for c in cols])
                zf.writestr(f"{table}.csv", sio.getvalue().encode('utf-8'))
        zip_buf.seek(0)
        return Response(
            zip_buf.getvalue(),
            mimetype='application/zip',
            headers={'Content-Disposition': 'attachment; filename="magazzino_csv.zip"'}
        )

    # Esporta in JSON: un unico file JSON con un oggetto per tabella
    if fmt == 'json':
        payload = {}
        for table in tables:
            _, rows_data = read_table(table)
            payload[table] = rows_data
        js = json.dumps(payload, ensure_ascii=False, indent=2)
        return Response(
            js.encode('utf-8'),
            mimetype='application/json',
            headers={'Content-Disposition': 'attachment; filename="magazzino.json"'}
        )

    # Esporta in SQLite: restituisce una copia del file SQLite
    if fmt == 'sqlite':
        with open(db_path, 'rb') as f:
            blob = f.read()
        return Response(
            blob,
            mimetype='application/octet-stream',
            headers={'Content-Disposition': 'attachment; filename="magazzino.sqlite"'}
        )

    # Esporta tutto: crea un archivio ZIP con SQLite, JSON, CSV e (se possibile) Excel
    if fmt == 'all':
        all_buf = io.BytesIO()
        with zipfile.ZipFile(all_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Aggiungi il file SQLite (e i WAL/SHM se presenti)
            try:
                with open(db_path, 'rb') as f:
                    zf.writestr('magazzino.sqlite', f.read())
                wal = db_path + '-wal'
                shm = db_path + '-shm'
                if os.path.exists(wal):
                    with open(wal, 'rb') as f:
                        zf.writestr('magazzino.sqlite-wal', f.read())
                if os.path.exists(shm):
                    with open(shm, 'rb') as f:
                        zf.writestr('magazzino.sqlite-shm', f.read())
            except Exception:
                pass
            # JSON
            payload_all = {}
            for table in tables:
                _, rows_data = read_table(table)
                payload_all[table] = rows_data
            zf.writestr('magazzino.json', json.dumps(payload_all, ensure_ascii=False, indent=2).encode('utf-8'))
            # CSV per ogni tabella
            for table in tables:
                cols, data = read_table(table)
                sio = io.StringIO(newline='')
                writer = csv.writer(sio)
                writer.writerow(cols)
                for row in data:
                    writer.writerow([row.get(c) for c in cols])
                zf.writestr(f"{table}.csv", sio.getvalue().encode('utf-8'))
            # Excel, se le librerie sono disponibili
            try:
                import pandas as pd  # type: ignore
                from openpyxl import Workbook  # type: ignore
                xbio = io.BytesIO()
                with pd.ExcelWriter(xbio, engine='openpyxl') as writer:
                    for table in tables:
                        cols, data = read_table(table)
                        df = pd.DataFrame(data, columns=cols) if data else pd.DataFrame(columns=cols)
                        sheet_name = table[:31] if len(table) > 31 else table
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                xbio.seek(0)
                zf.writestr('magazzino.xlsx', xbio.getvalue())
            except Exception:
                pass
        all_buf.seek(0)
        return Response(
            all_buf.getvalue(),
            mimetype='application/zip',
            headers={'Content-Disposition': 'attachment; filename="magazzino_export_all.zip"'}
        )

    # Formato non riconosciuto
    flash('Formato di esportazione non riconosciuto.', 'warning')
    return redirect(request.referrer or url_for('accessi'))


@app.route('/reset_database', methods=['POST'])
def reset_database() -> Response:
    """Svuota completamente il database, rimuovendo tutte le tabelle (tranne gli utenti) e i file caricati.

    È richiesto che l'utente abbia accesso alla tab "accessi".  Dopo l'operazione il database
    conterrà solo la struttura vuota delle tabelle; i dizionari e gli
    articoli saranno ripristinati quando verrà inserito del nuovo
    contenuto tramite l'interfaccia.
    """
    allowed = session.get('allowed_tabs', []) or []
    if 'accessi' not in allowed:
        flash('Non hai i permessi per eseguire questa operazione.', 'danger')
        return redirect(url_for('dashboard'))
    conn = get_db_connection()
    try:
        # Recupera tutte le tabelle utente tranne quelle interne SQLite
        tbl_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        tables_to_clear: list[str] = []
        for r in tbl_rows:
            if isinstance(r, (list, tuple)):
                tables_to_clear.append(r[0])
            else:
                tables_to_clear.append(r['name'])
        for table in tables_to_clear:
            # Non cancelliamo la tabella degli utenti
            if table == USERS_TABLE:
                continue
            try:
                # Elimina tutti i record dalla tabella
                conn.execute(f"DELETE FROM {table}")
            except Exception:
                # Ignora eventuali errori se la tabella non supporta DELETE
                pass
            try:
                # Ripristina il contatore AUTOINCREMENT eliminando la voce
                # dalla tabella interna sqlite_sequence. In SQLite l'ultimo
                # valore usato per le colonne dichiarate con AUTOINCREMENT
                # viene memorizzato in questa tabella. Cancellando la
                # corrispondente riga, il successivo inserimento partirà da 1.
                conn.execute("DELETE FROM sqlite_sequence WHERE name=?", (table,))
            except Exception:
                # Se la tabella non utilizza AUTOINCREMENT oppure sqlite_sequence
                # non è presente, ignoriamo l'errore.
                pass
        conn.commit()
    finally:
        conn.close()
    # Svuota la directory uploads (documenti caricati), eliminando anche eventuali sottocartelle
    try:
        up_dir = os.path.join(os.path.dirname(__file__), 'uploads')
        if os.path.isdir(up_dir):
            for fname in os.listdir(up_dir):
                fpath = os.path.join(up_dir, fname)
                try:
                    if os.path.isfile(fpath) or os.path.islink(fpath):
                        os.remove(fpath)
                    elif os.path.isdir(fpath):
                        shutil.rmtree(fpath)
                except Exception:
                    # Ignora eventuali errori durante la rimozione
                    pass
    except Exception:
        pass
    flash('Database ripristinato con successo.', 'success')
    return redirect(url_for('accessi'))


if __name__ == '__main__':
    # Avvia il server sul'IP locale accessibile anche da altri dispositivi nella rete
    app.run(debug=True, host='0.0.0.0')


@app.route('/upload_docs_combo', methods=['POST'])
def upload_docs_combo():
    """Carica documenti associandoli a una combinazione di anagrafiche articoli.

    La combinazione completa include materiale, tipo, spessore, dimensione_x,
    dimensione_y e produttore.  Per garantire la retrocompatibilità con
    installazioni precedenti, assegniamo il campo ``material_id`` a 0 (che non
    corrisponde a nessun materiale reale) in modo da soddisfare il vincolo
    ``NOT NULL``.  I nuovi documenti sono quindi associati unicamente alla
    combinazione delle anagrafiche articoli.
    """
    materiale = (request.form.get('materiale') or '').strip()
    tipo = (request.form.get('tipo') or '').strip()
    spessore = (request.form.get('spessore') or '').strip()
    dimensione_x = (request.form.get('dimensione_x') or '').strip()
    dimensione_y = (request.form.get('dimensione_y') or '').strip()
    # Produttore facoltativo: può essere vuoto se il documento è generico per la combinazione senza produttore
    produttore = (request.form.get('produttore') or '').strip()

    files = request.files.getlist('documents')
    if not files:
        flash('Nessun file selezionato per il caricamento.', 'warning')
        # Torna alla pagina precedente se disponibile
        return redirect(request.referrer or url_for('dashboard'))

    saved_any = False
    with get_db_connection() as conn:
        for f in files:
            if not f or f.filename == '':
                continue
            # Verifica l'estensione del file (PDF/JPG/PNG)
            if not allowed_file(f.filename):
                flash(f"Estensione non permessa: {f.filename}", 'warning')
                continue
            safe_name, orig_name = save_upload_file(f)
            try:
                # Inseriamo il record specificando material_id=0 per documenti legati solo alla combinazione
                conn.execute(
                    "INSERT INTO documenti (material_id, materiale, tipo, spessore, dimensione_x, dimensione_y, produttore, filename, original_name) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        0,  # material_id placeholder per combinazioni
                        materiale,
                        tipo or None,
                        spessore or None,
                        dimensione_x or None,
                        dimensione_y or None,
                        produttore or None,
                        safe_name,
                        orig_name,
                    )
                )
                saved_any = True
            except Exception:
                # In caso di errore specifico, saltiamo senza interrompere l'intero ciclo
                pass
        if saved_any:
            try:
                conn.commit()
            except Exception:
                # Se il commit fallisce ignoriamo l'errore; i record potrebbero non essere persistiti
                pass
    if saved_any:
        flash('Documento/i caricati con successo.', 'success')
    return redirect(request.referrer or url_for('dashboard'))

# ---------------------------------------------------------------------------
# Gestione del link di nesting per una lastra
#
# Questa vista consente agli utenti autorizzati (con permesso "accessi")
# di impostare o modificare il link alla cartella di nesting associata
# a una lastra.  Il link viene salvato all'interno della tabella
# ``slab_history`` per tutte le occorrenze della lastra specificata.

@app.route('/edit_nesting/<int:slab_id>', methods=['GET', 'POST'])
def edit_nesting(slab_id: int):
    """Modifica il percorso della cartella di nesting per una lastra.

    Sono ammessi solo gli utenti che dispongono del permesso ``accessi``.
    L'URI di tipo UNC inserito (es. ``\\\\server\\cartella``) viene
    convertito in un collegamento ``file:////server/cartella`` che
    dovrebbe aprirsi in Esplora Risorse.

    :param slab_id: identificativo della lastra su cui intervenire
    :return: una pagina HTML per l'editing o un redirect dopo il salvataggio
    """
    allowed_tabs = session.get('allowed_tabs', []) or []
    # Consente l'accesso solo agli utenti con la scheda "accessi" (Admin)
    if 'accessi' not in allowed_tabs:
        flash('Accesso non autorizzato.', 'danger')
        return redirect(url_for('storico'))
    current_link: str | None = None
    # Usa una connessione dedicata per leggere e aggiornare il link
    with get_db_connection() as conn:
        try:
            row = conn.execute(
                "SELECT nesting_link FROM slab_history WHERE slab_id=? AND nesting_link IS NOT NULL ORDER BY timestamp DESC LIMIT 1",
                (slab_id,),
            ).fetchone()
            if row:
                if isinstance(row, (list, tuple)):
                    current_link = row[0]
                else:
                    current_link = row['nesting_link']
            if request.method == 'POST':
                path_raw = (request.form.get('path') or '').strip()
                new_url: str | None = None
                if path_raw:
                    # Rimuove eventuale prefisso file: o file:// inserito dall'utente
                    cleaned = path_raw
                    if cleaned.lower().startswith('file:'):
                        cleaned = cleaned.split(':', 1)[1]
                    # Rimuove slash e backslash iniziali
                    cleaned = cleaned.lstrip('/').lstrip('\\')
                    # Sostituisce backslash con slash
                    cleaned = cleaned.replace('\\', '/')
                    # Costruisce l'URI UNC con quattro slash dopo file:
                    new_url = f"file:////{cleaned}"
                conn.execute(
                    "UPDATE slab_history SET nesting_link=? WHERE slab_id=?",
                    (new_url, slab_id),
                )
                conn.commit()
                flash('Link di nesting aggiornato.', 'success')
                return redirect(url_for('storico'))
        except Exception:
            pass
    return render_template('edit_nesting.html', nesting_link=current_link)

