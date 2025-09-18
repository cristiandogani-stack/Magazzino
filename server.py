"""
server.py – avvia l'applicazione Flask utilizzando un server WSGI
di produzione.

Questo modulo importa l'istanza `app` definita in ``app.py`` e la
esegue attraverso Waitress, un server WSGI affidabile e leggero che
gestisce correttamente più connessioni simultanee. L'utilizzo di
Waitress evita di ricorrere al server di sviluppo integrato in Flask e
consente di esporre l'applicazione in modalità HTTP senza il supporto
HTTPS, soddisfacendo la necessità di un server operativo capace di
gestire più connessioni fisse.

Per avviare manualmente il server dalla riga di comando:

    python server.py

Assicurati che la dipendenza ``waitress`` sia installata (è
inclusa in ``requirements.txt``).  Sulle piattaforme Windows e
Linux/macOS sono disponibili gli script ``start_server.bat`` e
``start_server.sh`` che installano le dipendenze ed avviano questo
modulo.
"""

from waitress import serve  # type: ignore

# Importa l'applicazione Flask dall'app principale
from app import app


def main() -> None:
    """Punto di ingresso principale per avviare il server HTTP.

    Questa funzione utilizza ``waitress.serve`` per esporre
    l'applicazione Flask su tutte le interfacce disponibili (``0.0.0.0``)
    sulla porta 5000. Waitress gestisce il pooling dei thread
    automaticamente e mantiene aperto il server anche con molte
    connessioni simultanee.  Non viene fornito alcun contesto SSL,
    quindi il server opera esclusivamente in HTTP.
    """
    # Configura Waitress per ascoltare su tutte le interfacce alla porta 5000.
    serve(app, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()
