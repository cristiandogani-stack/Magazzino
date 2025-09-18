#!/bin/bash
# ----------------------------------------------------------
# Script di avvio per l'applicazione magazzino su Linux/macOS
# Installa automaticamente le dipendenze (se non presenti) e avvia il server
# ----------------------------------------------------------

set -e
echo "Installazione dipendenze..."
pip3 install --user -r requirements.txt

echo "Avvio del server di produzione (HTTPS)..."
# Avvia il server utilizzando il server WSGI multithread con SSL definito in https_server.py.
python3 https_server.py