@echo off
REM ----------------------------------------------------------
REM File di avvio per l'applicazione magazzino su Windows
REM Installa automaticamente le dipendenze se mancano e avvia il server
REM ----------------------------------------------------------


echo Installazione dipendenze...
pip install -r requirements.txt

REM Avvia il server in modalità HTTPS tramite il modulo https_server.py
echo Avvio del server di produzione (HTTPS)...
python https_server.py

pause