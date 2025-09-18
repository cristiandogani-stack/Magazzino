@echo off
cd /d C:\Users\cristian.dogani\Desktop\magazzino_cd

:: Se è la prima volta, inizializza il repo
if not exist ".git" (
    echo Inizializzazione repository Git...
    git init
    git branch -M main
    git remote add origin https://github.com/cristiandogani-stack/Magazzino.git
)

:: Aggiungi tutti i file
git add .

:: Commit con data e ora per tener traccia delle versioni
git commit -m "Aggiornamento del %date% %time%"

:: Invia su GitHub
git push -u origin main

pause
