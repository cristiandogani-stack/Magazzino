@echo off
cd /d C:\Users\cristian.dogani\Desktop\magazzino_cd

:: Inizializza git se non esiste ancora
if not exist ".git" (
    echo Inizializzazione repository Git...
    git init
    git branch -M main
    git remote add origin https://github.com/cristiandogani-stack/Magazzino.git
)

:: Aggiungi tutti i file
git add .

:: Commit con data/ora
git commit -m "Aggiornamento forzato del %date% %time%" 2>nul

:: Forza il push, sovrascrivendo il remoto
git push -u origin main --force-with-lease

pause
