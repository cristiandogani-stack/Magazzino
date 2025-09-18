@echo on
setlocal EnableExtensions
title Push Magazzino (forzato, con log)
color 0A

:: LOG nel percorso della cartella corrente
set "LOG=%~dp0push_log.txt"
echo =============================================================>>"%LOG%"
echo Avvio: %date% %time% >>"%LOG%"

echo.
echo [1/8] Verifico che Git sia installato...
where git >>"%LOG%" 2>&1
if errorlevel 1 (
    echo ERRORE: Git non trovato nel PATH. Installa "Git for Windows" da https://git-scm.com/download/win
    echo ERRORE: Git non trovato nel PATH. >>"%LOG%"
    echo.
    pause
    exit /b 1
)

echo.
echo [2/8] Vado nella cartella del progetto...
cd /d C:\Users\cristian.dogani\Desktop\magazzino_cd >>"%LOG%" 2>&1
if errorlevel 1 (
    echo ERRORE: impossibile entrare nella cartella C:\Users\cristian.dogani\Desktop\magazzino_cd
    echo ERRORE: cd fallita. >>"%LOG%"
    echo.
    pause
    exit /b 1
)

echo.
echo [3/8] Config utente minima (puoi cambiare email)...
git config user.name "cristiandogani-stack" >>"%LOG%" 2>&1
git config user.email "cristian.dogani@tondimeccanica.it"     >>"%LOG%" 2>&1

echo.
echo [4/8] Inizializzo repo se manca...
if not exist ".git" (
    git init >>"%LOG%" 2>&1
    git branch -M main >>"%LOG%" 2>&1
    git remote add origin https://github.com/cristiandogani-stack/Magazzino.git >>"%LOG%" 2>&1
) else (
    rem Se l'origin non esiste lo aggiungo
    git remote get-url origin >>"%LOG%" 2>&1
    if errorlevel 1 (
        git remote add origin https://github.com/cristiandogani-stack/Magazzino.git >>"%LOG%" 2>&1
    )
)

echo.
echo [5/8] Aggiorno riferimenti remoti (fetch)...
git fetch origin >>"%LOG%" 2>&1

echo.
echo [6/8] Aggiungo file e creo commit (ok anche se nulla da committare)...
git add . >>"%LOG%" 2>&1
git commit -m "Aggiornamento forzato del %date% %time%" >>"%LOG%" 2>&1

echo.
echo [7/8] Push con --force-with-lease (sicuro)...
git push -u origin main --force-with-lease
if errorlevel 1 (
    echo.
    echo Attenzione: push rifiutato (stale info o divergenze). Vedi log: "%LOG%"
    echo.
    choice /M "Vuoi forzare completamente il push con --force (SOVRASCRIVE IL REMOTO)?"
    if errorlevel 2 (
        echo Operazione annullata dall'utente. >>"%LOG%"
        echo.
        pause
        exit /b 1
    )
    echo [8/8] Push con --force (sovrascrive il remoto)...
    git push -u origin main --force
    if errorlevel 1 (
        echo ERRORE: push forzato fallito. Controlla rete/permessi/branch. >>"%LOG%"
        echo.
        pause
        exit /b 1
    )
)

echo.
echo ✅ Operazione completata. Log: "%LOG%"
echo.
pause
endlocal
