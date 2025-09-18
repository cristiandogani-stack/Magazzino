@echo off
setlocal

cd /d C:\Users\cristian.dogani\Desktop\magazzino_cd

:: Config base (solo la prima volta; modifica l'email se serve)
git config user.name "cristiandogani-stack" 1>nul 2>nul
git config user.email "you@example.com" 1>nul 2>nul

:: Inizializza repo se non esiste
if not exist ".git" (
    echo Inizializzazione repository Git...
    git init
    git branch -M main
    git remote add origin https://github.com/cristiandogani-stack/Magazzino.git
)

echo.
echo === Aggiorno riferimenti remoti (fetch) ===
git fetch origin

:: Aggiungi tutto e crea commit (ok anche se non ci sono modifiche)
git add .
git commit -m "Aggiornamento forzato del %date% %time%" 2>nul

echo.
echo === Push con force-with-lease (sicuro) ===
git push -u origin main --force-with-lease
if %errorlevel% EQU 0 goto :OK

echo.
echo *** Push rifiutato (stale info o divergenze persistenti). ***
echo Se sei SICURO di voler sovrascrivere il remoto ignorando il lease,
set /p CONF=digita "YES" per forzare completamente (--force): 
if /I "%CONF%" NEQ "YES" goto :END

echo.
echo === Push con --force (sovr
