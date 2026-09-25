@echo off
title Subir Bot para o GitHub
chcp 65001 > nul

echo ======================================================================
echo   ENVIANDO MELI INTELLIGENCE BOT PARA O SEU GITHUB
echo ======================================================================
echo.
echo Certifique-se de que criou o repositório 'meli-intelligence-bot' em:
echo https://github.com/new
echo.

git remote remove origin 2>nul
git remote add origin https://github.com/karl-albert/meli-intelligence-bot.git
git branch -M main
git add .
git commit -m "Update bot files" 2>nul
git push -u origin main

echo.
if %ERRORLEVEL% EQU 0 (
    echo ======================================================================
    echo   [SUCESSO] Código enviado com sucesso para o seu GitHub!
    echo   Link: https://github.com/karl-albert/meli-intelligence-bot
    echo ======================================================================
) else (
    echo [!] Se pediu login, autentique com seu token ou senha do GitHub.
)
echo.
pause
