@echo off
chcp 65001 >nul
cd /d "%~dp0"
title PolymarketFarm

:: Kill any process already on port 8000
for /f "tokens=5" %%a in ('netstat -aon ^| find "0.0.0.0:8000" 2^>nul') do (
    taskkill /F /PID %%a >nul 2>&1
)
for /f "tokens=5" %%a in ('netstat -aon ^| find "127.0.0.1:8000" 2^>nul') do (
    taskkill /F /PID %%a >nul 2>&1
)

echo.
echo  PolymarketFarm starting...
echo  Dashboard: http://localhost:8000
echo  Close this window to stop the bot.
echo.

start /min "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8000"

.venv\Scripts\python.exe -m uvicorn src.main:app --port 8000

echo.
echo  Server stopped.
pause
