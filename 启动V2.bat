@echo off
cd /d "%~dp0"
title ClinePass V2
echo Starting ClinePass console V2 ...
echo Keep this window open while using it. Close this window to STOP the service.
echo.
rem Loopback only by default. For LAN or phone access, set a proxy key in the console first,
rem then uncomment the next line:
rem set BIND_HOST=0.0.0.0
node server-v2.js
echo.
echo Service stopped. (If it says port 3123 is in use, the service is already running.)
pause
