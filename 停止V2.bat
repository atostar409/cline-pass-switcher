@echo off
title ClinePass V2 - STOP
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'node.exe' -and $_.CommandLine -like '*server-v2*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
echo ClinePass V2 service stopped. (SillyTavern is NOT touched.)
pause
