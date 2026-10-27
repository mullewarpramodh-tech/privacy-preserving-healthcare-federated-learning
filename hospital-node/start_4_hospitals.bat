@echo off
echo ============================================================
echo   LAUNCHING 4 DECENTRALIZED HOSPITAL EDGE CLIENTS
echo ============================================================

start "Hospital 1 - Apollo Hyderabad" cmd /k "python main.py 1 "Apollo Hospital Hyderabad""
timeout /t 2 >nul

start "Hospital 2 - AIIMS New Delhi" cmd /k "python main.py 2 "AIIMS New Delhi""
timeout /t 2 >nul

start "Hospital 3 - Fortis Bangalore" cmd /k "python main.py 3 "Fortis Healthcare Bangalore""
timeout /t 2 >nul

start "Hospital 4 - Tata Memorial Mumbai" cmd /k "python main.py 4 "Tata Memorial Hospital Mumbai""

echo All 4 Hospital Edge Nodes are running and connected to Kafka!