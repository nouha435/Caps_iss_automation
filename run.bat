@echo off
echo ===================================================
echo   Automatisation CB2C — Tous les cas
echo ===================================================

echo === Deploiement ===
docker cp %~dp0. CAPS-unix-3.5.4-RC2:/pwrcard/home/automation
docker exec -u root CAPS-unix-3.5.4-RC2 bash -c "chown -R pwrcard /pwrcard/home/automation"

echo === Nettoyage rapports ===
docker exec -u root CAPS-unix-3.5.4-RC2 bash -c "rm -rf /pwrcard/home/automation/reports/allure/*"

echo === Lancement cas1 ===
docker exec -u pwrcard CAPS-unix-3.5.4-RC2 bash -c "cd /pwrcard/home/automation && LD_PRELOAD=/usr/lib64/libexpat.so.1 python3 main_cb2c.py --cas cas1"

echo === Lancement cas2 ===
docker exec -u pwrcard CAPS-unix-3.5.4-RC2 bash -c "cd /pwrcard/home/automation && LD_PRELOAD=/usr/lib64/libexpat.so.1 python3 main_cb2c.py --cas cas2"

echo === Lancement cas3 ===
docker exec -u pwrcard CAPS-unix-3.5.4-RC2 bash -c "cd /pwrcard/home/automation && LD_PRELOAD=/usr/lib64/libexpat.so.1 python3 main_cb2c.py --cas cas3"

echo === Generation rapport final — apres les deux cas ===
docker exec -u pwrcard CAPS-unix-3.5.4-RC2 bash -c "cd /pwrcard/home/automation && allure generate reports/allure -o reports/allure-html --clean"

echo === Recuperation rapport ===
if exist "%~dp0reports\allure-html" rmdir /s /q "%~dp0reports\allure-html"
docker cp CAPS-unix-3.5.4-RC2:/pwrcard/home/automation/reports/allure-html %~dp0reports\allure-html

echo === Ouverture rapport ===
start python -m http.server 8081 --directory %~dp0reports\allure-html
timeout /t 2
start http://localhost:8081

echo ===================================================
echo   Done ! Rapport disponible sur localhost:8081
echo ===================================================
pause