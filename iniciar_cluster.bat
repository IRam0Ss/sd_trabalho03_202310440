@echo off
REM ============================================================================
REM E.D.E.N. (T#03) - Inicializador Automatico de 6 Processos para Windows
REM 3 Shards ([A..I], [J..R], [S..Z]) x 2 Replicas (1 Ativo + 1 Standby)
REM
REM Uso:
REM   iniciar_cluster.bat          (Modo padrao com bind 0.0.0.0)
REM   iniciar_cluster.bat --rede   (Modo Rede Fisica LAN / Wi-Fi)
REM ============================================================================

setlocal
cd /d "%~dp0"

set MODO_FLAG=
if /I "%~1"=="--rede" set MODO_FLAG=--rede
if /I "%~1"=="--lan" set MODO_FLAG=--rede

if not exist "Servidor\bin\Principal.class" (
    echo [SETUP] Compilando Servidor e Cliente antes de iniciar o cluster...
    if not exist "Servidor\bin" mkdir "Servidor\bin"
    if not exist "Cliente\bin\view" mkdir "Cliente\bin\view"
    javac -encoding UTF-8 -d Servidor\bin Servidor\Protocol\*.java Servidor\utils\*.java Servidor\controller\*.java Servidor\model\*.java Servidor\cluster\*.java Servidor\*.java
    if errorlevel 1 (
        echo [ERRO] Falha na compilacao do Servidor!
        pause
        exit /b 1
    )
    javac -encoding UTF-8 -d Cliente\bin Cliente\Protocol\*.java Cliente\utils\*.java Cliente\exceptions\*.java Cliente\model\*.java Cliente\view\*.java Cliente\*.java
    copy /Y "Cliente\style.css" "Cliente\bin\style.css" >nul 2>&1
    copy /Y "Cliente\view\edenIcon.png" "Cliente\bin\view\edenIcon.png" >nul 2>&1
)

echo ==========================================================================
echo   E.D.E.N. (T#03) - INICIANDO CLUSTER PARTICIONADO (6 PROCESSOS)
echo ==========================================================================

echo [1/2] Abrindo 3 primeiros nós (Lideres Pioneiros dos Shards)...
start "EDEN No 1 [S1 J..R] (Zero-Config)" cmd /k "java -cp Servidor/bin Principal %MODO_FLAG%"
timeout /t 1 /nobreak >nul
start "EDEN No 2 [S2 S..Z] (Zero-Config)" cmd /k "java -cp Servidor/bin Principal %MODO_FLAG%"
timeout /t 1 /nobreak >nul
start "EDEN No 3 [S0 A..I] (Zero-Config)" cmd /k "java -cp Servidor/bin Principal %MODO_FLAG%"

timeout /t 1 /nobreak >nul

echo [2/2] Abrindo 3 nós seguintes (Replicas Standby dos Shards)...
start "EDEN No 4 [S1 J..R] (Zero-Config)" cmd /k "java -cp Servidor/bin Principal %MODO_FLAG%"
timeout /t 1 /nobreak >nul
start "EDEN No 5 [S2 S..Z] (Zero-Config)" cmd /k "java -cp Servidor/bin Principal %MODO_FLAG%"
timeout /t 1 /nobreak >nul
start "EDEN No 6 [S0 A..I] (Zero-Config)" cmd /k "java -cp Servidor/bin Principal %MODO_FLAG%"

echo --------------------------------------------------------------------------
echo  6 janelas iniciadas com sucesso! Digite 'status' em qualquer terminal.
echo  Para encerrar todas as janelas de uma vez, execute: parar_cluster.bat
echo ==========================================================================
endlocal

