@echo off
REM ============================================================================
REM E.D.E.N. (T#03) - Encerra as 6 janelas do Cluster no Windows
REM ============================================================================

echo === Encerrando janelas do cluster E.D.E.N. ===
taskkill /FI "WINDOWTITLE eq EDEN No *" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq EDEN *" /F >nul 2>&1
echo Processos do cluster encerrados.

