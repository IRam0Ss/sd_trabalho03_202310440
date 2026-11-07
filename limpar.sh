#!/bin/bash
# Script para limpar arquivos compilados (.class), diretorios bin/ e logs temporarios do projeto E.D.E.N.

echo "Limpando arquivos compilados (.class) e artefatos temporarios..."
find . -name "*.class" -type f -delete 2>/dev/null
rm -rf Cliente/bin Servidor/bin testes/bin 2>/dev/null
rm -f logs_cluster/*.log .cluster_pids 2>/dev/null

echo "Concluido! Diretorios de build e logs temporarios foram removidos com sucesso."

