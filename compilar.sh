#!/bin/bash
# Script de compilacao completa do projeto E.D.E.N (Servidor + Cluster/Shards + Cliente)

set -e

echo "=== COMPILANDO E.D.E.N. (Sistemas Distribuidos - Trabalho 03) ==="

# Remove eventuais arquivos .class soltos fora de bin/ para evitar sombreamento de versoes antigas
find Servidor Cliente -name "*.class" -not -path "*/bin/*" -type f -delete 2>/dev/null || true

mkdir -p Servidor/bin Cliente/bin

echo "1. Compilando Servidor, Shards e Cluster..."
javac -d Servidor/bin Servidor/Protocol/*.java Servidor/utils/*.java Servidor/controller/*.java Servidor/model/*.java Servidor/cluster/*.java Servidor/*.java

echo "2. Compilando Cliente JavaFX..."
javac -d Cliente/bin Cliente/Protocol/*.java Cliente/utils/*.java Cliente/exceptions/*.java Cliente/model/*.java Cliente/view/*.java Cliente/*.java
mkdir -p Cliente/bin/view
cp -f Cliente/style.css Cliente/bin/style.css 2>/dev/null || true
cp -f Cliente/view/edenIcon.png Cliente/bin/view/edenIcon.png 2>/dev/null || true

echo "Compilacao concluida com sucesso!"
