#!/bin/bash
# Script de compilacao completa do projeto E.D.E.N (Servidor + Cluster + Cliente)

set -e

echo "=== COMPILANDO E.D.E.N. (Sistemas Distribuidos - Trabalho 02) ==="

mkdir -p Servidor/bin Cliente/bin

echo "1. Compilando Servidor e Cluster..."
javac -d Servidor/bin Servidor/Protocol/*.java Servidor/utils/*.java Servidor/controller/*.java Servidor/model/*.java Servidor/cluster/*.java Servidor/*.java

echo "2. Compilando Cliente JavaFX..."
javac -d Cliente/bin Cliente/Protocol/*.java Cliente/utils/*.java Cliente/exceptions/*.java Cliente/model/*.java Cliente/view/*.java Cliente/*.java

echo "Compilacao concluida com sucesso!"

