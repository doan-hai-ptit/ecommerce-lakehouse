#!/bin/bash

# Thư mục chứa script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
JSON_FILE="$DIR/../database/debezium_postgres_connector.json"

echo "Đang đăng ký Debezium PostgreSQL Connector..."
curl -i -X POST -H "Content-Type: application/json" \
  -d @"$JSON_FILE" \
  http://localhost:8083/connectors

echo -e "\n\nKiểm tra trạng thái hoạt động:"
curl -s http://localhost:8083/connectors/ecommerce-postgres-connector/status | grep -o '"state":"[^"]*"'
echo ""
