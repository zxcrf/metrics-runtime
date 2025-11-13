#!/bin/bash

# 测试指标计算与存储API
# 用法: ./test_kpi_compute.sh

API_URL="http://localhost:8080/api/open/kpi/srcTableComplete"

echo "=========================================="
echo "测试指标计算与存储API"
echo "=========================================="
echo ""

# 测试1：计算用户行为数据指标
echo "测试1：计算用户行为数据指标"
echo "请求参数："
echo '{'
echo '  "tableName": "user_behavior",'
echo '  "opTime": "20251112"'
echo '}'
echo ""

curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "tableName": "user_behavior",
    "opTime": "20251112"
  }' \
  $API_URL | jq .

echo ""
echo "=========================================="
echo ""

# 测试2：计算业务交易数据指标
echo "测试2：计算业务交易数据指标"
echo "请求参数："
echo '{'
echo '  "tableName": "transaction",'
echo '  "opTime": "20251112"'
echo '}'
echo ""

curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "tableName": "transaction",
    "opTime": "20251112"
  }' \
  $API_URL | jq .

echo ""
echo "=========================================="
echo "测试完成"
echo "=========================================="
