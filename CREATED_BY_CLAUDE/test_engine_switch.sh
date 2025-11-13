#!/bin/bash

echo "==================================="
echo "KPI查询引擎切换测试脚本"
echo "==================================="
echo ""

# 测试当前引擎
echo "1. 测试当前引擎信息"
echo "---"
curl -s http://localhost:8080/api/kpi/engineInfo | python3 -m json.tool
echo ""
echo ""

# 测试查询功能
echo "2. 测试KPI查询功能"
echo "---"
curl -s -X POST http://localhost:8080/api/kpi/queryKpiData \
  -H "Content-Type: application/json" \
  -d '{
    "kpiArray": ["KD1002"],
    "opTimeArray": ["20251101"],
    "dimCodeArray": ["county_id"],
    "dimConditionArray": [{"dimConditionCode": "county_id", "dimConditionVal": "4"}],
    "includeHistoricalData": true,
    "includeTargetData": false
  }' | python3 -c "import sys, json; data = json.load(sys.stdin); print(f\"KPI: {data['dataArray'][0]['kpi_id']}\"); print(f\"Current: {data['dataArray'][0]['current']}\"); print(f\"Status: {data['status']}\")"
echo ""
echo ""

# 切换到SQLite引擎
echo "3. 切换引擎配置 (仅演示，实际需要重启服务)"
echo "---"
echo "当前配置: metrics.engine.type=MySQL"
echo "要切换到SQLite引擎，需要:"
echo "  1. 修改 application.properties:"
echo "     metrics.engine.type=SQLite"
echo "  2. 重启服务"
echo "  3. 访问 http://localhost:8080/api/kpi/engineInfo 验证"
echo ""
echo ""

# 验证配置方法
echo "4. 查看引擎配置"
echo "---"
grep "metrics.engine.type" src/main/resources/application.properties
echo ""

echo "==================================="
echo "测试完成!"
echo "==================================="
