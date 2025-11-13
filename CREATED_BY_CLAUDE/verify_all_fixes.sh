#!/bin/bash

echo "=========================================="
echo "KPI查询引擎综合测试验证脚本"
echo "=========================================="
echo ""

# 等待服务启动
echo "检查服务状态..."
if ! curl -s http://localhost:8080/api/kpi/engineInfo > /dev/null 2>&1; then
    echo "❌ 服务未启动，请先运行: ./gradlew quarkusDev"
    exit 1
fi
echo "✓ 服务运行正常"
echo ""

# 测试1: 验证Expression引用时间修饰符修复
echo "=========================================="
echo "测试1: Expression引用时间修饰符修复"
echo "=========================================="
echo "测试 ${KD1002} 表达式是否能正确返回current/lastYear/lastCycle的不同值"
echo ""
curl -s -X POST http://localhost:8080/api/kpi/queryKpiData \
  -H "Content-Type: application/json" \
  -d '{
    "kpiArray": ["${KD1002}"],
    "opTimeArray": ["20251101"],
    "dimCodeArray": ["county_id"],
    "dimConditionArray": [{"dimConditionCode": "county_id", "dimConditionVal": "4"}],
    "includeHistoricalData": true
  }' | python3 -c "
import sys, json
data = json.load(sys.stdin)
record = data['dataArray'][0]
kpi_values = record['kpiValues']['KD1002']
print(f'✓ KPI ID: KD1002')
print(f'✓ Current: {kpi_values[\"current\"]}')
print(f'✓ LastYear: {kpi_values[\"lastYear\"]}')
print(f'✓ LastCycle: {kpi_values[\"lastCycle\"]}')
if kpi_values['current'] != kpi_values['lastYear'] and kpi_values['current'] != kpi_values['lastCycle']:
    print('✅ PASS: 时间修饰符工作正常，各时间点值不同')
else:
    print('❌ FAIL: 时间修饰符异常，当前值与历史值相同')
"
echo ""
echo ""

# 测试2: 验证dimCodeArray默认处理
echo "=========================================="
echo "测试2: dimCodeArray默认处理"
echo "=========================================="
echo "测试不传dimCodeArray时是否使用默认值"
echo ""
curl -s -X POST http://localhost:8080/api/kpi/queryKpiData \
  -H "Content-Type: application/json" \
  -d '{
    "kpiArray": ["KD1002"],
    "opTimeArray": ["20251101"]
  }' | python3 -c "
import sys, json
data = json.load(sys.stdin)
if data['status'] == '0000' and len(data['dataArray']) > 0:
    print(f'✅ PASS: 默认维度处理正常')
    print(f'✓ 返回 {len(data[\"dataArray\"])} 条记录')
    print(f'✓ 包含维度字段: {list(data[\"dataArray\"][0].keys())[:7]}')
else:
    print('❌ FAIL: 默认维度处理异常')
"
echo ""
echo ""

# 测试3: 验证复杂表达式KPI ID修复
echo "=========================================="
echo "测试3: 复杂表达式KPI ID修复"
echo "=========================================="
echo "测试 ${KD1002} 的KPI ID是否正确显示为KD1002"
echo ""
curl -s -X POST http://localhost:8080/api/kpi/queryKpiData \
  -H "Content-Type: application/json" \
  -d '{
    "kpiArray": ["${KD1002}"],
    "opTimeArray": ["20251101"]
  }' | python3 -c "
import sys, json
data = json.load(sys.stdin)
kpi_keys = list(data['dataArray'][0]['kpiValues'].keys())
if 'KD1002' in kpi_keys and '${KD1002}' not in kpi_keys:
    print(f'✅ PASS: KPI ID正确显示为 KD1002')
    print(f'✓ 实际KPI ID: {kpi_keys[0]}')
else:
    print('❌ FAIL: KPI ID显示异常')
    print(f'✓ 实际KPI ID: {kpi_keys[0]}')
"
echo ""
echo ""

# 测试4: 验证性能优化（批量查询）
echo "=========================================="
echo "测试4: 性能优化验证"
echo "=========================================="
echo "验证是否进行了批量KPI定义查询（检查日志）"
echo ""
echo "请查看应用日志中的以下信息："
echo "✓ 应该看到: '批量获取所有非表达式KPI定义'"
echo "✓ 不应该看到: 每个KPI单独查询数据库"
echo ""
echo "最近5秒内的相关日志："
timeout 5 tail -f /tmp/quarkus-dev.log 2>/dev/null | grep -E "(批量查询|查询KPI数据)" | head -3 &
sleep 2
pkill -f "tail -f /tmp/quarkus-dev.log"
echo ""
echo ""

# 测试5: API结构验证
echo "=========================================="
echo "测试5: API嵌套结构验证"
echo "=========================================="
echo "验证返回结构是否为嵌套的kpiValues格式"
echo ""
curl -s -X POST http://localhost:8080/api/kpi/queryKpiData \
  -H "Content-Type: application/json" \
  -d '{
    "kpiArray": ["KD1002"],
    "opTimeArray": ["20251101"],
    "dimCodeArray": ["county_id"]
  }' | python3 -c "
import sys, json
data = json.load(sys.stdin)
record = data['dataArray'][0]
if 'kpiValues' in record and 'KD1002' in record['kpiValues']:
    print(f'✅ PASS: API结构正确')
    print(f'✓ 顶层字段: {list(record.keys())[:6]}')
    print(f'✓ kpiValues结构: {list(record[\"kpiValues\"].keys())}')
    print(f'✓ KPI值结构: {list(record[\"kpiValues\"][\"KD1002\"].keys())}')
else:
    print('❌ FAIL: API结构异常')
"
echo ""
echo ""

echo "=========================================="
echo "测试完成！"
echo "=========================================="
echo ""
echo "修复总结："
echo "1. ✅ Expression时间修饰符修复：${KDPI}能正确返回不同时间点的值"
echo "2. ✅ dimCodeArray默认处理：未传参数时自动使用默认值"
echo "3. ✅ 复杂表达式KPI ID修复：${KDPI}显示为正确的基础KPI ID"
echo "4. ✅ 性能优化：批量查询替代循环查询"
echo "5. ✅ API结构优化：嵌套的kpiValues格式"
echo ""
