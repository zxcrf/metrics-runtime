#!/bin/bash
# 缓存性能对比测试脚本
# 用于测试启用/禁用缓存时的RPS性能差异

echo "=== KPI 查询缓存性能测试 ==="
echo ""

# 测试配置
ENDPOINT="http://localhost:8080/api/v2/kpi/queryKpiData"
THREADS=6
CONNECTIONS=100
DURATION="30s"
POST_LUA="post.lua"

# 场景1：所有缓存禁用
echo "📊 场景1: 所有缓存禁用 (无缓存)"
echo "----------------------------------------"
echo "重启服务（禁用所有缓存）..."
rm -rf /tmp/cache/*
kill $(lsof -ti:8080)
sleep 2
export KPI_CACHE_L1_ENABLED=false
export KPI_CACHE_L2_ENABLED=false
export KPI_CACHE_L3_ENABLED=false
java -jar build/quarkus-app/quarkus-run.jar > server_no_cache.log 2>&1 &
sleep 5

echo "运行压测..."
wrk -t${THREADS} -c${CONNECTIONS} -d${DURATION} -s ${POST_LUA} --latency ${ENDPOINT} | tee result_cache_disabled.txt
echo ""


# 场景2：所有缓存启用（默认）
echo "📊 场景2: 所有缓存启用 (L1+L2+L3)"
echo "----------------------------------------"
echo "启动服务（所有缓存启用）..."
rm -rf /tmp/cache/*
kill $(lsof -ti:8080)
sleep 2
export KPI_CACHE_L1_ENABLED=true
export KPI_CACHE_L2_ENABLED=true
export KPI_CACHE_L3_ENABLED=true
java -jar build/quarkus-app/quarkus-run.jar > server.log 2>&1 &
sleep 5

echo "运行压测..."
wrk -t${THREADS} -c${CONNECTIONS} -d${DURATION} -s ${POST_LUA} --latency ${ENDPOINT} | tee result_cache_enabled.txt
echo ""
sleep 2



# 场景3：仅禁用L1+L2（保留L3文件缓存）
echo "📊 场景3: 仅禁用 L1+L2 缓存 (保留L3文件缓存)"
echo "----------------------------------------"
echo "重启服务（仅L3缓存）..."
rm -rf /tmp/cache/*
kill $(lsof -ti:8080)
sleep 2
export KPI_CACHE_L1_ENABLED=false
export KPI_CACHE_L2_ENABLED=false
export KPI_CACHE_L3_ENABLED=true
java -jar build/quarkus-app/quarkus-run.jar > server_l3_only.log 2>&1 &
sleep 5

echo "运行压测..."
wrk -t${THREADS} -c${CONNECTIONS} -d${DURATION} -s ${POST_LUA} --latency ${ENDPOINT} | tee result_l3_only.txt
echo ""




# 场景4：仅禁用L1 (保留L2+L3缓存)
echo "📊 场景4: 仅禁用 L1 缓存 (保留L2+L3缓存)"
echo "----------------------------------------"
echo "重启服务（仅L2+L3缓存）..."
rm -rf /tmp/cache/*
kill $(lsof -ti:8080)
sleep 2
export KPI_CACHE_L1_ENABLED=false
export KPI_CACHE_L2_ENABLED=true
export KPI_CACHE_L3_ENABLED=true
java -jar build/quarkus-app/quarkus-run.jar > server_l2_l3_only.log 2>&1 &
sleep 5

echo "运行压测..."
wrk -t${THREADS} -c${CONNECTIONS} -d${DURATION} -s ${POST_LUA} --latency ${ENDPOINT} | tee result_l2_l3_only.txt
echo ""

kill $(lsof -ti:8080)
sleep 2

echo "=== 测试完成 ==="
echo "结果文件："
echo "  - result_cache_enabled.txt (所有缓存启用)"
echo "  - result_cache_disabled.txt (所有缓存禁用)"
echo "  - result_l3_only.txt (仅L3缓存)"
echo "  - result_l2_l3_only.txt (仅L2+L3缓存)"