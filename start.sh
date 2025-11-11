#!/bin/bash

# DataOS Metrics Runtime 快速启动脚本

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║          DataOS Metrics Runtime - 快速启动                     ║"
echo "║                                                                ║"
echo "║  Quarkus + JDK 21 虚拟线程 + SQLite                            ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检查Java环境
if ! command -v java &> /dev/null; then
    echo -e "${RED}错误: 未找到Java，请先安装JDK 21+${NC}"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 21 ]; then
    echo -e "${RED}错误: 需要JDK 21+，当前版本: $(java -version 2>&1 | head -n 1)${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Java环境检查通过: $(java -version 2>&1 | head -n 1)"

# 检查Gradle
if ! command -v gradle &> /dev/null && ! command -v ./gradlew &> /dev/null; then
    echo -e "${YELLOW}警告: 未找到Gradle，将使用包装器${NC}"
    if [ ! -f "./gradlew" ]; then
        echo -e "${RED}错误: 请安装Gradle或下载gradlew包装器${NC}"
        exit 1
    fi
    GRADLE_CMD="./gradlew"
else
    GRADLE_CMD="gradle"
fi

echo -e "${GREEN}✓${NC} Gradle环境检查通过"

# 检查Docker
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}警告: 未找到Docker，将跳过服务启动${NC}"
    SKIP_DOCKER=true
else
    echo -e "${GREEN}✓${NC} Docker环境检查通过"
    SKIP_DOCKER=false
fi

# 启动依赖服务
if [ "$SKIP_DOCKER" = false ]; then
    echo ""
    echo -e "${BLUE}启动依赖服务 (PostgreSQL, Redis, MinIO)...${NC}"
    docker-compose up -d postgres redis minio

    # 等待服务启动
    echo -e "${YELLOW}等待服务启动...${NC}"
    sleep 10

    # 检查服务状态
    if docker-compose ps | grep -q "Up"; then
        echo -e "${GREEN}✓ 依赖服务启动成功${NC}"
    else
        echo -e "${RED}错误: 依赖服务启动失败${NC}"
        docker-compose logs
        exit 1
    fi
fi

# 编译项目
echo ""
echo -e "${BLUE}编译项目...${NC}"
$GRADLE_CMD clean build -x test

if [ $? -ne 0 ]; then
    echo -e "${RED}错误: 编译失败${NC}"
    exit 1
fi

echo -e "${GREEN}✓ 项目编译成功${NC}"

# 启动应用
echo ""
echo -e "${BLUE}启动应用...${NC}"
$GRADLE_CMD quarkusDev

# 如果没有以开发模式启动，则以生产模式运行
if [ $? -ne 0 ]; then
    echo ""
    echo -e "${YELLOW}尝试以生产模式启动...${NC}"
    java -jar build/quarkus-app/quarkus-run.jar
fi
