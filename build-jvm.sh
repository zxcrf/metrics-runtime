#!/bin/bash
#
# JVM 构建脚本
# 构建标准 JVM 版本（jar文件）
#
# 使用方法:
#   chmod +x build-jvm.sh
#   ./build-jvm.sh
#

set -e

echo "========================================="
echo "  DataOS Metrics Runtime - JVM Build"
echo "========================================="
echo ""

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 检查 Java
export JAVA_HOME=/Users/qqz/.sdkman/candidates/java/21.0.9-zulu
export PATH=$JAVA_HOME/bin:$PATH

java -version
echo ""

# 清理
echo -e "${YELLOW}清理旧的构建结果...${NC}"
./gradlew clean --no-daemon
echo ""

# 构建
echo -e "${BLUE}开始构建 JVM 版本...${NC}"
./gradlew build -x test --no-daemon

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ JVM 构建成功${NC}"
else
    echo ""
    echo -e "${RED}✗ JVM 构建失败${NC}"
    exit 1
fi

echo ""
echo "========================================="
echo -e "${GREEN}  构建完成！${NC}"
echo "========================================="
echo ""

# 显示构建产物
JAR_FILE=$(find build -name "*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)

if [ -n "$JAR_FILE" ]; then
    SIZE=$(du -h "$JAR_FILE" | cut -f1)
    echo "产物信息:"
    echo "  文件: $JAR_FILE"
    echo "  大小: $SIZE"
    echo ""
    echo "使用方式:"
    echo "  java -jar $JAR_FILE"
    echo ""
    echo "后台运行:"
    echo "  nohup java -jar $JAR_FILE > app.log 2>&1 &"
    echo ""
fi

exit 0
