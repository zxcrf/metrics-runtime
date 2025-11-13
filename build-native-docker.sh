#!/bin/bash
#
# Docker 多平台构建脚本
# 使用 Docker Buildx 构建 ARM64 和 x86_64 两个平台的 native 镜像
#
# 使用方法:
#   chmod +x build-native-docker.sh
#   ./build-native-docker.sh
#

set -e

echo "========================================="
echo "  DataOS Metrics Runtime - Native Build"
echo "      Using Docker Multi-Platform"
echo "========================================="
echo ""

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 检查 Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}错误: Docker 未安装${NC}"
    exit 1
fi

echo -e "${BLUE}✓ Docker 已安装${NC}"

# 检查 Docker Buildx
docker buildx version &> /dev/null
if [ $? -ne 0 ]; then
    echo -e "${RED}错误: Docker Buildx 未安装或未启用${NC}"
    exit 1
fi

echo -e "${BLUE}✓ Docker Buildx 可用${NC}"
echo ""

# 清理旧构建
echo -e "${YELLOW}清理旧的构建结果...${NC}"
rm -rf build/* 2>/dev/null || true
mkdir -p build
echo ""

# 构建前先清理
echo -e "${YELLOW}执行 Gradle 清理...${NC}"

# 使用 GraalVM 进行 native 构建
export GRAALVM_HOME=/Users/qqz/.sdkman/candidates/java/21.0.2-graalce
export JAVA_HOME=$GRAALVM_HOME
export PATH=$GRAALVM_HOME/bin:$PATH

./gradlew clean --no-daemon
echo ""

# 构建 native 镜像 (仅构建 native 包，不构建 JAR)
echo -e "${YELLOW}构建 native 镜像...${NC}"
echo ""
# 使用 Quarkus 推荐的方式构建 native 镜像
./gradlew quarkusBuild -Dquarkus.package.type=native -x test --no-daemon
echo ""

# 方法 1: 直接使用 Docker Buildx 构建多平台镜像
echo -e "${BLUE}使用 Docker Buildx 构建多平台镜像...${NC}"
echo ""

# 构建命令
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --tag harbor-apaas-dnc.asiainfo.com.cn/public/dataos-metrics-runtime:1.0.0-native \
    --push \
    -f Dockerfile.native .

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ 多平台镜像构建并推送到仓库${NC}"
else
    echo ""
    echo -e "${RED}✗ Docker 构建失败${NC}"
    exit 1
fi

# 方法 2: 构建本地镜像（不推送）
# docker buildx build \
#     --platform linux/amd64,linux/arm64 \
#     --tag dataos-metrics-runtime-native:latest \
#     -f Dockerfile.native .

echo ""
echo "========================================="
echo -e "${GREEN}  构建完成！${NC}"
echo "========================================="
echo ""
echo "镜像信息:"
echo "  名称: quay.io/quarkus/dataos-metrics-runtime:1.0.0-native"
echo "  平台: linux/amd64, linux/arm64"
echo ""

echo "使用 Docker 运行:"
echo "  docker run -it --rm -p 8080:8080 \\"
echo "    quay.io/quarkus/dataos-metrics-runtime:1.0.0-native"
echo ""

echo "从不同平台拉取镜像:"
echo "  AMD64 (x86_64):"
echo "    docker pull --platform linux/amd64 harbor-apaas-dnc.asiainfo.com.cn/public/dataos-metrics-runtime:1.0.0-native"
echo ""
echo "  ARM64:"
echo "    docker pull --platform linux/arm64 harbor-apaas-dnc.asiainfo.com.cn/public/dataos-metrics-runtime:1.0.0-native"
echo ""

exit 0
