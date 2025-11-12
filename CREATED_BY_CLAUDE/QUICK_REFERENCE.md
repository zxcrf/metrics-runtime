# Gradle 快速参考

## 基础命令

### 编译和构建
```bash
# 清理项目
./gradlew clean

# 编译项目
./gradlew compileJava

# 构建项目 (编译+测试+打包)
./gradlew build

# 构建但不运行测试
./gradlew build -x test

# 仅运行测试
./gradlew test
```

### 开发模式
```bash
# 启动Quarkus开发模式 (热重载)
./gradlew quarkusDev

# 后台运行开发模式
./gradlew quarkusDev --daemon
```

### 依赖管理
```bash
# 查看依赖树
./gradlew dependencies

# 查看特定配置的依赖
./gradlew dependencies --configuration runtimeClasspath

# 刷新依赖
./gradlew --refresh-dependencies clean build

# 强制更新依赖
./gradlew clean build --refresh-dependencies
```

### Quarkus特定任务
```bash
# 开发模式 (带热重载)
./gradlew quarkusDev

# 构建JVM包
./gradlew build

# 构建Native镜像
./gradlew buildNative

# 拉取配置
./gradlew quarkusGenerateConfig

# 扩展生成
./gradlew quarkusGenerateSchema
```

## 高级命令

### 并行和缓存
```bash
# 并行构建
./gradlew build --parallel

# 使用构建缓存
./gradlew build --build-cache

# 配置缓存
./gradlew build --configuration-cache

# 组合使用
./gradlew build --parallel --build-cache --configuration-cache
```

### 调试和诊断
```bash
# 生成构建扫描
./gradlew build --scan

# 详细日志
./gradlew build --info

# 调试日志
./gradlew build --debug

# 仅显示错误
./gradlew build --quiet
```

### 任务管理
```bash
# 查看所有可用任务
./gradlew tasks

# 查看构建任务
./gradlew tasks --group=build

# 查看应用任务
./gradlew tasks --group=application

# 跳过某个任务
./gradlew build -x test
```

### 性能优化
```bash
# 使用守护进程
./gradlew build

# 在线模式 (下载依赖)
./gradlew build --online

# 离线模式 (不下载依赖)
./gradlew build --offline
```

## 项目结构命令

### 源码集管理
```bash
# 编译main源码
./gradlew compileJava

# 编译测试源码
./gradlew compileTestJava

# 处理资源文件
./gradlew processResources
```

### 打包
```bash
# 创建JAR文件
./gradlew jar

# 创建Source JAR
./gradlew sourcesJar

# 创建Javadoc JAR
./gradlew javadoc

# 发布到Maven仓库
./gradlew publish
```

## 清理和维护

### 清理
```bash
# 清理构建文件
./gradlew clean

# 清理所有构建输出 (包括wrapper distribution)
./gradlew clean cleanBuildCache

# 清理Gradle缓存
rm -rf ~/.gradle/caches/

# 清理Wrapper下载
rm -rf ~/.gradle/wrapper/dists/
```

### 验证
```bash
# 检查代码
./gradlew check

# 运行所有测试
./gradlew test

# 生成测试报告
./gradlew testReport

# 代码格式检查 (如果使用Spotless)
./gradlew spotlessCheck
```

## 环境信息

### 系统信息
```bash
# 查看Gradle版本
./gradlew --version

# 查看Java版本
./gradlew --version

# 查看可用任务
./gradlew tasks --all
```

### 依赖信息
```bash
# 列出所有依赖
./gradlew dependencies

# 查看特定模块依赖
./gradlew dependencies --configuration compileClasspath

# 查看依赖冲突
./gradlew app:dependencies --configuration runtimeClasspath | grep Conflict
```

## 常见问题解决

### 问题1: 构建失败
```bash
# 清理重新构建
./gradlew clean build --refresh-dependencies

# 检查详细错误
./gradlew build --info --stacktrace
```

### 问题2: 依赖解析失败
```bash
# 刷新依赖
./gradlew --refresh-dependencies

# 清除缓存
rm -rf ~/.gradle/caches/modules-2/
./gradlew clean build
```

### 问题3: 内存不足
```bash
# 增加JVM堆内存
export GRADLE_OPTS="-Xmx4g"
./gradlew build

# 或在gradle.properties中设置
# org.gradle.jvmargs=-Xmx4g
```

## CI/CD 集成

### GitHub Actions
```yaml
- name: Build with Gradle
  uses: gradle/gradle-build-action@v2
  with:
    gradle-version: 8.8
    arguments: build

- name: Run tests
  uses: gradle/gradle-build-action@v2
  with:
    arguments: test
```

### Jenkins
```groovy
pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                sh './gradlew clean build'
            }
        }
        stage('Test') {
            steps {
                sh './gradlew test'
            }
        }
    }
}
```

## 环境变量

常用环境变量：
```bash
export GRADLE_OPTS="-Xmx2g -XX:MaxMetaspaceSize=1g"
export GRADLE_HOME=/path/to/gradle
export PATH=$PATH:$GRADLE_HOME/bin
```

## 快捷别名

添加到 `~/.bashrc` 或 `~/.zshrc`:
```bash
# 常用命令别名
alias gr='./gradlew'
alias grc='./gradlew clean'
alias grb='./gradlew build'
alias grt='./gradlew test'
alias grd='./gradlew quarkusDev'
alias grdc='./gradlew --refresh-dependencies clean build'
```

## 性能调优

### gradle.properties 优化
```properties
# JVM内存
org.gradle.jvmargs=-Xmx4g -XX:MaxMetaspaceSize=1g

# 并行构建
org.gradle.parallel=true

# 守护进程
org.gradle.daemon=true

# 配置缓存
org.gradle.configuration-cache=true

# 构建缓存
org.gradle.caching=true

# 启用新的孵化功能
org.gradle.unsafe.configuration-projection=true
```

### 项目特定优化
```properties
# Kotlin编译器优化
kotlin.incremental=true

# 编译缓存
org.gradle.caching.java=true

# 失败时继续
org.gradle.continue=true
```
