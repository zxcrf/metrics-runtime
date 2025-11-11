package com.asiainfo.metrics;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

/**
 * 应用程序主类
 * Quarkus启动入口
 */
@QuarkusMain
public class Application {

    public static void main(String[] args) {
        Quarkus.run(args);
    }

    /**
     * 应用程序实例
     */
    public static class App implements QuarkusApplication {

        @Override
        public int run(String... args) throws Exception {
            // 启动日志
            System.out.println("╔════════════════════════════════════════════════════════════════╗");
            System.out.println("║                                                                ║");
            System.out.println("║           DataOS Metrics Runtime Engine                        ║");
            System.out.println("║                                                                ║");
            System.out.println("║  Quarkus + JDK 21 虚拟线程 + SQLite 水平扩展                   ║");
            System.out.println("║                                                                ║");
            System.out.println("║  - 高性能: Native编译 + 内存计算 + 并行处理                     ║");
            System.out.println("║  - 高扩展性: Pod级别水平扩展，线性性能增长                      ║");
            System.out.println("║  - 低成本: 小规格实例，资源利用率高                            ║");
            System.out.println("║  - 易维护: 清晰的层次架构，便于调试和故障排查                   ║");
            System.out.println("║                                                                ║");
            System.out.println("╚════════════════════════════════════════════════════════════════╝");
            System.out.println();

            // 启动Quarkus应用
            Quarkus.waitForExit();
            return 0;
        }
    }
}
