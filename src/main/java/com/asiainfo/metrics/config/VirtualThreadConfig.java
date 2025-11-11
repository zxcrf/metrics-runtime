package com.asiainfo.metrics.config;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * 虚拟线程配置
 * JDK 21 虚拟线程 - 简单粗暴有效
 *
 * 优势:
 * - 无队列限制
 * - 无线程数限制
 * - 按需创建，用完即销毁
 * - 内存占用：每个虚拟线程 ~KB级别
 */
@ApplicationScoped
public class VirtualThreadConfig {

    private static final Logger log = LoggerFactory.getLogger(VirtualThreadConfig.class);

    private Executor computeExecutor;

    void onStart(@Observes StartupEvent event) {
        // 就这么简单！
        this.computeExecutor = Executors.newVirtualThreadPerTaskExecutor();

        log.info("虚拟线程执行器初始化完成");
        log.info("使用 JDK 21 虚拟线程，无需调优任何参数");
    }

    /**
     * 获取计算执行器
     * 用于I/O密集型任务：文件I/O、网络I/O、数据库查询
     */
    public Executor getComputeExecutor() {
        return computeExecutor;
    }
}
