package com.asiainfo.metrics.api.dto;

/**
 * 模型执行结果
 */
public record ModelExecutionResult(
        String modelId,
        String opTime,
        String status,
        String message,
        int computeCount,
        int storageCount
) {
    
    /**
     * 成功结果
     */
    public static ModelExecutionResult success(String modelId, String opTime, int computeCount, int storageCount) {
        return new ModelExecutionResult(modelId, opTime, "SUCCESS", null, computeCount, storageCount);
    }
    
    /**
     * 失败结果
     */
    public static ModelExecutionResult failed(String modelId, String opTime, String message) {
        return new ModelExecutionResult(modelId, opTime, "FAILED", message, 0, 0);
    }
    
    /**
     * 跳过结果（并发执行中）
     */
    public static ModelExecutionResult skipped(String modelId, String opTime, String message) {
        return new ModelExecutionResult(modelId, opTime, "SKIPPED", message, 0, 0);
    }
}
