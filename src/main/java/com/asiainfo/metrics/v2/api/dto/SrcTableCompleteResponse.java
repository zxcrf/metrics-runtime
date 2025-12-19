package com.asiainfo.metrics.v2.api.dto;

import java.util.List;

/**
 * srcTableComplete 响应体
 */
public record SrcTableCompleteResponse(
        String status,
        String message,
        List<ModelExecutionResult> triggeredModels,
        List<String> waitingModels,
        List<String> skippedModels
) {
    
    /**
     * 成功响应
     */
    public static SrcTableCompleteResponse success(
            List<ModelExecutionResult> triggeredModels,
            List<String> waitingModels,
            List<String> skippedModels) {
        return new SrcTableCompleteResponse("SUCCESS", null, triggeredModels, waitingModels, skippedModels);
    }
    
    /**
     * 无模型依赖此表
     */
    public static SrcTableCompleteResponse ignored(String message) {
        return new SrcTableCompleteResponse("IGNORED", message, List.of(), List.of(), List.of());
    }
    
    /**
     * 错误响应
     */
    public static SrcTableCompleteResponse error(String message) {
        return new SrcTableCompleteResponse("ERROR", message, null, null, null);
    }
}
