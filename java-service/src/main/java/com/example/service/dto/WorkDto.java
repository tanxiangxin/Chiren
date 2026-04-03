package com.example.service.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class WorkDto {
    @Schema(description = "任务ID",requiredMode = Schema.RequiredMode.REQUIRED)
    private String id;

    @Schema(description = "文件名称",requiredMode = Schema.RequiredMode.REQUIRED)
    private String fileName;

    @Schema(description = "文件绝对路径",requiredMode = Schema.RequiredMode.REQUIRED)
    private String src;

    @Schema(description = "任务状态",requiredMode = Schema.RequiredMode.REQUIRED)
    private String status;

    @Schema(description = "创建时间",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private LocalDateTime createdAt;

    @Schema(description = "修改时间",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private LocalDateTime updatedAt;
}
