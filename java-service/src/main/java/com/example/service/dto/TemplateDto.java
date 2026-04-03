package com.example.service.dto;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TemplateDto {
    @Schema(description = "模板ID",requiredMode = Schema.RequiredMode.REQUIRED)
    private String id;
    @Schema(description = "模板名称",requiredMode = Schema.RequiredMode.REQUIRED)
    private String name;
    @Schema(description = "模板显示名称",requiredMode = Schema.RequiredMode.REQUIRED)
    private String displayName;
    @Schema(description = "模板预览图片",requiredMode = Schema.RequiredMode.REQUIRED)
    private String previewImageUrl;
    @Schema(description = "模板描述",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private String description;
    @Schema(description = "是否激活",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private Boolean isActive;
    @Schema(description = "创建时间",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private LocalDateTime createdAt;
    @Schema(description = "更新时间",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private LocalDateTime updatedAt;
}
