package com.example.service.dto;

import com.baomidou.mybatisplus.annotation.TableId;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class ResumeSectionDto {
    @Schema(description = "区块ID",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    @TableId("id")
    private String id;
    @Schema(description = "所属简历ID",requiredMode = Schema.RequiredMode.REQUIRED)
    private String resumeId;
    @Schema(description = "区块类型",requiredMode = Schema.RequiredMode.REQUIRED)
    private String type;
    @Schema(description = "区块显示标题",requiredMode = Schema.RequiredMode.REQUIRED)
    private String title;
    @Schema(description = "排序序号",requiredMode = Schema.RequiredMode.REQUIRED)
    private Integer sortOrder;
    @Schema(description = "是否可见",requiredMode = Schema.RequiredMode.REQUIRED,defaultValue = "true")
    private Boolean visible;
    @Schema(description = "区块内容（JSON字符串）",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private String content;
    @Schema(description = "创建时间",requiredMode = Schema.RequiredMode.REQUIRED)
    private LocalDateTime createdAt;
    @Schema(description = "更新时间",requiredMode = Schema.RequiredMode.REQUIRED)
    private LocalDateTime updatedAt;
}
