package com.example.service.dto;

import com.baomidou.mybatisplus.annotation.TableId;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class ResumeDto {
    @Schema(description = "简历ID",requiredMode = Schema.RequiredMode.REQUIRED)
    @TableId("id")
    private String id;
    @Schema(description = "所属用户ID",requiredMode = Schema.RequiredMode.REQUIRED)
    private String userId;
    @Schema(description = "所属Workspace（顶级简历）的ID",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "")
    private String workspaceId;
    @Schema(description = "简历标题",requiredMode = Schema.RequiredMode.REQUIRED)
    private String title;
    @Schema(description = "模板名称",requiredMode = Schema.RequiredMode.REQUIRED)
    private String template;
    @Schema(description = "主题配置（JSON字符串）",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "")
    private String themeConfig;
    @Schema(description = "是否为用户的默认简历",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "true")
    private Boolean isDefault;
    @Schema(description = "简历语言",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "zn")
    private String language;
    @Schema(description = "分享链接Token",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "")
    private String shareToken;
    @Schema(description = "是否公开",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "false")
    private Boolean isPublic;
    @Schema(description = "分享密码",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "")
    private String sharePassword;
    @Schema(description = "浏览次数",requiredMode = Schema.RequiredMode.NOT_REQUIRED,defaultValue = "0")
    private Integer viewCount;
    @Schema(description = "创建时间",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private LocalDateTime createdAt;
    @Schema(description = "更新时间",requiredMode = Schema.RequiredMode.NOT_REQUIRED)
    private LocalDateTime updatedAt;
}
