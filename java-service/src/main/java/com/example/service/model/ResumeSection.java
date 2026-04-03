package com.example.service.model;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.example.service.common.enums.TypeEnum;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.LocalDateTime;

/**
 * 简历区块实体
 */
@Schema(description = "简历区块")
@TableName("resume_sections")
public class ResumeSection {

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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getResumeId() {
        return resumeId;
    }

    public void setResumeId(String resumeId) {
        this.resumeId = resumeId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(Integer sortOrder) {
        this.sortOrder = sortOrder;
    }

    public Boolean getVisible() {
        return visible;
    }

    public void setVisible(Boolean visible) {
        this.visible = visible;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
