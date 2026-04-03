package com.example.service.model;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.LocalDateTime;

/**
 * 简历模板实体
 */
@Schema(description = "简历模板")
@TableName("templates")
public class Template {

    @Schema(description = "模板ID")
    @TableId("id")
    private String id;
    @Schema(description = "模板名称")
    @TableField("name")
    private String name;
    @Schema(description = "模板显示名称")
    @TableField("display_name")
    private String displayName;
    @Schema(description = "模板缩略图URL")
    @TableField("preview_image_url")
    private String previewImageUrl;
    @Schema(description = "模板描述")
    @TableField("description")
    private String description;
    @Schema(description = "是否启用")
    @TableField("is_active")
    private Boolean isActive;
    @Schema(description = "创建时间")
    @TableField("created_at")
    private LocalDateTime createdAt;
    @Schema(description = "更新时间")
    @TableField("updated_at")
    private LocalDateTime updatedAt;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getPreviewImageUrl() {
        return previewImageUrl;
    }

    public void setPreviewImageUrl(String previewImageUrl) {
        this.previewImageUrl = previewImageUrl;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
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
