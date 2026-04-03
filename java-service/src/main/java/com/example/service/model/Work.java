package com.example.service.model;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 工作任务实体
 */
@Data
@TableName("work")
@Schema(description = "工作任务")
public class Work {

    @TableId("id")
    @Schema(description = "任务ID")
    private String id;

    @Schema(description = "文件名称")
    private String fileName;

    @Schema(description = "文件绝对路径")
    private String src;

    @Schema(description = "任务状态")
    private String status;

    @Schema(description = "模板名称")
    private String template;

    @Schema(description = "简历标题")
    private String title;

    @Schema(description = "创建时间")
    private LocalDateTime createdAt;

    @Schema(description = "修改时间")
    private LocalDateTime updatedAt;
}
