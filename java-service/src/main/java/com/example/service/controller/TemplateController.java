package com.example.service.controller;

import com.example.service.common.po.Result;
import com.example.service.dto.TemplateDto;
import com.example.service.model.Template;
import com.example.service.service.TemplateService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/resume/template")
@Tag(name = "简历模板管理", description = "简历模板相关接口")
public class TemplateController {
    private final TemplateService templateService;

    public TemplateController(TemplateService templateService) {
        this.templateService = templateService;
    }

    @GetMapping("/list")
    @Operation(summary = "获取简历模板列表")
    public Result<List<Template>> list() {
        return Result.success(templateService.list());
    }

    @GetMapping("/active")
    @Operation(summary = "获取所有启用的模板")
    public Result<List<Template>> listActive() {
        return Result.success(templateService.listActive());
    }

    @GetMapping("/{name}")
    @Operation(summary = "根据模板名称获取简历模板")
    public Result<Template> getByName(@Parameter(description = "模板名称") @PathVariable String name) {
        Template template = templateService.getByName(name);
        if (template == null) {
            return Result.error("模板不存在");
        }
        return Result.success(template);
    }

    @PostMapping("/create")
    @Operation(summary = "创建简历模板")
    public Result<Template> createTemplate(@RequestBody TemplateDto dto) {
        return Result.success(templateService.createTemplate(dto));
    }

    @DeleteMapping("/delete")
    @Operation(summary = "根据模板id删除简历模板")
    public Result<String> deleteTemplate(@Parameter(description = "模板名称") @RequestParam String id) {
        return templateService.deleteTemplate(id);
    }

    @PostMapping("/update")
    @Operation(summary = "更新简历模板")
    public Result<Template> updateTemplate(@RequestBody TemplateDto dto) {
        return Result.success(templateService.updateTemplate(dto));
    }
}
