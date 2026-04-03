package com.example.service.controller;

import com.example.service.common.po.Result;
import com.example.service.model.ResumeSection;
import com.example.service.service.ResumeSectionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/resume/section")
@Tag(name = "区域管理", description = "区块相关接口")
public class ResumeSectionController {

    private final ResumeSectionService resumeSectionService;

    public ResumeSectionController(ResumeSectionService resumeSectionService) {
        this.resumeSectionService = resumeSectionService;
    }

    @Operation(summary = "获取指定简历指定区块信息", description = "获取单个区块的信息")
    @GetMapping("/{id}/{type}")
    public Result<ResumeSection> getByIdAndType(@PathVariable String id, @PathVariable String type) {
        return Result.success(resumeSectionService.getByIdAndType(id,type));
    }

    @Operation(summary = "根据简历id获取区块列表", description = "获取指定简历的所有区块信息")
    @GetMapping("/{id}/list")
    public Result<List<ResumeSection>> getByResumeId(@Parameter(description = "简历ID") @PathVariable String id) {
        List<ResumeSection> section = resumeSectionService.getByResumeId(id);
        if (section.isEmpty()) {
            return Result.error("简历不存在");
        }
        return Result.success(section);
    }

    @Operation(summary = "添加区块", description = "添加区块")
    @PostMapping("/create")
    public Result<ResumeSection> create(@RequestBody ResumeSection section) {
        resumeSectionService.create(section);
        return Result.success(section);
    }
}
