package com.example.service.controller;

import com.example.service.common.po.Result;
import com.example.service.dto.ResumeDto;
import com.example.service.model.Resume;
import com.example.service.model.ResumeSection;
import com.example.service.model.Template;
import com.example.service.service.ResumeSectionService;
import com.example.service.service.ResumeService;
import com.example.service.service.TemplateService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "简历管理", description = "简历相关接口")
@RestController
@RequestMapping("/api/resume")
public class ResumeController {

    private final ResumeService resumeService;

    public ResumeController(ResumeService resumeService) {
        this.resumeService = resumeService;
    }

    // ==================== 简历 ====================

    @Operation(summary = "查询某用户的简历列表")
    @GetMapping("/list")
    public Result<List<Resume>> listResumes(@Parameter(description = "用户ID", required = true) @RequestParam String userId) {
        return Result.success(resumeService.listByUserId(userId));
    }

    @Operation(summary = "查询单个简历")
    @GetMapping("/detail/{id}")
    public Result<Resume> getResume(@Parameter(description = "简历ID",required = true) @PathVariable String id) {
        Resume resume = resumeService.getById(id);
        if (resume == null) {
            return Result.error("简历不存在");
        }
        return Result.success(resume);
    }

    @Operation(summary = "创建新简历")
    @PostMapping("/create")
    public Result<Resume> createResume(@RequestBody ResumeDto dto) {
        Resume created = resumeService.createResume(dto);
        return Result.success("创建成功", created);
    }

    @Operation(summary = "更新简历")
    @PostMapping("/update")
    public Result<Resume> updateResume(@RequestBody ResumeDto dto) {
        Resume updated = resumeService.updateResume(dto);
        return Result.success("更新成功", updated);
    }

    @Operation(summary = "删除简历")
    @PostMapping("/delete")
    public Result<String> deleteResume(@Parameter(description = "简历ID",required = true) @RequestParam String id){
        return resumeService.deleteResume(id);
    }

}
