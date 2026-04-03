package com.example.service.controller;

import com.example.service.common.po.Result;
import com.example.service.dto.WorkDto;
import com.example.service.model.Work;
import com.example.service.service.WorkService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.BeanUtils;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/work")
@Tag(name = "工作任务管理", description = "工作任务相关接口")
public class WorkController {

    private final WorkService workService;

    public WorkController(WorkService workService) {
        this.workService = workService;
    }

    @Operation(summary = "获取所有工作任务")
    @GetMapping("/list")
    public Result<List<Work>> list() {
        return Result.success(workService.list());
    }

    @Operation(summary = "按状态查询工作任务")
    @GetMapping("/listByStatus")
    public Result<List<Work>> listByStatus(@Parameter(description = "任务状态", required = true) @RequestParam String status) {
        return Result.success(workService.listByStatus(status));
    }

    @Operation(summary = "根据ID查询工作任务")
    @GetMapping("/{id}")
    public Result<Work> getById(@Parameter(description = "任务ID", required = true) @PathVariable String id) {
        Work work = workService.getById(id);
        if (work == null) {
            return Result.error("任务不存在");
        }
        return Result.success(work);
    }

    @Operation(summary = "创建工作任务")
    @PostMapping("/create")
    public Result<Work> create(@RequestBody WorkDto dto) {
        LocalDateTime now = LocalDateTime.now();
        Work work = new Work();
        BeanUtils.copyProperties(dto, work);
        work.setCreatedAt(now);
        work.setUpdatedAt(now);
        Work created = workService.createWork(work);
        return Result.success("创建成功", created);
    }

    @Operation(summary = "更新任务状态")
    @PostMapping("/updateStatus")
    public Result<Work> updateStatus(@RequestParam String id, @RequestParam String status) {
        Work work = workService.updateWorkStatus(id, status);
        if (work == null) {
            return Result.error("任务不存在");
        }
        return Result.success("状态更新成功", work);
    }

    @Operation(summary = "删除工作任务")
    @DeleteMapping("/delete/{id}")
    public Result<Void> delete(@Parameter(description = "任务ID") @PathVariable String id) {
        Work work = workService.getById(id);
        if (work == null) {
            return Result.error("任务不存在");
        }
        workService.removeById(id);
        return Result.success("删除成功", null);
    }
}
