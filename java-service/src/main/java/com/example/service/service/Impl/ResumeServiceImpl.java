package com.example.service.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.example.service.common.po.BusinessException;
import com.example.service.common.po.Result;
import com.example.service.dto.ResumeDto;
import com.example.service.mapper.ResumeMapper;
import com.example.service.model.Resume;
import com.example.service.service.ResumeService;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class ResumeServiceImpl extends ServiceImpl<ResumeMapper, Resume> implements ResumeService {

    @Override
    public List<Resume> listByUserId(String userId) {
        return list(new LambdaQueryWrapper<Resume>()
                .eq(Resume::getUserId, userId)
                .orderByDesc(Resume::getCreatedAt));
    }

    @Override
    public Resume createResume(ResumeDto dto) {
        Resume resume = initResume(dto);
        if (save(resume)) {
            throw new BusinessException("新增失败");
        }
        return resume;
    }

    @Override
    public Resume updateResume(ResumeDto dto) {
        Resume resume = initResume(dto);
        if (updateById(resume)){
            throw new BusinessException("更新失败");
        }
        return resume;
    }

    @Override
    public Result<String> deleteResume(String id) {
        Resume one = getById(id);
        if (one == null){
            throw new BusinessException("该简历不存在");
        }
        if (!removeById(id)){
            throw new BusinessException("删除失败");
        }
        return Result.success("删除成功");
    }

    public Resume initResume(ResumeDto dto){
        LocalDateTime now = LocalDateTime.now();
        Resume resume = new Resume();
        BeanUtils.copyProperties(dto,resume);
        if (resume.getCreatedAt() == null){
            resume.setCreatedAt(now);
        }
        if (resume.getUpdatedAt() == null){
            resume.setUpdatedAt(now);
        }
        return resume;
    }
}
