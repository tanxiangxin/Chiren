package com.example.service.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.example.service.common.po.BusinessException;
import com.example.service.mapper.ResumeSectionMapper;
import com.example.service.model.ResumeSection;
import com.example.service.service.ResumeSectionService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ResumeSectionServiceImpl extends ServiceImpl<ResumeSectionMapper, ResumeSection> implements ResumeSectionService {

    @Override
    public List<ResumeSection> listByResumeId(String resumeId) {
        return list(new LambdaQueryWrapper<ResumeSection>()
                .eq(ResumeSection::getResumeId, resumeId)
                .orderByAsc(ResumeSection::getSortOrder));
    }

    @Override
    public void create(ResumeSection section) {
        LambdaQueryWrapper<ResumeSection> eq = new LambdaQueryWrapper<ResumeSection>()
                .eq(section.getResumeId() != null, ResumeSection::getResumeId, section.getResumeId())
                .eq(section.getType()!=null,ResumeSection::getType,section.getType());
        if (getOne(eq) != null){
            throw new BusinessException("该区块已存在");
        }
        if (!save(section)) {
            throw new BusinessException("新增失败");
        }
    }

    @Override
    public ResumeSection getByIdAndType(String id, String type) {
        if (id == null || type == null){
            throw new BusinessException("不能为空");
        }
        LambdaQueryWrapper<ResumeSection> eq = new LambdaQueryWrapper<ResumeSection>()
                .eq( ResumeSection::getResumeId,id)
                .eq(ResumeSection::getType,type);
        return getOne(eq);
    }

    @Override
    public List<ResumeSection> getByResumeId(String id) {
            return list(new LambdaQueryWrapper<ResumeSection>()
                    .eq(ResumeSection::getResumeId,id));
    }
}
