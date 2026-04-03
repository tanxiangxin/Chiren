package com.example.service.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.example.service.model.ResumeSection;

import java.util.List;

public interface ResumeSectionService extends IService<ResumeSection> {

    List<ResumeSection> listByResumeId(String resumeId);

    void create(ResumeSection section);

    ResumeSection getByIdAndType(String id, String type);

    List<ResumeSection> getByResumeId(String id);
}
