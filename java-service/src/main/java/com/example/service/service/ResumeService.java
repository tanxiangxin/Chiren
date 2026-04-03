package com.example.service.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.example.service.common.po.Result;
import com.example.service.dto.ResumeDto;
import com.example.service.model.Resume;

import java.util.List;

public interface ResumeService extends IService<Resume> {

    List<Resume> listByUserId(String userId);

    Resume createResume(ResumeDto dto);

    Resume updateResume(ResumeDto dto);

    Result<String> deleteResume(String id);
}
