package com.example.service.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.example.service.common.po.Result;
import com.example.service.dto.TemplateDto;
import com.example.service.model.Template;

import java.util.List;

public interface TemplateService extends IService<Template> {

    List<Template> listActive();

    Template getByName(String name);

    Boolean create(Template template);

    Template createTemplate(TemplateDto dto);

    Result<String> deleteTemplate(String id);

    Template updateTemplate(TemplateDto dto);
}
