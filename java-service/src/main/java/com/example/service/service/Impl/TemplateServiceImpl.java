package com.example.service.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.example.service.common.enums.DiscriptionEnum;
import com.example.service.common.po.BusinessException;
import com.example.service.common.po.Result;
import com.example.service.dto.TemplateDto;
import com.example.service.mapper.TemplateMapper;
import com.example.service.model.Template;
import com.example.service.service.TemplateService;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class TemplateServiceImpl extends ServiceImpl<TemplateMapper, Template> implements TemplateService {

    @Override
    public List<Template> listActive() {
        return list(new LambdaQueryWrapper<Template>()
                .eq(Template::getIsActive, true)
                .orderByAsc(Template::getCreatedAt));
    }

    @Override
    public Template getByName(String name) {
        return getOne(new LambdaQueryWrapper<Template>()
                .eq(Template::getName, name));
    }

    @Override
    public Boolean create(Template template) {
        return save(template);
    }

    @Override
    public Template createTemplate(TemplateDto dto){
        Template template = iniiTemplate(dto);
        if (!save(template)) {
           throw new BusinessException("添加失败");
        }
        return template;
    }

    @Override
    public Result<String> deleteTemplate(String id) {
        Template template = getById(id);
        if ( template == null){
            throw new BusinessException("模板不存在");
        }
        if (!removeById(id)) {
            throw new BusinessException("删除失败");
        }
        return Result.success("删除成功");
    }

    @Override
    public Template updateTemplate(TemplateDto dto) {
        Template template = iniiTemplate(dto);
        if (!updateById(template)){
            throw new BusinessException("更新失败");
        }
        return null;
    }

    public Template iniiTemplate(TemplateDto dto){
        Template template = new Template();
        BeanUtils.copyProperties(dto,template);
        if (template.getId() == null){
            template.setId(String.valueOf(UUID.randomUUID()));
        }
        LocalDateTime now = LocalDateTime.now();
        if (template.getCreatedAt() == null){
            template.setCreatedAt(now);
        }
        if (template.getUpdatedAt() == null){
            template.setUpdatedAt(now);
        }
        if (template.getDescription() == null){
            template.setDescription(DiscriptionEnum.DEFATULE.getDiscription());
        }
        if (template.getIsActive() == null){
            template.setIsActive(true);
        }
        return template;
    }
}
