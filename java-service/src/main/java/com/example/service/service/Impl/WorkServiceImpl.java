package com.example.service.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.example.service.mapper.WorkMapper;
import com.example.service.model.Work;
import com.example.service.service.WorkService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class WorkServiceImpl extends ServiceImpl<WorkMapper, Work> implements WorkService {

    @Override
    public List<Work> listByStatus(String status) {
        return list(new LambdaQueryWrapper<Work>()
                .eq(Work::getStatus, status));
    }

    @Override
    public Work createWork(Work work) {
        save(work);
        return getById(work.getId());
    }

    @Override
    public Work updateWorkStatus(String id, String status) {
        Work work = getById(id);
        if (work == null) {
            return null;
        }
        work.setStatus(status);
        updateById(work);
        return work;
    }
}
