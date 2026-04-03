package com.example.service.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.example.service.model.Work;

import java.util.List;

public interface WorkService extends IService<Work> {

    List<Work> listByStatus(String status);

    Work createWork(Work work);

    Work updateWorkStatus(String id, String status);
}
