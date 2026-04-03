package com.example.service.common.enums;

import lombok.Getter;

@Getter
public enum StatusEnum {
    NOT_START("未开始"),
    BEGIN("进行中");



    private final String status;

    StatusEnum(String status) {
        this.status = status;
    }
}
