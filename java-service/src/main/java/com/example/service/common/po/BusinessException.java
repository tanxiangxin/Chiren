package com.example.service.common.po;

import lombok.Getter;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Getter
public class BusinessException extends RuntimeException{
    private final Integer code;

    public BusinessException(Integer code, String message) {
        super(message);
        this.code = code;
    }

    public BusinessException(String message) {
        super(message);
        this.code = 400;
    }

}
