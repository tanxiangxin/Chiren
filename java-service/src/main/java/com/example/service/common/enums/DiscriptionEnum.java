package com.example.service.common.enums;

public enum DiscriptionEnum {
    DEFATULE("该用户很懒，什么都没有");

    private final String discription;

    DiscriptionEnum(String s) {
        this.discription = s;
    }

    public String getDiscription() {
        return discription;
    }
}
