package com.example.service.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 简历区块类型枚举
 */
@Getter
@AllArgsConstructor
public enum TypeEnum {

    PERSONAL_INFO("personal_info", "个人信息", "姓名、联系方式等基本信息"),
    SUMMARY("summary", "个人简介", "一段自我介绍文字"),
    WORK_EXPERIENCE("work_experience", "工作经历", "多条工作/项目经历列表"),
    PROJECTS("projects", "项目经历", "独立的项目/作品集列表"),
    EDUCATION("education", "教育背景", "教育经历列表"),
    SKILLS("skills", "技能特长", "按分类组织的技能列表"),
    LANGUAGES("languages", "语言能力", "语言能力列表"),
    CERTIFICATIONS("certifications", "资格证书", "证书/资格认证列表"),
    QR_CODES("qr_codes", "二维码", "二维码链接列表"),
    GITHUB("github", "GitHub 项目", "GitHub 仓库展示列表"),
    CUSTOM("custom", "自定义区域", "用户自定义的区块");

    /**
     * 数据库存储值
     */
    private final String code;

    /**
     * 中文标题
     */
    private final String title;

    /**
     * 说明
     */
    private final String description;

    /**
     * 根据 code 获取枚举
     */
    public static TypeEnum fromCode(String code) {
        for (TypeEnum type : values()) {
            if (type.code.equals(code)) {
                return type;
            }
        }
        return null;
    }
}
