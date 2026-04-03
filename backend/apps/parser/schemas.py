"""Parser Pydantic 模型。"""

from pydantic import BaseModel, ConfigDict, Field


def _strip_title(schema: dict) -> None:
    """移除 JSON Schema 中的 title 字段。"""

    def strip_obj(obj: dict) -> None:
        obj.pop("title", None)
        if "properties" in obj:
            for prop in obj["properties"].values():
                prop.pop("title", None)
                if prop.get("type") == "array" and "items" in prop:
                    prop["items"].pop("title", None)
        if "$defs" in obj:
            for def_schema in obj["$defs"].values():
                strip_obj(def_schema)

    strip_obj(schema)


class BaseSchema(BaseModel):
    """Schema 基类，自动移除 JSON Schema 中的 title 字段。"""

    model_config = ConfigDict(json_schema_extra=_strip_title)


class TaskIdResponse(BaseSchema):
    """任务 ID 响应模型。"""

    task_id: str


class PersonalInfo(BaseSchema):
    """个人信息模型。"""

    full_name: str = Field(default="", description="姓名")
    job_title: str = Field(default="", description="求职意向/职位")
    email: str = Field(default="", description="邮箱")
    phone: str = Field(default="", description="电话")
    location: str = Field(default="", description="所在城市")
    salary: str = Field(default="", description="期望薪资")
    age: str = Field(default="", description="年龄")
    gender: str = Field(default="", description="性别")
    political_status: str = Field(default="", description="政治面貌")
    education_level: str = Field(default="", description="学历")
    avatar: str = Field(default="", description="头像url")


class EducationItem(BaseSchema):
    """教育背景条目模型。"""

    id: str = Field(default="", description="条目唯一标识")
    institution: str = Field(default="", description="学校/机构名称")
    degree: str = Field(default="", description="学位")
    field: str = Field(default="", description="专业")
    location: str = Field(default="", description="所在地")
    start_date: str = Field(default="", description="入学日期")
    end_date: str = Field(default="", description="毕业日期")
    gpa: str = Field(default="", description="绩点")
    highlights: list[str] = Field(default_factory=list, description="荣誉/成就列表")


class SkillCategory(BaseSchema):
    """技能分类模型。"""

    name: str = Field(default="", description="分类名称")
    skills: list[str] = Field(default_factory=list, description="该分类下的技能列表")


class ProjectItem(BaseSchema):
    """项目经历条目模型。"""

    name: str = Field(default="", description="项目名称")
    description: str = Field(default="", description="项目描述")
    technologies: list[str] = Field(default_factory=list, description="技术栈")
    highlights: list[str] = Field(default_factory=list, description="项目亮点列表")
    url: str = Field(default="", description="项目链接/地址")
    start_date: str = Field(default="", description="开始日期")
    end_date: str = Field(default="", description="结束日期")


class CertificationItem(BaseSchema):
    """资格证书条目模型。"""

    name: str = Field(default="", description="证书名称")
    issuer: str = Field(default="", description="颁发机构")
    date: str = Field(default="", description="获得日期")


class ParserResult(BaseSchema):
    """解析结果模型。"""

    personal_info: PersonalInfo = Field(
        default_factory=PersonalInfo, description="个人信息"
    )
    summary: str = Field(default="", description="个人简介")
    education: list[EducationItem] = Field(default_factory=list, description="教育背景")
    skills: list[SkillCategory] = Field(default_factory=list, description="技能")
    projects: list[ProjectItem] = Field(default_factory=list, description="项目经历")
    certifications: list[CertificationItem] = Field(
        default_factory=list, description="资格证书"
    )


if __name__ == "__main__":
    print(ParserResult.model_json_schema())
