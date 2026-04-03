## table_name：`resume_sections`

简历中的所有区块（区域）统一存储在一张表中，通过 `type` 字段区分不同类型的区块。

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | UUID | 区块唯一标识 | `4522f7a4-6011-4952-b5ee-0145626c2d88` |
| `resume_id` | UUID | 所属简历ID | `1f130bac-42b4-4415-8485-ee735134e573` |
| `type` | VARCHAR(50) | 区块类型，见下方类型枚举 | `personal_info` |
| `title` | VARCHAR(100) | 区块显示标题 | `个人信息` |
| `sort_order` | INT | 排序序号，越小越靠前 | `0` |
| `visible` | BOOLEAN | 是否可见 | `true` |
| `content` | JSON | 区块内容，结构随 type 不同而不同 | 见各类型详情 |
| `created_at` | DATETIME | 创建时间 | `2026-03-31T06:15:58.000Z` |
| `updated_at` | DATETIME | 更新时间 | `2026-03-31T13:33:41.000Z` |

### 区块类型枚举 (`type`)

| type 值 | 标题 | 说明 |
|---------|------|------|
| `personal_info` | 个人信息 | 姓名、联系方式等基本信息 |
| `summary` | 个人简介 | 一段自我介绍文字 |
| `work_experience` | 工作经历 | 多条工作/项目经历列表 |
| `projects` | 项目经历 | 独立的项目/作品集列表 |
| `education` | 教育背景 | 教育经历列表 |
| `skills` | 技能特长 | 按分类组织的技能列表 |
| `languages` | 语言能力 | 语言能力列表 |
| `certifications` | 资格证书 | 证书/资格认证列表 |
| `qr_codes` | 二维码 | 二维码链接列表 |
| `github` | GitHub 项目 | GitHub 仓库展示列表 |
| `custom` | 自定义区域 | 用户自定义的区块（如比赛等） |

---

## 各区块 `content` JSON 结构

### 1. `personal_info` - 个人信息

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `full_name` | STRING | 姓名 | `张贞翔` |
| `job_title` | STRING | 求职意向/职位 | `AI Agent 开发 / Python 开发 / Java 开发` |
| `email` | STRING | 邮箱 | `401303740@qq.com` |
| `phone` | STRING | 电话 | `19720136938` |
| `location` | STRING | 所在城市 | `深圳` |
| `salary` | STRING | 期望薪资 | `4-6K` |
| `age` | STRING | 年龄 | `21` |
| `gender` | STRING | 性别 | `男` |
| `political_status` | STRING | 政治面貌 | `` |
| `education_level` | STRING | 学历 | `本科` |
| `avatar` | STRING | 头像URL | `` |

---

### 2. `summary` - 个人简介

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `text` | TEXT | 简介文本 | `人工智能专业本科在读...` |

---

### 3. `work_experience` - 工作经历

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | 工作经历条目列表 |

`items` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 条目唯一标识 | `a1b2c3d4-0001` |
| `company` | STRING | 公司/组织名称 | `湖南工业大学双创中心` |
| `position` | STRING | 职位/角色 | `AI 原生应用开发组成员` |
| `location` | STRING | 工作地点 | `株洲` |
| `start_date` | STRING | 开始日期 | `2023-09` |
| `end_date` | STRING | 结束日期 | `至今` |
| `current` | BOOLEAN | 是否为当前工作 | `true` |
| `description` | TEXT | 工作描述 | `AI 原生应用开发组核心成员...` |
| `highlights` | ARRAY[STRING] | 工作亮点/成就列表 | `["负责模型 API 封装...", ...]` |

---

### 4. `education` - 教育背景

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | 教育经历条目列表 |

`items` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 条目唯一标识 | `e1f2g3h4-0001` |
| `institution` | STRING | 学校/机构名称 | `湖南工业大学` |
| `degree` | STRING | 学位 | `本科` |
| `field` | STRING | 专业 | `人工智能` |
| `location` | STRING | 所在地 | `湖南·株洲` |
| `start_date` | STRING | 入学日期 | `2023-09` |
| `end_date` | STRING | 毕业日期 | `2027` |
| `gpa` | STRING | 绩点 | `` |
| `highlights` | ARRAY[STRING] | 荣誉/成就列表 | `["湖南省大学生程序设计竞赛二等奖", ...]` |

---

### 5. `skills` - 技能特长

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `categories` | ARRAY | 技能分类列表 |

`categories` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 分类唯一标识 | `s1t2u3v4-0001` |
| `name` | STRING | 分类名称 | `LLM & Agent（核心）` |
| `skills` | ARRAY[STRING] | 该分类下的技能列表 | `["LangChain", "LangGraph", ...]` |

---

### 6. `languages` - 语言能力

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | 语言能力条目列表 |

`items` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 条目唯一标识 | `34ad9347-0b25-42ca-9aae-bc3c20e0ef39` |
| `language` | STRING | 语言名称 | `英语` |
| `proficiency` | STRING | 熟练程度/等级 | `6级` |
| `description` | STRING | 补充说明 | `掌握良好` |

---

### 7. `projects` - 项目经历

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | 项目条目列表 |

`items` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 条目唯一标识 | `36e0b95a-bc7d-4795-bcad-da5d8fa5ea16` |
| `name` | STRING | 项目名称 | `ai智能简历生成` |
| `description` | TEXT | 项目描述 | `用于ai自动生成和完善简历` |
| `technologies` | ARRAY[STRING] | 技术栈 | `["react", "fastapi", "openai", "tool_call"]` |
| `highlights` | ARRAY[STRING] | 项目亮点列表 | `["有多个功能可以ai自动生成解析和完善"]` |
| `url` | STRING | 项目链接/地址 | `无` |
| `start_date` | STRING | 开始日期 | `2026-04` |
| `end_date` | STRING | 结束日期 | `2026-05` |

---

### 8. `certifications` - 资格证书

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | 证书条目列表 |

`items` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 条目唯一标识 | `12f69b03-07bf-445d-9cc8-b625d895488a` |
| `name` | STRING | 证书名称 | `四级` |
| `issuer` | STRING | 颁发机构 | `国家` |
| `date` | STRING | 获得日期 | `2022年` |

---

### 9. `github` - GitHub 项目

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | GitHub 仓库条目列表 |

`items` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 条目唯一标识 | `31ebdce7-4b53-40d3-8b4d-530e3b54665f` |
| `repo_url` | STRING | GitHub 仓库链接 | `htttps://github.com/zhangzhenxiang6666` |
| `name` | STRING | 仓库名称 | `` |
| `stars` | INT | Star 数量 | `0` |
| `language` | STRING | 主要编程语言 | `` |
| `description` | TEXT | 仓库描述 | `我的仓库` |

---

### 10. `qr_codes` - 二维码

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | 二维码条目列表（可为空） |

`items` 数组中每个元素（暂无实例，预计结构）：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `id` | STRING | 条目唯一标识 |
| `url` | STRING | 二维码指向的链接 |
| `label` | STRING | 显示标签 |

---

### 10. `custom` - 自定义区域

顶层结构：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `items` | ARRAY | 自定义条目列表 |

`items` 数组中每个元素：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | STRING | 条目唯一标识 | `410f71eb-8a4c-4837-b855-18c684b0b750` |
| `title` | STRING | 条目标题 | `xx比赛` |
| `description` | TEXT | 条目描述，支持换行符 | `xx比赛描述\nxxx` |
| `date` | STRING | 日期范围 | `xxx年xx月xx日-xxx年xxx月xx日` |
