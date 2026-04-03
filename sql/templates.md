## table_name：`templates`

简历模板表，存储所有可用的简历模板元数据。

模板的布局逻辑和样式由前端 React 组件实现（位于 `components/preview/templates/` 目录下），每个模板组件对应一个唯一的 `name` 标识。

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | UUID | 模板唯一标识，主键 | `a1b2c3d4-e5f6-7890-abcd-ef1234567890` |
| `name` | VARCHAR(50) | 模板唯一名称，对应前端组件名或文件路径 | `two-column` |
| `display_name` | VARCHAR(100) | 前端展示用的模板名称 | `双栏布局` |
| `preview_image_url` | VARCHAR(255) | 模板缩略图 URL | `/images/templates/two-column-preview.png` |
| `description` | TEXT | 模板描述，说明适用场景 | `经典双栏布局，适合大多数求职场景` |
| `is_active` | BOOLEAN | 是否启用，禁用后用户无法选择 | `true` |
| `created_at` | DATETIME | 创建时间 | `2026-03-01T00:00:00.000Z` |
| `updated_at` | DATETIME | 最后更新时间 | `2026-03-01T00:00:00.000Z` |

### 设计说明

- `name` 字段唯一，映射到前端 `templates/` 目录下的 React 组件（如 `ats.tsx` → name 为 `ats`）
- 暂不存储 `layout_config`，因为当前所有模板的布局逻辑完全由前端 React 组件控制
- 后续如需在数据库层面控制布局，可在 `layout_config` JSON 字段中定义 section 映射和区域大小

---

## 初始数据（Seed Data）

前端当前已有 50+ 模板，以下是部分核心模板的初始数据示例：

| name | display_name | description |
|------|-------------|-------------|
| `ats` | ATS 简约 | 专为 ATS 系统优化的简洁排版 |
| `two-column` | 双栏布局 | 经典双栏布局，左侧边栏展示个人信息 |
| `minimal` | 极简风格 | 简约至上，留白充足 |
| `modern` | 现代风格 | 配色鲜艳，视觉冲击力强 |
| `professional` | 专业商务 | 商务场合首选，稳重得体 |
| `creative` | 创意风格 | 设计感强，适合创意类岗位 |
| `developer` | 开发者 | 代码风格，适合技术岗位 |
| `academic` | 学术风格 | 适合申请学术职位 |
| `legal` | 法律风格 | 适合法律行业 |
| `medical` | 医疗风格 | 适合医疗行业 |
| `executive` | 高管风格 | 适合高级管理职位 |
| `teacher` | 教师风格 | 适合教育行业 |
