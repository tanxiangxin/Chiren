## table_name：`resumes`

简历主表，存储简历的全局配置信息。

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | UUID | 简历唯一标识，主键 | `1f130bac-42b4-4415-8485-ee735134e573` |
| `user_id` | UUID | 所属用户ID，外键 | `8e22361d-cfc7-4f16-94d9-a3c05d55fcb5` |
| `workspace_id` | UUID | 所属 Workspace（顶级简历）的 ID，为空表示本身就是 Workspace | `null` / `3fa24c8d-...` |
| `title` | VARCHAR(100) | 简历标题 | `我的简历` |
| `template` | VARCHAR(50) | 模板名称，外键关联 `templates.name`，唯一标识一个模板 | `two-column` |
| `theme_config` | JSON | 主题配置，见下方详细结构 | 见 themeConfig 示例 |
| `is_default` | BOOLEAN | 是否为用户的默认简历 | `false` |
| `language` | VARCHAR(10) | 简历语言 | `zh` / `en` |
| `share_token` | VARCHAR(64) | 分享链接 Token，为空表示未开启分享 | `null` |
| `is_public` | BOOLEAN | 是否公开简历 | `false` |
| `share_password` | VARCHAR(128) | 分享密码（可选），为空表示无密码 | `null` |
| `view_count` | INT | 公开/分享页面的浏览次数 | `0` |
| `created_at` | DATETIME | 创建时间 | `2026-03-31T06:15:58.000Z` |
| `updated_at` | DATETIME | 最后更新时间 | `2026-04-01T03:07:37.000Z` |

---

## `theme_config` JSON 结构

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `primaryColor` | STRING | 主色调（HEX） | `#0f172a` |
| `accentColor` | STRING | 强调色（HEX） | `#6366f1` |
| `fontFamily` | STRING | 字体名称 | `Inter` |
| `fontSize` | STRING | 字号级别 | `small` / `medium` / `large` |
| `lineSpacing` | FLOAT | 行间距倍数 | `1.6` |
| `margin` | OBJECT | 页边距配置 | 见下方 margin 结构 |
| `sectionSpacing` | INT | 区块之间的间距（px） | `14` |
| `avatarStyle` | STRING | 头像样式 | `circle` / `oneInch` |

### `margin` 子结构

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `top` | INT | 上边距（px） | `20` |
| `right` | INT | 右边距（px） | `20` |
| `bottom` | INT | 下边距（px） | `20` |
| `left` | INT | 左边距（px） | `20` |

### themeConfig 完整示例

```json
{
  "primaryColor": "#0f172a",
  "accentColor": "#6366f1",
  "fontFamily": "Inter",
  "fontSize": "medium",
  "lineSpacing": 1.6,
  "margin": {
    "top": 20,
    "right": 20,
    "bottom": 20,
    "left": 20
  },
  "sectionSpacing": 14,
  "avatarStyle": "oneInch"
}
```

---

## 表关系

```
users (1) ───< (N) resumes
templates (1) ───< (N) resumes
resumes (1) ───< (N) resume_sections

-- Workspace 自关联（一对多）
resumes (workspace) (1) ───< (N) resumes (子简历)
```
