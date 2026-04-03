-- 简历模板表
CREATE TABLE IF NOT EXISTS templates (
    id TEXT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(100) NOT NULL,
    preview_image_url VARCHAR(255),
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

-- 简历主表
CREATE TABLE IF NOT EXISTS resumes (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    workspace_id TEXT,
    title VARCHAR(100) NOT NULL,
    template VARCHAR(50) NOT NULL,
    theme_config TEXT,
    is_default BOOLEAN NOT NULL DEFAULT 0,
    language VARCHAR(10) NOT NULL DEFAULT 'zh',
    share_token VARCHAR(64),
    is_public BOOLEAN NOT NULL DEFAULT 0,
    share_password VARCHAR(128),
    view_count INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (template) REFERENCES templates(name)
);

CREATE INDEX IF NOT EXISTS idx_resumes_user_id ON resumes(user_id);
CREATE INDEX IF NOT EXISTS idx_resumes_workspace_id ON resumes(workspace_id);

-- 简历区块表
CREATE TABLE IF NOT EXISTS resume_sections (
    id TEXT PRIMARY KEY,
    resume_id TEXT NOT NULL,
    type VARCHAR(50) NOT NULL,
    title VARCHAR(100) NOT NULL,
    sort_order INT NOT NULL DEFAULT 0,
    visible BOOLEAN NOT NULL DEFAULT 1,
    content TEXT,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (resume_id) REFERENCES resumes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_resume_sections_resume_id ON resume_sections(resume_id);
CREATE INDEX IF NOT EXISTS idx_resume_sections_type ON resume_sections(type);

-- 工作任务表
CREATE TABLE IF NOT EXISTS work (
    id TEXT PRIMARY KEY,
    file_name VARCHAR(100),
    src VARCHAR(100) NOT NULL,
    status VARCHAR(5) NOT NULL,
    template VARCHAR(20),
    title VARCHAR(20),
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
);
