-- 基础文件信息表
-- 创建包表
CREATE TABLE IF NOT EXISTS FNPack (
    id UUID PRIMARY KEY,
    base_path VARCHAR NOT NULL,                                   -- 基础路径
    add_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP                  -- 添加时间
);

-- 创建文件节点表（核心表）
CREATE TABLE IF NOT EXISTS FileNode (
    id UUID PRIMARY KEY,
    pack_id UUID NOT NULL,
    parent_id UUID,
    name VARCHAR NOT NULL,

    path_segments VARCHAR[] NOT NULL,                             -- 相对路径切片
    is_dir bool NOT NULL,                                         -- 是否是文件夹
    is_active BOOLEAN DEFAULT TRUE NOT NULL,                      -- 文件启用状态  与文件夹无关
    is_deployed BOOLEAN DEFAULT FALSE NOT NULL,                   -- 文件部署状态  与文件夹无关
    size uint64,                                                  -- 文件大小 文件夹为NULL
    hash VARCHAR,                                                 -- 文件哈希 文件夹为NULL

    full_path VARCHAR GENERATED ALWAYS AS (array_to_string(path_segments, '/')) VIRTUAL,
    depth INT GENERATED ALWAYS AS (array_length(path_segments)) VIRTUAL,

);

-- 创建包优先级表（带状态） 
-- 包的覆盖状态使用查询获取
CREATE SEQUENCE IF NOT EXISTS priority_seq START 1 INCREMENT BY 1;
CREATE TABLE IF NOT EXISTS PriorityPack (
    pack_id UUID PRIMARY KEY,
    priority DOUBLE NOT NULL DEFAULT nextval('priority_seq'),    -- 包覆盖优先级
    is_deployed BOOLEAN DEFAULT FALSE NOT NULL,                  -- 包部署状态
    is_active BOOLEAN DEFAULT FALSE NOT NULL,                    -- 包启用状态
    UNIQUE (priority)
);

-- 文件覆盖优先级表（独立覆盖规则） 
-- 只记录顶部文件
-- 不与包信息的覆盖状态合并，仅在查询部署时合并
CREATE TABLE IF NOT EXISTS PriorityFN (
    path VARCHAR PRIMARY KEY,                                     -- 覆盖路径
    main_id UUID NOT NULL,                                        -- 主要文件id
    pack_id UUID NOT NULL,                                        -- 所属包id 
    is_active BOOLEAN DEFAULT TRUE NOT NULL,                      -- 覆盖规则启用状态
);
