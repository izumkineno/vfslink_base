use duckdb::{types::Value, Statement};
use serde::{Deserialize, Serialize};


/// 包信息结构体
#[derive(Debug, Serialize)]
pub struct PackInfo {
    /// 包的唯一标识符
    pub id: String,
    /// 包的基础路径
    pub base_path: String,
    /// 包的优先级
    pub priority: i32,
    /// 包的名称
    pub name: Option<String>,
    /// 包的标签列表
    pub tags: Vec<String>,
    /// 包的描述信息
    pub description: Option<String>,
    /// 包的作者
    pub author: Option<String>,
    /// 包的版本号
    pub version: Option<String>,
    /// 包是否已部署
    pub is_deployed: bool,
    /// 包是否处于激活状态
    pub is_active: bool,
    /// 包的添加时间（毫秒时间戳）
    pub add_time: i64,
    /// 包的元数据最后更新时间（毫秒时间戳）
    pub meta_updated_at: i64,
}

impl PackInfo {
    /// 从数据库连接中获取所有包信息
    #[inline]
    pub fn get_res(stmt: &mut Statement<'_>) -> anyhow::Result<Vec<Self>> {
        // 局部引入 duckdb 相关类型
        use duckdb::types::{TimeUnit, Value};

        // 查询并映射为 PackInfo 结构体
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let base_path: String = row.get(1)?;
            let priority: i32 = row.get(2)?;
            let name: Option<String> = row.get(3)?;
            let tags: Value = row.get(4)?;
            let description: Option<String> = row.get(5)?;
            let author: Option<String> = row.get(6)?;
            let version: Option<String> = row.get(7)?;
            let is_deployed: bool = row.get(8)?;
            let is_active: bool = row.get(9)?;
            let add_time: Value = row.get(10)?;
            let meta_updated_at: Value = row.get(11)?;

            // 处理 tags 字段为 Vec<String>
            let tags = tags.into_inner_as::<Vec<Value>>().unwrap_or(vec![]);
            let tags: Vec<String> = tags
                .into_iter()
                .map(|v| v.into_inner_as::<String>())
                .filter_map(|v| v)
                .collect();

            // 处理时间戳字段为 i64（毫秒）
            let add_time = add_time.into_inner_as::<(TimeUnit, i64)>().unwrap();
            let add_time = add_time.0.to_micros(add_time.1) / 1000;
            let meta_updated_at = meta_updated_at.into_inner_as::<(TimeUnit, i64)>().unwrap();
            let meta_updated_at = meta_updated_at.0.to_micros(meta_updated_at.1) / 1000;

            Ok(PackInfo {
                id,
                base_path,
                priority,
                name,
                tags,
                description,
                author,
                version,
                is_deployed,
                is_active,
                add_time,
                meta_updated_at,
            })
        })?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}

/// 文件树节点结构体
#[derive(Debug, Serialize, Clone)]
pub struct FileTreeNode {
    /// 文件的唯一标识符
    pub id: String,
    /// 文件名称
    pub name: String,
    /// 文件路径段
    pub path_segments: Vec<String>,
    /// 是否为目录
    pub is_dir: bool,
    /// 文件大小
    pub size: Option<i64>,
    /// 文件哈希值
    pub hash: Option<String>,
    /// 是否处于激活状态
    pub is_active: bool,
    /// 是否已部署
    pub is_deployed: bool,
    /// 在树中的深度
    pub depth: i32,
}

impl FileTreeNode {
    /// 根据包ID获取文件树
    #[inline]
    pub fn get_res(stmt: &mut Statement<'_>) -> anyhow::Result<Vec<Self>> {
        use duckdb::types::Value;

        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let path_segments: Value = row.get(2)?;
            let is_dir: bool = row.get(3)?;
            let size: Option<i64> = row.get(4)?;
            let hash: Option<String> = row.get(5)?;
            let is_active: bool = row.get(6)?;
            let is_deployed: bool = row.get(7)?;
            let depth: i32 = row.get(8)?;

            // 处理 path_segments 字段为 Vec<String>
            let path_segments = path_segments
                .into_inner_as::<Vec<Value>>()
                .unwrap_or(vec![]);
            let path_segments: Vec<String> = path_segments
                .into_iter()
                .map(|v| v.into_inner_as::<String>())
                .filter_map(|v| v)
                .collect();

            Ok(FileTreeNode {
                id,
                name,
                path_segments,
                is_dir,
                size,
                hash,
                is_active,
                is_deployed,
                depth,
            })
        })?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}



/// 文件树视图结构体
#[derive(Debug, Serialize, Clone)]
pub struct FileOverTree {
    /// 文件的唯一标识符
    pub file_id: String,
    /// 文件所属的包的唯一标识符
    pub pack_id: String,
    /// 文件的父节点的唯一标识符
    pub parent_id: Option<String>,
    /// 文件路径段
    pub path_segments: Vec<String>,
    /// 是否为目录
    pub is_dir: bool,
    /// 是否处于激活状态
    pub is_active: bool,
    /// 是否已部署
    pub is_deployed: bool,
}

impl FileOverTree {
    /// 获取覆盖树文件列表
    #[inline]
    pub fn get_res(stmt: &mut Statement<'_>) -> anyhow::Result<Vec<Self>> {
        use duckdb::types::Value;

        let rows = stmt.query_map([], |row| {
            let file_id: String = row.get(0)?;
            let pack_id: String = row.get(1)?;
            let parent_id: Option<String> = row.get(2)?;
            let path_segments: Value = row.get(3)?;
            let is_dir: bool = row.get(4)?;
            let is_active: bool = row.get(5)?;
            let is_deployed: bool = row.get(6)?;

            // 处理 path_segments 字段为 Vec<String>
            let path_segments = path_segments
                .into_inner_as::<Vec<Value>>()
                .unwrap_or(vec![]);
            let path_segments: Vec<String> = path_segments
                .into_iter()
                .map(|v| v.into_inner_as::<String>())
                .filter_map(|v| v)
                .collect();

            Ok(FileOverTree {
                file_id,
                pack_id,
                parent_id,
                path_segments,
                is_active,
                is_dir,
                is_deployed,
            })
        })?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct FileOverLinkList {
    pub file_id: String,
    pub pack_id: String,
    pub parent_id: Option<String>,
    pub path_relative: String,
    pub path_absolute: String,
    pub is_deployed: bool,
}

impl FileOverLinkList {
    /// 获取覆盖链接列表
    #[inline]
    pub fn get_res(stmt: &mut Statement<'_>) -> anyhow::Result<Vec<Self>> {

        let rows = stmt.query_map([], |row| {
            let file_id: String = row.get(0)?;
            let pack_id: String = row.get(1)?;
            let parent_id: Option<String> = row.get(2)?;
            let path_relative: String = row.get(3)?;
            let path_absolute: String = row.get(4)?;
            let is_deployed: bool = row.get(5)?;

            Ok(FileOverLinkList {
                file_id,
                pack_id,
                parent_id,
                path_relative,
                path_absolute,
                is_deployed,
            })
        })?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConflictFile {
    /// 包id
    pub pack_id: String,
    /// 文件id
    pub file_id: String,
    /// 父节点id
    pub parent_id: String,
    /// 文件优先级
    pub priority: f64,
    /// 文件启用状态
    pub file_active: bool,
    /// 包路径    
    pub base_path: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ConflictFileList {
    /// 相对路径
    pub relative_path: String,
    /// 文件id
    pub file_id: String,
    /// 包id
    pub pack_id: String,
    /// 冲突文件列表
    pub files: Vec<ConflictFile>,
}

impl ConflictFileList {
    /// 从查询结果中获取冲突文件列表
    pub fn get_res(stmt: &mut Statement<'_>) -> anyhow::Result<Vec<Self>> {
        let rows = stmt.query_map([], |row| {
            let relative_path: String = row.get(0)?;
            let file_id: String = row.get(1)?;
            let pack_id: String = row.get(2)?;

            let files: Value = row.get(3)?;
            let json_value = serde_json::to_value(&files).unwrap();
            let files: Vec<ConflictFile> = serde_json::from_value(json_value).unwrap();

            Ok(ConflictFileList {
                relative_path,
                file_id,
                pack_id,
                files,
            })
        })?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}