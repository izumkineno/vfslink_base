use std::sync::Arc;

use hashbrown::HashMap;
use parking_lot::Mutex;
use rayon::iter::{ParallelBridge, ParallelIterator};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use walkdir::WalkDir;

/// 文件节点
pub struct FileNode {
    /// 唯一id
    id: Uuid,
    /// packid
    pack_id: Uuid,
    /// 父节点id
    parent_id: Option<Uuid>,
    /// 文件名
    name: String,
    /// 相对路径切片
    path_segments: Vec<String>,
    /// 是否是文件夹
    is_dir: bool,
    /// 哈希
    hash: Option<String>,
    /// 大小
    size: Option<u64>,
}

impl FileNode {
    /// 创建新的文件节点
    pub fn new(
        id: Uuid,
        pack_id: Uuid,
        parent_id: Option<Uuid>,
        name: String,
        path_segments: Vec<String>,
        is_dir: bool,
        hash: Option<String>,
        size: Option<u64>,
    ) -> Self {
        Self {
            id,
            pack_id,
            parent_id,
            name,
            path_segments,
            is_dir,
            hash,
            size,
        }
    }

    /// 生成插入 FileNode 的 SQL 语句
    pub fn to_sql(&self) -> String {
        let array_str = self
            .path_segments
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<String>>()
            .join(", ");

        let parent_id_str = match self.parent_id {
            Some(pid) => format!("'{}'", pid),
            None => "NULL".to_string(),
        };

        let hash_str = match &self.hash {
            Some(h) => format!("'{}'", h),
            None => "NULL".to_string(),
        };

        let size_str = match self.size {
            Some(s) => s.to_string(),
            None => "NULL".to_string(),
        };

        let sql = format!(
            "INSERT INTO FileNode (id, pack_id, parent_id, name, path_segments, is_dir,  size, hash) 
            VALUES ('{}', '{}', {}, '{}', ARRAY[{}], {}, {}, {});",
            self.id, self.pack_id, parent_id_str, self.name, array_str, self.is_dir, size_str, hash_str
        );

        sql
    }
}

/// 文件包
pub struct FNPack {
    /// 唯一id
    id: Uuid,
    /// 存放路径
    path: String,
    /// 字节点表
    children: Option<Vec<FileNode>>,
}

impl FNPack {
    /// 创建新的文件包
    pub fn new(path: &str, id: Uuid) -> Self {
        Self {
            id,
            path: path.to_string(),
            children: None,
        }
    }

    /// 获取包的 UUID
    pub fn get_uuid(&self) -> Uuid {
        self.id
    }

    /// 遍历目录，生成文件节点
    pub fn walk_dir(&mut self) {
        let pack_id = self.id;
        let path = &self.path;

        let id_map = Arc::new(Mutex::new(HashMap::new())); // path -> id
        let res = WalkDir::new(path)
            .follow_links(false)
            .min_depth(1)
            .into_iter()
            .par_bridge()
            .filter_map(|e| e.ok())
            .map(|entry| {
                let id = Uuid::new_v4();
                let path = entry.path().to_path_buf();
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().is_dir();

                let size = match is_dir {
                    false => Some(entry.metadata().unwrap().len()),
                    _ => None,
                };

                let hash = match is_dir {
                    false => {
                        let mut hasher = blake3::Hasher::new();
                        if let Some(size) = size {
                            if size >= 16 * 1024 {
                                // 对于大文件，使用内存映射
                                hasher.update_mmap(&path).unwrap();
                            } else {
                                // 对于小文件，使用流式读取
                                let mut file = std::fs::File::open(&path).unwrap();
                                hasher.update_reader(&mut file).unwrap();
                            }
                        }
                        let result = hasher.finalize();
                        Some(result.to_hex().to_string())
                    }
                    _ => None,
                };

                {
                    let mut map = id_map.lock();
                    map.insert(path.clone(), id);
                }

                if let Some(parent_path) = path.parent() {
                    let pid = {
                        let map = id_map.lock();
                        map.get(parent_path).cloned().unwrap_or(pack_id)
                    };

                    let path_segments = {
                        let curr_path = path.to_str().unwrap();
                        let root_path = self.path.clone();
                        let relative_path = curr_path.strip_prefix(&root_path).unwrap();
                        let segments: Vec<&str> = relative_path
                            .split(|c| c == '/' || c == '\\')
                            .filter(|s| !s.is_empty()) // 过滤掉空字符串
                            .collect();
                        segments
                            .iter()
                            .map(|&s| s.to_string())
                            .collect::<Vec<String>>()
                    };

                    Some(FileNode::new(
                        id,
                        pack_id,
                        Some(pid),
                        name,
                        path_segments,
                        is_dir,
                        hash,
                        size,
                    ))
                } else {
                    None
                }
            })
            .filter_map(|v| v)
            .collect::<Vec<_>>();

        self.children = Some(res);
    }

    /// 生成插入 FNPack 的 SQL 语句
    pub fn to_sql(&self) -> String {
        let sql = format!(
            "INSERT INTO FNPack (id, base_path) VALUES ('{}', '{}');",
            self.id, self.path
        );
        // println!("FNPack sql: {}", sql);
        sql
    }

    /// 保存所有文件节点到数据库
    pub fn save_all_files_to_db(&self, conn: &duckdb::Connection) {
        if self.children.is_none() {
            return;
        }

        let sql = self.to_sql();
        conn.execute(&sql, []).unwrap();

        self.children
            .as_ref()
            .unwrap()
            .into_iter()
            .for_each(|file_node| {
                let sql = file_node.to_sql();
                conn.execute(&sql, []).unwrap();
            });

        conn.execute(
            format!(
                "INSERT INTO PriorityPack (pack_id)
                VALUES ('{}');",
                self.id
            )
            .as_str(),
            [],
        )
        .unwrap();
    }
}

/// 基础信息表对应结构体
#[derive(Debug, Serialize, Deserialize)]
pub struct InfoBase {
    /// 唯一id
    pub id: Uuid,
    /// 名称
    pub name: String,
    /// 标签列表
    pub tag: Vec<String>,
    /// 描述信息
    pub description: Option<String>,
    /// 作者
    pub author: Option<String>,
    /// 版本号
    pub version: Option<String>,
}

impl InfoBase {
    /// 生成插入 InfoBase 的 SQL 语句
    pub fn to_sql(&self) -> String {
        let tag_array = self
            .tag
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<String>>()
            .join(", ");

        let description_str = self
            .description
            .as_ref()
            .map_or("NULL".to_string(), |s| format!("'{}'", s));

        let author_str = self
            .author
            .as_ref()
            .map_or("NULL".to_string(), |s| format!("'{}'", s));

        let version_str = self
            .version
            .as_ref()
            .map_or("NULL".to_string(), |s| format!("'{}'", s));

        format!(
            "INSERT INTO InfoBase (id, name, tags, description, author, version)
            VALUES ('{}', '{}', ARRAY[{}], {}, {}, {});",
            self.id, self.name, tag_array, description_str, author_str, version_str
        )
    }
}
