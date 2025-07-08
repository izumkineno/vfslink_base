use std::{borrow::Cow, fmt::Display};

use anyhow::Ok;
use duckdb::Connection;

pub mod model_insert;
pub mod model_select;

pub use model_insert::*;
pub use model_select::*;

const SQL_INIT: &'static str = include_str!(r"..\sql\init\core.sql");
const SQL_INIT_INFO: &'static str = include_str!(r"..\sql\init\info.sql");
const SQL_VIEW_AOFS: &'static str = include_str!(r"..\sql\view\path_override_files.sql");
const SQL_VIEW_PACK: &'static str = include_str!(r"..\sql\view\pack.sql");

/// 文件数据库
#[derive(Debug)]
pub struct FNDB {
    path: String,
    instance_w: Option<Connection>,
}

impl FNDB {
    /// 创建新的数据库实例
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            instance_w: None,
        }
    }

    /// 连接数据库（读写）
    pub fn connect_rw(&mut self) {
        // 只在此处引入 duckdb::Connection
        self.instance_w = Some(duckdb::Connection::open(&self.path).unwrap());
    }

    /// 获取数据库连接
    pub fn get_conn(&self) -> &duckdb::Connection {
        self.instance_w.as_ref().unwrap()
    }

    /// 断开数据库连接
    pub fn disconnect_w(&mut self) {
        if self.instance_w.is_none() {
            return;
        };
        if let Some(conn) = self.instance_w.take() {
            conn.close().unwrap();
        }
        self.instance_w = None;
    }

    /// 初始化数据库表结构
    pub fn init(&self) {
        if self.instance_w.is_none() {
            return;
        }
        self.get_conn().execute_batch(&SQL_INIT).unwrap();
        self.get_conn().execute_batch(&SQL_INIT_INFO).unwrap();
    }

    /// 初始化视图
    pub fn init_view(&self) {
        if self.instance_w.is_none() {
            return;
        }
        self.get_conn().execute_batch(&SQL_VIEW_AOFS).unwrap();
        self.get_conn().execute_batch(&SQL_VIEW_PACK).unwrap();
    }
}

/// 覆盖树视图
pub enum ViewOverTree<'a> {
    /// 获取顶层目录
    GetTop,
    /// 获取子目录，参数为目录路径
    GetChildren(&'a Vec<String>),
    /// 获取所有路径
    GetAllPaths,
    /// 获取路径冲突文件 参数为文件路径
    GetConflictFiles(&'a str),
    /// 获取哈希相等的文件列表
    GetHashEqualFiles(&'a str),
    /// 获取所有哈希冲突的文件
    GetAllHashEqualFiles,
}

impl<'a> ViewOverTree<'a> {
    const SQL_GET_: &'static str = {
        r#"select
            top_file_id,
            top_pack_id,
            top_parent_id,
            path_segments,
            top_is_dir,
            top_file_active,
            top_is_deployed
        from deployed_files
        where depth = 
        "#
    };

    const SQL_TOOL_GET_ALL_PATHS: &'static str =
        include_str!(r"..\sql\tool\get_all_override_files_path.sql");

    const SQL_GET_CONFLICT_FILES: &'static str =
        include_str!(r"..\sql\tool\get_conflict_files.sql");

    const SQL_GET_HASH_EQUAL_FILES: &'static str =
        include_str!(r"..\sql\tool\get_hash_equal_files.sql");

    const SQL_GET_ALL_HASH_EQUAL_FILES: &'static str =
        include_str!(r"..\sql\tool\get_hash_equal_group.sql");
}

#[derive(Debug)]
pub enum ResultOverTree {
    TreeList(Vec<FileOverTree>),
    LinkList(Vec<FileOverLinkList>),
    ConflictFileList(Vec<ConflictFileList>),
    HashEqualFileList(Vec<HashEqualFiles>),
    HashEqualFileGroup(Vec<Vec<HashEqualFiles>>),
}

impl ResultOverTree {
    pub fn as_file_over_tree(self) -> Vec<FileOverTree> {
        if let Self::TreeList(list) = self {
            list
        } else {
            vec![]
        }
    }
    pub fn as_file_over_link_list(self) -> Vec<FileOverLinkList> {
        if let Self::LinkList(list) = self {
            list
        } else {
            vec![]
        }
    }
    pub fn as_conflict_file_list(self) -> Vec<ConflictFileList> {
        if let Self::ConflictFileList(list) = self {
            list
        } else {
            vec![]
        }
    }
    pub fn as_hash_equal_file_list(self) -> Vec<HashEqualFiles> {
        if let Self::HashEqualFileList(list) = self {
            list
        } else {
            vec![]
        }
    }
    pub fn as_hash_equal_file_group(self) -> Vec<Vec<HashEqualFiles>> {
        if let Self::HashEqualFileGroup(list) = self {
            list
        } else {
            vec![]
        }
    }
}

impl<'a> ViewOverTree<'a> {
    pub fn execute(&self, conn: &duckdb::Connection) -> anyhow::Result<ResultOverTree> {
        let sql = self.to_sql();
        let mut stmt = conn.prepare(&sql)?;

        // 根据变体选择解析函数
        match self {
            Self::GetTop | Self::GetChildren(_) => {
                let result = FileOverTree::get_res(&mut stmt)?;
                Ok(ResultOverTree::TreeList(result))
            }
            Self::GetAllPaths => {
                let result = FileOverLinkList::get_res(&mut stmt)?;
                Ok(ResultOverTree::LinkList(result))
            }
            Self::GetConflictFiles(_) => {
                let result = ConflictFileList::get_res(&mut stmt)?;
                Ok(ResultOverTree::ConflictFileList(result))
            }
            Self::GetHashEqualFiles(_) => {
                let result = HashEqualFiles::get_res(&mut stmt)?;
                Ok(ResultOverTree::HashEqualFileList(result))
            }
            Self::GetAllHashEqualFiles => {
                let result = HashEqualFiles::get_res_group(&mut stmt)?;
                Ok(ResultOverTree::HashEqualFileGroup(result))
            }
        }
    }

    #[inline]
    pub fn to_sql(&self) -> Cow<'static, str> {
        use ViewOverTree::*;
        match self {
            GetTop => (Self::SQL_GET_.to_string() + " 1").into(),
            GetChildren(path_segments) => {
                let base = (Self::SQL_GET_.to_string() + &(path_segments.len() + 1).to_string())
                    .to_string();
                let sql = format!(
                    "{} and path_segments[:{}] = {:?}",
                    base,
                    path_segments.len(),
                    path_segments
                )
                .replace("\"", "'");
                sql.into()
            }
            GetAllPaths => Self::SQL_TOOL_GET_ALL_PATHS.into(),
            GetConflictFiles(path) => format!(
                "{} and relative_path = '{}'",
                Self::SQL_GET_CONFLICT_FILES,
                path
            )
            .into(),
            GetHashEqualFiles(path) => Self::SQL_GET_HASH_EQUAL_FILES.replace('$', path).into(),
            GetAllHashEqualFiles => Self::SQL_GET_ALL_HASH_EQUAL_FILES.into(),
        }
    }
}

/// 包视图
pub enum ViewPack<'a> {
    /// 获取包的信息视图列表
    GetPackInfo,
    /// 获取指定id包的带树关系的文件的视图
    GetFileById(&'a str),
}

impl ViewPack<'_> {
    const SQL_GET_LIST: &'static str = {
        r#"
        from view_pack_info
        select
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
            meta_updated_at
        "#
    };

    const SQL_GET_: &'static str = {
        r#"
        SELECT
        f.id,
        f.name,
        f.path_segments,
        f.is_dir,
        f.size,
        f.hash,
        f.is_active,
        f.is_deployed,
        f.depth
        FROM
        FileNode f
        "#
    };
}

#[derive(Debug)]
pub enum ResultPack {
    InfoList(Vec<PackInfo>),
    FileList(Vec<FileTreeNode>),
}

impl ResultPack {
    pub fn as_pack_info(self) -> Vec<PackInfo> {
        if let Self::InfoList(list) = self {
            list
        } else {
            vec![]
        }
    }
    pub fn as_file_tree_node(self) -> Vec<FileTreeNode> {
        if let Self::FileList(list) = self {
            list
        } else {
            vec![]
        }
    }
}

impl<'a> ViewPack<'a> {
    pub fn execute(&self, conn: &duckdb::Connection) -> anyhow::Result<ResultPack> {
        let sql = self.to_sql();
        let mut stmt = conn.prepare(&sql)?;

        // 根据变体选择解析函数
        match self {
            Self::GetPackInfo => {
                let result = PackInfo::get_res(&mut stmt)?;
                Ok(ResultPack::InfoList(result))
            }
            Self::GetFileById(_) => {
                let result = FileTreeNode::get_res(&mut stmt)?;
                Ok(ResultPack::FileList(result))
            }
        }
    }

    #[inline]
    pub fn to_sql(&self) -> Cow<'static, str> {
        use ViewPack::*;
        match self {
            GetPackInfo => (Self::SQL_GET_LIST.to_string() + "order by priority asc").into(),
            GetFileById(id) => {
                let sql = format!(
                    "{} WHERE f.pack_id = '{}' ORDER BY f.path_segments;",
                    Self::SQL_GET_,
                    id
                );
                sql.into()
            }
        }
    }
}

/// 包的管理
pub enum ListPack<'a> {
    /// 插入包 参数为包的路径、基础信息
    Insert(&'a str, InfoBase),
    /// 删除包 参数为包的id
    RemoveById(&'a str),
    /// 更新包的活动状态，参数为文件id、是否活动
    SetActive(&'a str, bool),
    // 更新部署状态，参数为文件id、是否部署
    SetDeployed(&'a str, bool),
    /// 更新包的优先级，参数为文件id、优先级
    SetPriority(&'a str, f64),
    /// 更新包的基础信息
    SetInfo(&'a str, InfoBase),
    /// 整理优先级，去除因为插入导致的小数，但不重置计数器
    ResetPriority,
}

impl<'a> ListPack<'a> {
    const SQL_REMOVE_PACK: &'static str = include_str!(r"..\sql\tool\delete_pack.sql");
    const SQL_TOOL_RESET_PRIORITY: &'static str = include_str!(r"..\sql\tool\reset_priority.sql");

    fn set_pack_sql(key: &str, value: impl Display, id: &str) -> String {
        format!(
            "UPDATE PriorityPack SET {} = {} WHERE pack_id = '{}';",
            key, value, id
        )
    }
    /// 执行包相关操作
    pub fn execute(&self, conn: &Connection) {
        use ListPack::*;
        match self {
            Insert(path, info) => {
                // 创建并保存第一个包
                let mut root = FNPack::new(path, info.id);
                root.walk_dir();
                root.save_all_files_to_db(conn);
                conn.execute(&info.to_sql(), []).unwrap();
            }
            RemoveById(id) => {
                let sql = Self::SQL_REMOVE_PACK.replace('?', &format!("'{}'", id));
                conn.execute_batch(&sql).unwrap();
            }
            SetActive(file_id, is_active) => {
                let sql = Self::set_pack_sql("is_active", is_active, file_id);
                conn.execute(&sql, []).unwrap();
            }
            SetDeployed(file_id, is_deployed) => {
                let sql = Self::set_pack_sql("is_deployed", is_deployed, file_id);
                conn.execute(&sql, []).unwrap();
            }
            SetPriority(file_id, priority) => {
                let sql = Self::set_pack_sql("priority", priority, file_id);
                conn.execute(&sql, []).unwrap();
            }
            SetInfo(file_id, info) => {
                // 删除包的信息
                let sql = format!("DELETE FROM InfoBase WHERE id = '{}';", file_id);
                conn.execute(&sql, []).unwrap();
                // 插入新的信息
                let sql = info.to_sql();
                conn.execute_batch(&sql).unwrap();
            }
            ResetPriority => {
                conn.execute_batch(&Self::SQL_TOOL_RESET_PRIORITY).unwrap();
            }
        }
    }
}

/// 文件管理
pub enum ListTree<'a> {
    /// 更新文件的活动状态，参数为文件id、是否活动
    SetActive(&'a str, bool),
    /// 设置部署状态
    SetDeployed(&'a str, bool),
    /// 添加独立覆盖规则
    AddCoverRule(&'a str, &'a str, &'a str),
    /// 移除独立覆盖规则
    RemoveCoverRule(&'a str),
    /// 按包id移除独立覆盖规则
    RemoveCoverRuleByPackId(&'a str),
}

impl ListTree<'_> {
    fn set_file_node_sql(key: &str, value: impl Display, id: &str) -> String {
        format!(
            "UPDATE FileNode SET {} = {} WHERE id = '{}';",
            key, value, id
        )
    }

    /// 生成 SQL 或执行操作
    pub fn execute(&self, conn: &Connection) {
        use ListTree::*;
        match self {
            SetActive(file_id, is_active) => {
                let sql = Self::set_file_node_sql("is_active", is_active, file_id);
                conn.execute(&sql, []).unwrap();
            }
            SetDeployed(file_id, is_deployed) => {
                let sql = Self::set_file_node_sql("is_deployed", is_deployed, file_id);
                conn.execute(&sql, []).unwrap();
            }
            AddCoverRule(path, file_id, pack_id) => {
                let sql = format!(
                    "insert into PriorityFN (path, main_id, pack_id) values ('{}', '{}', '{}');",
                    path, file_id, pack_id
                );
                conn.execute(&sql, []).unwrap();
            }
            RemoveCoverRule(file_id) => {
                let sql = format!("DELETE FROM PriorityFN WHERE path = '{}';", file_id);
                conn.execute(&sql, []).unwrap();
            }
            RemoveCoverRuleByPackId(pack_id) => {
                let sql = format!("DELETE FROM PriorityFN WHERE pack_id = '{}';", pack_id);
                conn.execute(&sql, []).unwrap();
            }
        }
    }
}
