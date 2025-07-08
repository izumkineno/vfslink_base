#[cfg(test)]
mod test {

    use duckdb::arrow::{array::RecordBatch, util::pretty::print_batches};
    use uuid::Uuid;

    use vfslink_base::{FNDB, ListPack, ListTree, ViewOverTree, ViewPack};

    const PATH: &str = "example1.db";

    // 初始化
    #[test]
    fn test_init() {
        let mut fndb = FNDB::new(PATH);
        fndb.connect_rw();
        fndb.init();
        fndb.init_view();
    }

    mod table {
        use super::*;
        use vfslink_base::model_insert::InfoBase;

        // 查询包列表
        #[test]
        fn test_show_pack_list() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();

            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取值测试
            let res = ViewPack::GetPackInfo
                .execute(fndb.get_conn())
                .unwrap()
                .as_pack_info();
            println!("{:#?}", res);
        }

        // 插入包
        #[test]
        fn test_insert_pack() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 创建并保存第一个包
            let info = InfoBase {
                id: Uuid::new_v4(),
                name: "音乐包1".to_string(),
                tag: vec!["音乐".to_string(), "流行".to_string()],
                description: Some("第一个音乐包".to_string()),
                author: Some("作者1".to_string()),
                version: Some("1.0.0".to_string()),
            };
            ListPack::Insert(r"D:\CloudMusic\1", info).execute(fndb.get_conn());

            // 创建并保存第二个包
            let info = InfoBase {
                id: Uuid::new_v4(),
                name: "音乐包2".to_string(),
                tag: vec!["音乐".to_string(), "古典".to_string()],
                description: Some("第二个音乐包".to_string()),
                author: Some("作者2".to_string()),
                version: Some("1.0.0".to_string()),
            };
            ListPack::Insert(r"D:\CloudMusic\2", info).execute(fndb.get_conn());

            // 创建并保存第三个包
            let info = InfoBase {
                id: Uuid::new_v4(),
                name: "音乐包3".to_string(),
                tag: vec!["音乐".to_string(), "摇滚".to_string()],
                description: Some("第三个音乐包".to_string()),
                author: Some("作者3".to_string()),
                version: Some("1.0.0".to_string()),
            };
            ListPack::Insert(r"D:\CloudMusic\3", info).execute(fndb.get_conn());

            // 创建并保存第四个包
            let info = InfoBase {
                id: Uuid::new_v4(),
                name: "音乐包4".to_string(),
                tag: vec!["音乐".to_string(), "爵士".to_string()],
                description: Some("第四个音乐包".to_string()),
                author: Some("作者4".to_string()),
                version: Some("1.0.0".to_string()),
            };
            ListPack::Insert(r"D:\CloudMusic\4", info).execute(fndb.get_conn());

            // 查询
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }

        // 删除包
        #[test]
        fn test_delete_pack() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 查询
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取第一条记录的ID
            let id = stmt
                .query_map([], |row| {
                    let id: String = row.get(0).unwrap();
                    Ok(id)
                })
                .unwrap()
                .filter_map(|v| match v {
                    Ok(v) => Some(v),
                    Err(e) => {
                        println!("{:?}", e);
                        None
                    }
                })
                .collect::<Vec<_>>();
            let id = id[0].clone();

            let conn = fndb.get_conn();
            let sql = ListPack::RemoveById(&id);
            sql.execute(conn);

            // 查询
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }

        // 更新包状态
        #[test]
        fn test_update_pack_status() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
            // 循环更新所有的包状态
            let ids = stmt
                .query_map([], |row| {
                    let id: String = row.get(0).unwrap();
                    Ok(id)
                })
                .unwrap();

            for id in ids {
                let id = id.unwrap();
                ListPack::SetActive(&id, true).execute(fndb.get_conn());
            }

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }

        // 更新包优先级
        #[test]
        fn test_update_pack_priority() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取第一条记录的ID
            let id: String = stmt
                .query_map([], |row| {
                    let id: String = row.get(0).unwrap();
                    Ok(id)
                })
                .unwrap()
                .next()
                .unwrap()
                .unwrap();

            // 更新包优先级
            ListPack::SetPriority(&id, 10.1).execute(fndb.get_conn());

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }

        // 更新包信息
        #[test]
        fn test_update_pack_info() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();
            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取第一条记录的ID
            let id: String = stmt
                .query_map([], |row| {
                    let id: String = row.get(0).unwrap();
                    Ok(id)
                })
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            // 更新包信息
            let info = InfoBase {
                id: id.parse().unwrap(),
                name: "更新后的包名".to_string(),
                tag: vec!["更新".to_string(), "测试".to_string()],
                description: Some("这是一个更新后的包描述".to_string()),
                author: Some("更新后的作者".to_string()),
                version: Some("1.0.0".to_string()),
            };

            ListPack::SetInfo(&id, info).execute(fndb.get_conn());
            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }

        // 查询包下面的文件树
        #[test]
        fn test_get_file_tree() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取第一条记录的ID
            let id = stmt
                .query_map([], |row| {
                    let id: String = row.get(0).unwrap();
                    Ok(id)
                })
                .unwrap()
                .next()
                .unwrap()
                .unwrap();

            // 查询文件树
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetFileById(&id).to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取详细文件信息
            let rows = ViewPack::GetFileById(&id)
                .execute(fndb.get_conn())
                .unwrap()
                .as_file_tree_node();
            println!("{:#?}", rows);
        }

        // 更新文件状态
        #[test]
        fn test_update_file_status() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取第一条记录的ID
            let id = stmt
                .query_map([], |row| {
                    let id: String = row.get(0).unwrap();
                    Ok(id)
                })
                .unwrap()
                .next()
                .unwrap()
                .unwrap();

            // 查询文件树
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetFileById(&id).to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 获取第一条记录的ID
            let id = stmt
                .query_map([], |row| {
                    let id: String = row.get(0).unwrap();
                    Ok(id)
                })
                .unwrap()
                .next()
                .unwrap()
                .unwrap();

            // 更新文件状态
            ListTree::SetActive(&id, false).execute(fndb.get_conn());

            // 查询文件树
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetFileById(&id).to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }

        // 整理文件优先级
        #[test]
        fn test_update_file_priority() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            ListPack::ResetPriority.execute(fndb.get_conn());

            // 查询包列表
            let mut stmt = fndb
                .get_conn()
                .prepare(&ViewPack::GetPackInfo.to_sql())
                .unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }
    }

    mod tree {
        use super::*;

        // 查询覆盖树
        #[test]
        fn test_get_over_tree() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            // 查询顶层目录
            let sql = ViewOverTree::GetTop.to_sql();
            let mut stmt = fndb.get_conn().prepare(&sql).unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            // 查询子项
            let sql = ViewOverTree::GetChildren(&vec!["音乐".to_string()]).to_sql();
            let mut stmt = fndb.get_conn().prepare(&sql).unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();
        }

        // 获取所有覆盖叠加后的路径
        #[test]
        fn test_get_all_paths() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            let sql = ViewOverTree::GetAllPaths.to_sql();
            let mut stmt = fndb.get_conn().prepare(&sql).unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            let rows = ViewOverTree::GetAllPaths
                .execute(fndb.get_conn())
                .unwrap()
                .as_file_over_link_list();
            println!("{:?}", rows);
        }

        // 查询冲突文件
        #[test]
        fn test_get_conflict_files() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            let sql = ViewOverTree::GetConflictFiles("说明.txt").to_sql();
            let mut stmt = fndb.get_conn().prepare(&sql).unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            let t = vfslink_base::model_select::ConflictFileList::get_res(&mut stmt).unwrap();

            println!("{:#?}", t);
        }

        // 为冲突文件添加独立覆盖规则
        #[test]
        fn test_add_conflict_rule() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            let res = ViewOverTree::GetConflictFiles("说明.txt")
                .execute(fndb.get_conn())
                .unwrap();
            println!("{:#?}", res);

            let t = res.as_conflict_file_list();

            let path = &t[0].relative_path;
            let fid = &t[0].files[1].file_id;
            let pid = &t[0].files[1].pack_id;

            ListTree::RemoveCoverRule(path).execute(fndb.get_conn());

            let res = ViewOverTree::GetConflictFiles("说明.txt")
                .execute(fndb.get_conn())
                .unwrap();
            println!("{:#?}", res);

            ListTree::AddCoverRule(path, fid, pid).execute(fndb.get_conn());

            let res = ViewOverTree::GetConflictFiles("说明.txt")
                .execute(fndb.get_conn())
                .unwrap();
            println!("{:#?}", res);
        }

        // 检查指定id的哈希冲突列表
        #[test]
        fn test_check_hash_conflict() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            let res = ViewOverTree::GetHashEqualFiles("0cc6ab58-79a2-47ae-b253-66bbeeaf8f38")
                .execute(fndb.get_conn())
                .unwrap()
                .as_hash_equal_file_list();
            println!("{:#?}", res);
        }

        // 获取所有哈希冲突文件
        #[test]
        fn test_get_all_hash_conflict_files() {
            let mut fndb = FNDB::new(PATH);
            fndb.connect_rw();

            let sql = ViewOverTree::GetAllHashEqualFiles.to_sql();
            let mut stmt = fndb.get_conn().prepare(&sql).unwrap();
            let rbs: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
            print_batches(&rbs).unwrap();

            let res = ViewOverTree::GetAllHashEqualFiles
                .execute(fndb.get_conn())
                .unwrap()
                .as_hash_equal_file_group();
            println!("{:#?}", res);
        }

        //TODO：部署未部署的文件

        //TODO：移除所有部署文件

        //TODO：重新部署所有文件
    }
}
