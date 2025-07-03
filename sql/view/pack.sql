-- 主要pack信息视图
CREATE
OR REPLACE VIEW view_pack_info AS
SELECT
    p.id,
    p.base_path,
    p.add_time,
    -- PriorityPack 信息
    pr.priority,
    pr.is_deployed,
    pr.is_active,
    -- 元信息（可选，可能为空）
    b.name,
    b.tags,
    b.description,
    b.author,
    b.version,
    b.update_at AS meta_updated_at
FROM
    FNPack p
    LEFT JOIN PriorityPack pr ON p.id = pr.pack_id
    LEFT JOIN InfoBase b ON p.id = b.id