-- filenode覆盖视图，仅启用
CREATE OR REPLACE VIEW deployed_files AS
WITH ranked_files AS (
    SELECT
        fn.path_segments,
        fn.pack_id,
        fn.id            AS file_id,
        fn.parent_id     AS parent_id,
        pp.priority,
        fn.is_active     AS file_active,
        COALESCE(pp.is_active, FALSE) AS pack_active,
        fp.base_path,
        fn.depth,
        fn.is_dir,
        fn.is_deployed,
        ROW_NUMBER() OVER (
            PARTITION BY fn.path_segments
            ORDER BY pp.priority DESC
        ) AS rn
    FROM FileNode fn
    LEFT JOIN PriorityPack pp ON fn.pack_id = pp.pack_id
    LEFT JOIN FNPack fp ON fn.pack_id = fp.id
    WHERE pp.is_active = true
),
aggregated_files AS (
    SELECT
        path_segments,
        depth,
        LIST(STRUCT_PACK(
            pack_id      := pack_id,
            file_id      := file_id,
            parent_id    := parent_id,
            priority     := priority,
            file_active  := file_active,
            base_path    := base_path
        )) AS pairs
    FROM ranked_files
    GROUP BY path_segments, depth
),
top_priority_files AS (
    SELECT
        path_segments,
        pack_id      AS top_pack_id,
        file_id      AS top_file_id,
        parent_id    AS top_parent_id,
        priority     AS top_priority,
        file_active  AS top_file_active,
        pack_active  AS top_pack_active,
        base_path    AS top_base_path,
        is_dir       AS top_is_dir,
        is_deployed  AS top_is_deployed
    FROM ranked_files
    WHERE rn = 1
),
priority_fn_files AS (
    SELECT
        fn.path_segments,
        fn.pack_id                    AS top_pack_id,
        fn.id                         AS top_file_id,
        fn.parent_id                  AS top_parent_id,
        pp.priority                   AS top_priority,
        fn.is_active                  AS top_file_active,
        COALESCE(pp.is_active, TRUE)  AS top_pack_active,
        fp.base_path                  AS top_base_path,
        fn.is_dir                     AS top_is_dir,
        fn.is_deployed                AS top_is_deployed
    FROM PriorityFN pfn
    JOIN FileNode fn       ON pfn.main_id = fn.id
    LEFT JOIN PriorityPack pp ON fn.pack_id = pp.pack_id
    LEFT JOIN FNPack fp ON fn.pack_id = fp.id
    WHERE pfn.is_active = TRUE
)
SELECT
    a.path_segments,
    a.depth,
    a.pairs,
    COALESCE(pfn.top_pack_id,      tpf.top_pack_id)      AS top_pack_id,
    COALESCE(pfn.top_file_id,      tpf.top_file_id)      AS top_file_id,
    COALESCE(pfn.top_parent_id,    tpf.top_parent_id)    AS top_parent_id,
    COALESCE(pfn.top_priority,     tpf.top_priority)     AS top_priority,
    COALESCE(pfn.top_file_active,  tpf.top_file_active)  AS top_file_active,
    COALESCE(pfn.top_pack_active,  tpf.top_pack_active)  AS top_pack_active,
    COALESCE(pfn.top_base_path,    tpf.top_base_path)    AS top_base_path,
    COALESCE(pfn.top_is_dir,       tpf.top_is_dir)       AS top_is_dir,
    COALESCE(pfn.top_is_deployed,  tpf.top_is_deployed)  AS top_is_deployed,
FROM aggregated_files   a
LEFT JOIN top_priority_files tpf ON a.path_segments = tpf.path_segments
LEFT JOIN priority_fn_files  pfn ON a.path_segments = pfn.path_segments;