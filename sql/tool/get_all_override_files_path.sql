-- 查询部署路径和绝对路径
SELECT
    top_file_id,
    top_pack_id,
    top_parent_id,
    array_to_string (path_segments, '/') AS relative_path,
    top_base_path || '/' || array_to_string (path_segments, '/') AS full_file_path,
    top_is_deployed
FROM
    deployed_files
WHERE
    top_file_active
    AND top_is_dir = false
ORDER BY
    full_file_path;