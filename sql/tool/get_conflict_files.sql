select
    array_to_string (path_segments, '/') AS relative_path,
    top_file_id,
    top_pack_id,
    pairs
from
    deployed_files
where
    array_length (pairs) > 1
    and not top_is_dir