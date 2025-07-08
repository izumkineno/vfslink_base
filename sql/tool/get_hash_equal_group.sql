WITH duplicate_groups AS (
  SELECT 
    hash,
        LIST({
            'id': id,
            'pack_id': pack_id,
            'pack_path': (SELECT base_path FROM FNPack WHERE id = fn.pack_id),
            'path': full_path,
            'hash': hash
        }) AS conflict_group
  FROM FileNode fn
  WHERE hash IN (
    SELECT hash
    FROM FileNode
    WHERE is_dir = 0 
      AND hash IS NOT NULL
    GROUP BY hash
    HAVING COUNT(*) > 1  -- 只选择重复的哈希值
  )
  AND is_dir = 0  -- 只处理文件
  GROUP BY hash  -- 按哈希值分组
)
SELECT conflict_group
FROM duplicate_groups;