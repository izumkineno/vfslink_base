WITH target_file AS (
  SELECT hash, pack_id 
  FROM FileNode 
  WHERE id = '$' AND hash IS NOT NULL
),
duplicate_hashes AS (
  SELECT hash
  FROM FileNode 
  WHERE hash = (SELECT hash FROM target_file)
    AND id <> '$'
    AND is_dir = false
)
SELECT 
  fn.id AS file_id,
  fn.pack_id,
  p.base_path,
  fn.full_path,
  fn.hash
FROM FileNode fn
JOIN FNPack p ON fn.pack_id = p.id
WHERE fn.hash IN (SELECT hash FROM duplicate_hashes)
  AND fn.is_dir = false
ORDER BY fn.hash, fn.pack_id;