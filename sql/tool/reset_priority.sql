
-- 重新编号为连续整数（保持顺序）
UPDATE PriorityPack 
SET priority = ranked.new_priority
FROM (
    SELECT 
        pack_id, 
        ROW_NUMBER() OVER (ORDER BY priority) AS new_priority
    FROM PriorityPack
) ranked
WHERE PriorityPack.pack_id = ranked.pack_id;

