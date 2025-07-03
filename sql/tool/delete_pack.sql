-- 删除元信息
DELETE FROM InfoBase
WHERE
    id = ?;

-- 删除独立覆盖规则
DELETE FROM PriorityFN
WHERE
    pack_id = ?;

-- 删除包的优先级设置
DELETE FROM PriorityPack
WHERE
    pack_id = ?;

-- 删除文件节点
DELETE FROM FileNode
WHERE
    pack_id = ?;

-- 删除包
DELETE FROM FNPack
WHERE
    id = ?;