-- 基础信息
CREATE TABLE IF NOT EXISTS InfoBase (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,                    
    tags TEXT[],                               
    description TEXT,                       
    author TEXT,                            
    version TEXT,                           
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);
