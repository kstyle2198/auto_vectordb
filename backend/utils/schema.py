pg_schema = [
    {'name': 'id', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'hashed_file', 'type': 'VARCHAR(64) UNIQUE NOT NULL'}, 
    {'name': 'page_content', 'type': 'TEXT NOT NULL'}, 
    {'name': 'metadata', 'type': 'JSONB'}, 
    {'name': 'dense_embeddings', 'type': 'VECTOR(1024)'}, 
    {'name': 'sparse_embeddings', 'type': 'JSONB'}, 
    {"name": "created_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
    {"name": "updated_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"}
    ]
