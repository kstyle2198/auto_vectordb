pg_schema = [
    {'name': 'id', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'page_content', 'type': 'TEXT NOT NULL'}, 
    {'name': 'filename', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'filepath', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'hashed_filename', 'type': 'VARCHAR(300)'}, 
    {'name': 'hashed_filepath', 'type': 'VARCHAR(300)'}, 
    {'name': 'hashed_page_content', 'type': 'VARCHAR(300)'}, 
    {'name': 'page', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'lv1_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv2_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv3_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv4_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'embeddings', 'type': 'TEXT'}, 
    {"name": "created_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
    {"name": "updated_at", "type": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"}
    ]


maria_schema = [
    {'name': 'id', 'type': 'VARCHAR(300) NOT NULL PRIMARY KEY'}, 
    {'name': 'page_content', 'type': 'TEXT'}, 
    {'name': 'filename', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'filepath', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'hashed_filename', 'type': 'VARCHAR(300)'}, 
    {'name': 'hashed_filepath', 'type': 'VARCHAR(300)'}, 
    {'name': 'hashed_page_content', 'type': 'VARCHAR(300)'}, 
    {'name': 'page', 'type': 'VARCHAR(300) NOT NULL'}, 
    {'name': 'lv1_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv2_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv3_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'lv4_cat', 'type': 'VARCHAR(300)'}, 
    {'name': 'embeddings', 'type': 'TEXT'}, 
    {'name': 'created_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'},
    {'name': 'updated_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'}
]