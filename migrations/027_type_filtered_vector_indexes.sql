-- Typed expressions prevent the planner from choosing the global graph.
-- Keep the global index and retain all lifecycle/model rows in each type graph.
CREATE INDEX IF NOT EXISTS memories_embedding_entity_idx ON braincore.memories
  USING hnsw (((CASE WHEN object_type = 'entity' THEN embedding ELSE NULL END)::vector(384))
    vector_cosine_ops)
  WHERE embedding IS NOT NULL AND object_type = 'entity';
CREATE INDEX IF NOT EXISTS memories_embedding_fact_idx ON braincore.memories
  USING hnsw (((CASE WHEN object_type = 'fact' THEN embedding ELSE NULL END)::vector(384))
    vector_cosine_ops)
  WHERE embedding IS NOT NULL AND object_type = 'fact';
CREATE INDEX IF NOT EXISTS memories_embedding_memory_idx ON braincore.memories
  USING hnsw (((CASE WHEN object_type = 'memory' THEN embedding ELSE NULL END)::vector(384))
    vector_cosine_ops)
  WHERE embedding IS NOT NULL AND object_type = 'memory';
CREATE INDEX IF NOT EXISTS memories_embedding_segment_idx ON braincore.memories
  USING hnsw (((CASE WHEN object_type = 'segment' THEN embedding ELSE NULL END)::vector(384))
    vector_cosine_ops)
  WHERE embedding IS NOT NULL AND object_type = 'segment';
CREATE INDEX IF NOT EXISTS memories_embedding_episode_idx ON braincore.memories
  USING hnsw (((CASE WHEN object_type = 'episode' THEN embedding ELSE NULL END)::vector(384))
    vector_cosine_ops)
  WHERE embedding IS NOT NULL AND object_type = 'episode';
