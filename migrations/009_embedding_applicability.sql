-- Empty canonical source text is retained, but cannot satisfy the embedding
-- input contract. Mark it explicitly instead of retrying an impossible job.
ALTER TABLE braincore.memories DROP CONSTRAINT memories_embedding_state_check;
ALTER TABLE braincore.memories ADD CONSTRAINT memories_embedding_state_check
  CHECK (embedding_state IN ('pending','ready','failed','not_applicable'));
ALTER TABLE braincore.memories ADD CONSTRAINT memories_embedding_applicability_check
  CHECK (embedding_state <> 'not_applicable'
         OR (content !~ '[^[:space:]]' AND embedding IS NULL
             AND embedding_model IS NULL AND embedding_preprocessing_revision IS NULL));
UPDATE braincore.memories SET embedding_state='not_applicable',embedding=NULL,
  embedding_model=NULL,embedding_preprocessing_revision=NULL
  WHERE content !~ '[^[:space:]]';
