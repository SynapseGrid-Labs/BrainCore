-- Validated current subtype attributes may change without a canonical content revision.
ALTER TABLE braincore.legacy_client_attributes
 ALTER COLUMN imported_payload_hash DROP NOT NULL,
 ADD COLUMN current_override boolean NOT NULL DEFAULT false;
DO $$
DECLARE subtype text; definition text;
BEGIN
 FOREACH subtype IN ARRAY ARRAY['entity','fact','memory','segment','episode'] LOOP
  definition := pg_get_viewdef(format('preserve.%I',subtype)::regclass,true);
  IF position('WHEN m.legacy_attributes_ready THEN m.legacy_attributes' IN definition)=0 THEN
   RAISE EXCEPTION 'canonical subtype attribute guard is missing';
  END IF;
  definition := replace(definition,'WHEN m.legacy_attributes_ready THEN m.legacy_attributes',
   'WHEN x.current_override THEN x.attributes WHEN m.legacy_attributes_ready THEN m.legacy_attributes');
  definition := replace(definition,'WHEN m.legacy_attributes_ready THEN m.legacy_content_hash',
   'WHEN x.current_override THEN x.content_hash WHEN m.legacy_attributes_ready THEN m.legacy_content_hash');
  definition := replace(definition,'WHEN m.legacy_attributes_ready THEN m.legacy_search_vector',
   'WHEN x.current_override THEN x.search_vector WHEN m.legacy_attributes_ready THEN m.legacy_search_vector');
  EXECUTE format('CREATE OR REPLACE VIEW preserve.%I WITH(security_barrier=true) AS %s',subtype,definition);
 END LOOP;
END $$;
