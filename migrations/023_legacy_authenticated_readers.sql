-- Every participant is explicitly authenticated; no tenant is implicitly public.
CREATE FUNCTION braincore.compat_tenants() RETURNS uuid[]
LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE configured text; identities jsonb; result uuid[];
BEGIN
 configured := current_setting('braincore.compat_read_tenants',true);
 IF configured IS NULL OR configured='' THEN RETURN ARRAY[braincore.compat_tenant()]; END IF;
 identities := configured::jsonb;
 IF jsonb_typeof(identities)<>'array' OR jsonb_array_length(identities) NOT BETWEEN 1 AND 4 THEN
  RAISE EXCEPTION 'invalid compatibility reader identities' USING ERRCODE='42501';
 END IF;
 SELECT array_agg(value::uuid) INTO result FROM jsonb_array_elements_text(identities);
 IF NOT COALESCE(braincore.compat_tenant()=ANY(result),false) THEN
  RAISE EXCEPTION 'primary reader identity missing' USING ERRCODE='42501';
 END IF;
 RETURN result;
END; $$;
DO $$
DECLARE relation record; definition text; updated text;
BEGIN
 FOR relation IN SELECT c.oid,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='preserve' AND c.relkind='v' LOOP
  definition := pg_get_viewdef(relation.oid,true);
  updated := regexp_replace(definition,
   '([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+) = braincore\.compat_tenant\(\)',
   '\1 = ANY (braincore.compat_tenants())','g');
  IF updated<>definition THEN
   EXECUTE format('CREATE OR REPLACE VIEW preserve.%I WITH(security_barrier=true) AS %s',relation.relname,updated);
  END IF;
 END LOOP;
END; $$;
-- Hydrate selected canonical IDs before subtype rendering. Full view visibility
-- and external identity checks remain in force, under invoker permissions.
DO $$
DECLARE subtype text; definition text; bounded text;
BEGIN
 PERFORM set_config('search_path','pg_catalog',true);
 FOREACH subtype IN ARRAY ARRAY['entity','fact','memory','segment','episode'] LOOP
  definition := pg_get_viewdef(format('preserve.%I',subtype)::regclass,true);
  bounded := replace(definition,'braincore.memories m',
   '(SELECT * FROM braincore.memories WHERE id = ANY ($1)) m');
  IF bounded=definition THEN RAISE EXCEPTION 'canonical compatibility view guard missing'; END IF;
  EXECUTE format('CREATE FUNCTION braincore.compat_hydrate_%I(uuid[]) RETURNS SETOF preserve.%I LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS %L',subtype,subtype,bounded);
 END LOOP;
END; $$;
