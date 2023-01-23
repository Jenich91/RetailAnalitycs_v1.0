DROP TABLE IF EXISTS date_of_analysis_formation CASCADE;
DROP TABLE IF EXISTS checks CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS stores CASCADE;
DROP TABLE IF EXISTS product CASCADE;
DROP TABLE IF EXISTS sku CASCADE;
DROP TABLE IF EXISTS sku_groups CASCADE;
DROP TABLE IF EXISTS cards CASCADE;
DROP TABLE IF EXISTS personal_data CASCADE;
DROP TYPE IF EXISTS MODE CASCADE;
DROP TYPE IF EXISTS uint CASCADE;

DO $clear_tables_and_func$
    DECLARE
        _sql    text;
        _schema text := CURRENT_SCHEMA;
    BEGIN
        SELECT INTO _sql string_agg(format('DROP %s %s CASCADE;'
                                        , CASE prokind
                                              WHEN 'f' THEN 'FUNCTION'
                                              WHEN 'a' THEN 'AGGREGATE'
                                              WHEN 'p' THEN 'PROCEDURE'
                                              WHEN 'w' THEN 'FUNCTION'
                                               END
                                        , oid::regprocedure)
                             , E'\n')
        FROM pg_proc
        WHERE pronamespace = _schema::regnamespace
          AND prokind = ANY ('{f,a,p,w}');

        IF _sql IS NOT NULL THEN
            RAISE NOTICE '%', _sql;
            EXECUTE _sql;
        ELSE
            RAISE NOTICE 'No fuctions found in schema %', quote_ident(_schema);
        END IF;
    END
$clear_tables_and_func$;

DO $clear_role_and_users$
    DECLARE
        rolenames text;
    BEGIN
        FOR rolenames IN
            SELECT rolname
            FROM pg_catalog.pg_roles
            WHERE rolname not like 'pg%'
              and rolname IN ('administrator', 'visitor', 'demo_user1', 'demo_user2')
            ORDER BY 1
            LOOP
                EXECUTE 'REASSIGN OWNED BY ' || rolenames || ' TO ' || (SELECT current_database());
                EXECUTE 'DROP OWNED BY ' || rolenames;
                EXECUTE 'DROP ROLE IF EXISTS ' || rolenames;
            END LOOP;
    END;
$clear_role_and_users$;