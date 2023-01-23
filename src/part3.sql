-- Внесите в скрипт part3.sql создание ролей и выдачу им прав

DO
$make_roles$
    DECLARE
        db_name text := (SELECT current_database());
        schema_name text := (SELECT CURRENT_SCHEMA);
    BEGIN
        -- отозвать разрешение на создание в общедоступной схемы по умолчанию для общедоступной роли
        EXECUTE 'REVOKE CREATE ON SCHEMA '||schema_name||' FROM PUBLIC';
        -- отменяет возможность подключения общедоступной роли к базе данных
        EXECUTE 'REVOKE ALL ON DATABASE '||db_name||' FROM PUBLIC';
        ALTER DEFAULT PRIVILEGES REVOKE ALL ON FUNCTIONS FROM PUBLIC;

        CREATE ROLE visitor;
        -- Предоставить этой роли разрешение на подключение к вашей целевой базе данных и схеме
        EXECUTE 'GRANT CONNECT ON DATABASE '||db_name||' TO visitor';
        EXECUTE 'GRANT USAGE ON SCHEMA '||schema_name||' TO visitor';
        -- предоставить доступ на чтение ко всем таблицам и представлениям в схеме
        GRANT pg_read_all_data TO visitor;
        -- автоматическое предоставление прав на новые таблицы
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA '||schema_name||' GRANT SELECT ON TABLES TO visitor';
        ALTER DEFAULT PRIVILEGES FOR ROLE visitor REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

        -- создать пользователя и назначьть ему одну из существующих ролей
        CREATE USER demo_user1 LOGIN;
        GRANT visitor TO demo_user1;

        ------------------------------
        -- Администратор имеет полные права на редактирование и просмотр любой информации, запуск и остановку процесса обработки.
        CREATE ROLE administrator;
        EXECUTE 'GRANT CONNECT ON DATABASE '||db_name||' TO administrator';
        EXECUTE 'GRANT USAGE, CREATE ON SCHEMA '||schema_name||' TO administrator';

        GRANT pg_read_all_data TO administrator;
        GRANT pg_write_all_data TO administrator;

        CREATE USER demo_user2 LOGIN;
        GRANT administrator TO demo_user2;
    END
$make_roles$;

------------------------------

CREATE OR REPLACE FUNCTION fnc_kill_lazy_process()
    RETURNS text AS
$$
DECLARE
    qry text;
BEGIN
    SELECT pid,
           now() - pg_stat_activity.query_start                  AS duration,
           query,
           state,
           (pg_cancel_backend(pid) or pg_terminate_backend(pid)) as killed
    FROM pg_stat_activity
    WHERE state != 'idle'
      AND (now() - pg_stat_activity.query_start) > INTERVAL '10 second'
      AND pid <> pg_backend_pid() -- не убить свой сеанс
      AND datname = current_database() -- не убить конект к другим БД
    INTO qry;
    RETURN 'kill pid: ' || COALESCE(qry, 'Nothing found');
END
$$
    LANGUAGE plpgsql
    SECURITY DEFINER;

GRANT EXECUTE ON FUNCTION fnc_kill_lazy_process() TO administrator;
------------------------------
-- Список ролей и юзеров
SELECT r.rolname,
       ARRAY(SELECT b.rolname
             FROM pg_catalog.pg_auth_members m
                      JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
             WHERE m.member = r.oid) AS memberof
FROM pg_catalog.pg_roles r
WHERE r.rolname NOT LIKE 'pg%'
ORDER BY 1;

-- для конекта под другим пользователем пишем в shell:
-- psql -d postgres -U demo_user1
-- или
-- psql -d postgres -U demo_user2
-- далее вводим текст команды в терминал

-- ok for visitor and administrator
SELECT *
FROM personal_data;

-- ok for administrator, fail for visitor
CREATE TABLE test_table AS
SELECT *
FROM personal_data;
DROP TABLE test_table;
INSERT INTO personal_data (customer_id, customer_name, customer_surname, customer_primary_email, customer_primary_phone)
VALUES ((SELECT max(customer_id) + 1 FROM personal_data), 'Валерий', 'Жмышенко', 'zadwd@main.ru', '+78003353535');
UPDATE personal_data
SET customer_name='Пожилой'
WHERE customer_id = (SELECT max(customer_id) FROM personal_data);
DELETE
FROM personal_data
WHERE customer_id = (SELECT max(customer_id) FROM personal_data);

-- запустить в отдельном сеансе
-- DO $$ BEGIN PERFORM pg_sleep(10000); END $$;

-- грохнуть зависшее
SELECT fnc_kill_lazy_process();
