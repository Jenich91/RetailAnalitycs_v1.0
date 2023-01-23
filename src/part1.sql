------------ создание таблиц ------------
CREATE TABLE personal_data
(
    customer_id            bigserial primary key,
    customer_name          varchar not null,
    customer_surname       varchar not null,
    customer_primary_email varchar not null,
    customer_primary_phone varchar not null,
    CONSTRAINT uk_personal_data unique (customer_id, customer_name, customer_surname, customer_primary_email, customer_primary_phone),
    CONSTRAINT ch_customer_name CHECK (customer_name ~ '^(([A-Z]{1}([a-z]| |-)*)|([А-ЯЁ]{1}([а-яё]| |-)*))$'),
    CONSTRAINT ch_customer_surname CHECK (customer_name ~ '^(([A-Z]{1}([a-z]| |-)*)|([А-ЯЁ]{1}([а-яё]| |-)*))$'),
    CONSTRAINT ch_customer_email CHECK (customer_primary_email ~ '^([a-zA-Z0-9_\-\.\+]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$'),
    CONSTRAINT ch_customer_phone CHECK (customer_primary_phone ~ '^\+7\d{10}$')
);

CREATE TABLE cards
(
    customer_card_id bigserial primary key,
    customer_id      bigint not null,
    CONSTRAINT fk_cards_customer_id FOREIGN KEY (customer_id) REFERENCES personal_data (customer_id)
);

CREATE TABLE sku_groups
(
    group_id   bigserial primary key,
    group_name varchar not null
);

CREATE TABLE sku
(
    sku_id   bigserial primary key,
    sku_name varchar not null,
    group_id bigint  not null,
    CONSTRAINT fk_product_group_id FOREIGN KEY (group_id) REFERENCES sku_groups (group_id)
);

CREATE TABLE stores
(
    transaction_store_id bigint,
    sku_id               bigint  not null,
    sku_purchase_price   numeric not null,
    sku_retail_price     numeric not null,
    CONSTRAINT fk_stores_sku_id FOREIGN KEY (sku_id) REFERENCES sku (sku_id)
);

CREATE TABLE transactions
(
    transaction_id        bigserial primary key,
    customer_card_id      bigint    not null,
    transaction_summ      numeric   not null,
    transaction_date_time timestamp not null,
    transaction_store_id  bigint    not null,
    CONSTRAINT uk_transactions unique (transaction_id),
    CONSTRAINT fk_transactions_customer_card_id FOREIGN KEY (customer_card_id) REFERENCES cards (customer_card_id)
);

CREATE TABLE checks
(
    transaction_id bigint  not null,
    sku_id         bigint  not null,
    sku_amount     numeric not null,
    sku_summ       numeric not null,
    sku_summ_paid  numeric not null,
    sku_discount   numeric not null,
    CONSTRAINT fk_checks_transaction_id FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id),
    CONSTRAINT fk_checks_sku_id FOREIGN KEY (sku_id) REFERENCES sku (sku_id)
);

CREATE TABLE date_of_analysis_formation
(
    analysis_formation timestamp not null
);


------------ функция нахождения в файле столбца id ------------
CREATE OR REPLACE FUNCTION fnc_get_columns_names(tablename text) RETURNS TEXT AS $$
BEGIN
    RETURN (SELECT string_agg(format('%s', quote_ident(column_name)), ',')
            FROM (SELECT column_name
                  FROM information_schema.columns
                  WHERE table_schema = CURRENT_SCHEMA
                    AND table_name = tablename
                    AND NOT column_name = 'id') AS names);
END;
$$ LANGUAGE PLPGSQL;

------------  функция импорта данных из файла ------------
CREATE OR REPLACE PROCEDURE pr_fill_table_from_datafile(IN tablename text, IN PATH text, IN delim text DEFAULT E'\t') LANGUAGE PLPGSQL AS $$
BEGIN
    -- чтобы в файлах не надо было руками заполнять столбец id, при заполнении таблицы перечисляем список столбцов кроме id
    EXECUTE format('COPY %1$s(%2$s) FROM %3$L WITH DELIMITER ''%4$s''', $1, fnc_get_columns_names(tablename), $2, $3);
    -- проверка что есть колонка id в таблице
    IF exists(SELECT 1
              FROM information_schema.columns
              WHERE table_schema = CURRENT_SCHEMA
                AND table_name = tablename
                AND column_name = 'id')
    THEN
        EXECUTE format('SELECT setval(''%1$s_id_seq'', (SELECT MAX(id) FROM %1$s))',
                       $1); -- сброс счетчика на текущее max(id)
    END IF;
END
$$;

------------ функция экспорт данных из таблиц в файлы ------------
CREATE OR REPLACE PROCEDURE pr_fill_datafile_from_table(IN tablename text, IN PATH text, IN delim text DEFAULT E'\t') LANGUAGE PLPGSQL AS $$
BEGIN
    EXECUTE format('COPY %s TO %L DELIMITER ''%s''', $1, $2, $3);
END;
$$;

------------ импорт данных в таблицы и экспорт данных из таблиц для csv файлов ------------
-- SET datestyle TO 'ISO, DMY';
-- SET path_to_project.var TO '/Users/username/Documents/projects/SQL3_RetailAnalitycs_v1.0-0';

-- CALL pr_fill_table_from_datafile('sku_groups', current_setting('path_to_project.var') || '/datasets/Groups_SKU_Mini.csv', ',');
-- CALL pr_fill_datafile_from_table('sku_groups',current_setting('path_to_project.var') || '/src/export/Groups_SKU_Mini.csv', ',');
-- delete from sku_groups;


------------ импорт данных в таблицы ------------
-- mini
SET datestyle TO 'ISO, DMY';
SET path_to_project.var TO '/Users/username/Documents/projects/SQL3_RetailAnalitycs_v1.0-0';-- <<< нужно указать свой путь до проекта

CALL pr_fill_table_from_datafile('date_of_analysis_formation', current_setting('path_to_project.var')||'/datasets/Date_Of_Analysis_Formation.tsv');
CALL pr_fill_table_from_datafile('personal_data', current_setting('path_to_project.var')||'/datasets/Personal_Data_Mini.tsv');
CALL pr_fill_table_from_datafile('cards', current_setting('path_to_project.var')||'/datasets/Cards_Mini.tsv');
CALL pr_fill_table_from_datafile('sku_groups', current_setting('path_to_project.var')||'/datasets/Groups_SKU_Mini.tsv');
CALL pr_fill_table_from_datafile('sku', current_setting('path_to_project.var')||'/datasets/SKU_Mini.tsv');
CALL pr_fill_table_from_datafile('stores', current_setting('path_to_project.var')||'/datasets/Stores_Mini.tsv');
CALL pr_fill_table_from_datafile('transactions', current_setting('path_to_project.var')||'/datasets/Transactions_Mini.tsv');
CALL pr_fill_table_from_datafile('checks', current_setting('path_to_project.var')||'/datasets/Checks_Mini.tsv');

-- bigdata
SET datestyle TO 'ISO, DMY';
SET path_to_project.var TO '/Users/username/Documents/projects/SQL3_RetailAnalitycs_v1.0-0';-- <<< нужно указать свой путь до проекта

CALL pr_fill_table_from_datafile('date_of_analysis_formation', current_setting('path_to_project.var')||'/datasets/Date_Of_Analysis_Formation.tsv');
CALL pr_fill_table_from_datafile('personal_data', current_setting('path_to_project.var')||'/datasets/Personal_Data.tsv');
CALL pr_fill_table_from_datafile('cards', current_setting('path_to_project.var')||'/datasets/Cards.tsv');
CALL pr_fill_table_from_datafile('sku_groups', current_setting('path_to_project.var')||'/datasets/Groups_SKU.tsv');
CALL pr_fill_table_from_datafile('sku', current_setting('path_to_project.var')||'/datasets/SKU.tsv');
CALL pr_fill_table_from_datafile('stores', current_setting('path_to_project.var')||'/datasets/Stores.tsv');
CALL pr_fill_table_from_datafile('transactions', current_setting('path_to_project.var')||'/datasets/Transactions.tsv');
CALL pr_fill_table_from_datafile('checks', current_setting('path_to_project.var')||'/datasets/Checks.tsv');


------------ экспорт данных из таблицы ------------
-- mini
SET path_to_project.var TO '/Users/username/Documents/projects/SQL3_RetailAnalitycs_v1.0-0';-- <<< нужно указать свой путь до проекта

CALL pr_fill_file_from_table('date_of_analysis_formation', current_setting('path_to_project.var')||'/src/export/Date_Of_Analysis_Formation.tsv');
CALL pr_fill_file_from_table('personal_data', current_setting('path_to_project.var')||'/src/export/Personal_Data_Mini.tsv');
CALL pr_fill_file_from_table('cards', current_setting('path_to_project.var')||'/src/export/Cards_Mini.tsv');
CALL pr_fill_file_from_table('sku_groups', current_setting('path_to_project.var')||'/src/export/Groups_SKU_Mini.tsv');
CALL pr_fill_file_from_table('sku', current_setting('path_to_project.var')||'/src/export/SKU_Mini.tsv');
CALL pr_fill_file_from_table('stores', current_setting('path_to_project.var')||'/src/export/Stores_Mini.tsv');
CALL pr_fill_file_from_table('transactions', current_setting('path_to_project.var')||'/src/export/Transactions_Mini.tsv');
CALL pr_fill_file_from_table('checks', current_setting('path_to_project.var')||'/src/export/Checks_Mini.tsv');

-- bigdata
SET path_to_project.var TO '/Users/username/Documents/projects/SQL3_RetailAnalitycs_v1.0-0';-- <<< нужно указать свой путь до проекта

CALL pr_fill_file_from_table('date_of_analysis_formation', current_setting('path_to_project.var')||'/src/export/Date_Of_Analysis_Formation.tsv');
CALL pr_fill_file_from_table('personal_data', current_setting('path_to_project.var')||'/src/export/Personal_Data.tsv');
CALL pr_fill_file_from_table('cards', current_setting('path_to_project.var')||'/src/export/Cards.tsv');
CALL pr_fill_file_from_table('sku_groups', current_setting('path_to_project.var')||'/src/export/Groups_SKU.tsv');
CALL pr_fill_file_from_table('sku', current_setting('path_to_project.var')||'/src/export/SKU.tsv');
CALL pr_fill_file_from_table('stores', current_setting('path_to_project.var')||'/src/export/Stores.tsv');
CALL pr_fill_file_from_table('transactions', current_setting('path_to_project.var')||'/src/export/Transactions.tsv');
CALL pr_fill_file_from_table('checks', current_setting('path_to_project.var')||'/src/export/Checks.tsv');
