-- Переносит покупателей и сделки из JSONB-массивов abstract_customer в отдельные таблицы.
-- Старые колонки остаются до проверки; удаляет их 002_drop_legacy_columns.sql.
-- contact_ids и company_ids заполнит первый полный прогон.
BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE amo_customer (
    id BIGINT PRIMARY KEY,
    login TEXT,
    name TEXT,
    contact_ids BIGINT[] NOT NULL DEFAULT '{}',
    company_ids BIGINT[] NOT NULL DEFAULT '{}',
    data JSONB NOT NULL,
    abstract_customer_id BIGINT NOT NULL REFERENCES abstract_customer (id) ON DELETE CASCADE
);

CREATE TABLE amo_lead (LIKE amo_customer INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE amo_lead
    ADD PRIMARY KEY (id),
    ADD FOREIGN KEY (abstract_customer_id) REFERENCES abstract_customer (id) ON DELETE CASCADE;

-- При дублях одного id в разных записях остаётся запись с меньшим id.
INSERT INTO amo_customer (id, abstract_customer_id, login, name, data)
SELECT DISTINCT ON ((item -> 'general' ->> 'id')::bigint)
       (item -> 'general' ->> 'id')::bigint, ac.id,
       item -> 'general' ->> 'login', item -> 'general' ->> 'name', item
FROM abstract_customer ac
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(ac.amo_customers) = 'array' THEN ac.amo_customers ELSE '[]'::jsonb END
) AS item
WHERE item -> 'general' ->> 'id' IS NOT NULL
ORDER BY (item -> 'general' ->> 'id')::bigint, ac.id;

INSERT INTO amo_lead (id, abstract_customer_id, login, name, data)
SELECT DISTINCT ON ((item -> 'general' ->> 'id')::bigint)
       (item -> 'general' ->> 'id')::bigint, ac.id,
       item -> 'general' ->> 'login', item -> 'general' ->> 'name', item
FROM abstract_customer ac
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(ac.amo_leads) = 'array' THEN ac.amo_leads ELSE '[]'::jsonb END
) AS item
WHERE item -> 'general' ->> 'id' IS NOT NULL
ORDER BY (item -> 'general' ->> 'id')::bigint, ac.id;

CREATE INDEX ix_amo_customer_login ON amo_customer (login);
CREATE INDEX ix_amo_customer_abstract_customer_id ON amo_customer (abstract_customer_id);
CREATE INDEX ix_amo_customer_name_trgm ON amo_customer USING gin (name gin_trgm_ops);
CREATE INDEX ix_amo_lead_login ON amo_lead (login);
CREATE INDEX ix_amo_lead_abstract_customer_id ON amo_lead (abstract_customer_id);
CREATE INDEX ix_amo_lead_name_trgm ON amo_lead USING gin (name gin_trgm_ops);

COMMIT;
