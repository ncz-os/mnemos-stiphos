-- Idempotent, operator-run migration from the deprecated MEMORY_* convention
-- to canonical STIPHOS Oracle schema version 2. This script does not run at
-- application startup. Back up the schema and test it on a clone first.

DECLARE
  PROCEDURE rename_table_if_needed(old_name VARCHAR2, new_name VARCHAR2) IS
    old_count NUMBER;
    new_count NUMBER;
  BEGIN
    SELECT COUNT(*) INTO old_count FROM user_tables WHERE table_name = UPPER(old_name);
    SELECT COUNT(*) INTO new_count FROM user_tables WHERE table_name = UPPER(new_name);
    IF old_count = 1 AND new_count = 0 THEN
      EXECUTE IMMEDIATE 'RENAME ' || old_name || ' TO ' || new_name;
    ELSIF old_count = 1 AND new_count = 1 THEN
      RAISE_APPLICATION_ERROR(-20001, 'Both ' || old_name || ' and ' || new_name ||
        ' exist; reconcile duplicate schemas before migration.');
    END IF;
  END;

  PROCEDURE add_column_if_needed(table_name_in VARCHAR2, column_name_in VARCHAR2, ddl VARCHAR2) IS
    column_count NUMBER;
  BEGIN
    SELECT COUNT(*) INTO column_count FROM user_tab_columns
      WHERE table_name = UPPER(table_name_in) AND column_name = UPPER(column_name_in);
    IF column_count = 0 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE ' || table_name_in || ' ADD (' || ddl || ')';
    END IF;
  END;
BEGIN
  rename_table_if_needed('MEMORY_AGENTS', 'AGENTS');
  rename_table_if_needed('MEMORY_JOBS', 'JOBS');
  rename_table_if_needed('MEMORY_MESSAGES', 'HIVE_MESSAGES');
  rename_table_if_needed('MEMORY_EVENTS', 'HIVE_EVENTS');
  rename_table_if_needed('MEMORY_HIVE_CACHE', 'HIVE_CACHE');
  rename_table_if_needed('MEMORY_WORKER_KIND_STATS', 'HIVE_WORKER_KIND_STATS');
  rename_table_if_needed('MEMORY_SCHEDULED_JOBS', 'HIVE_SCHEDULED_JOBS');

  add_column_if_needed('AGENTS', 'SUBSCRIPTION_POOLS',
    'subscription_pools CLOB CHECK (subscription_pools IS JSON)');
  add_column_if_needed('JOBS', 'ROUTED_AT',
    'routed_at TIMESTAMP WITH TIME ZONE');
  add_column_if_needed('JOBS', 'ROUTING_METADATA',
    'routing_metadata CLOB CHECK (routing_metadata IS JSON)');
END;
/

DECLARE
  table_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO table_count FROM user_tables WHERE table_name = 'HIVE_SCHEMA_METADATA';
  IF table_count = 0 THEN
    EXECUTE IMMEDIATE '
      CREATE TABLE hive_schema_metadata (
        component VARCHAR2(64) PRIMARY KEY,
        schema_version NUMBER(10) NOT NULL,
        installed_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
      )';
  END IF;
END;
/

MERGE INTO hive_schema_metadata dst
USING (SELECT 'stiphos-oracle' component, 2 schema_version FROM dual) src
ON (dst.component = src.component)
WHEN MATCHED THEN UPDATE SET dst.schema_version = src.schema_version,
  dst.installed_at = SYSTIMESTAMP
WHEN NOT MATCHED THEN INSERT (component, schema_version)
  VALUES (src.component, src.schema_version);

COMMIT;
