-- Operator-run migration from the mnemos-core MEMORY_* schema to the
-- canonical STIPHOS Oracle schema. Back up the schema and test on a clone.
-- Legacy source tables are preserved as LEGACY_MEMORY_* after a successful
-- copy. The version marker is written only after structural validation.

DECLARE
  FUNCTION table_exists(p_name VARCHAR2) RETURN BOOLEAN IS n NUMBER;
  BEGIN
    SELECT COUNT(*) INTO n FROM user_tables WHERE table_name=UPPER(p_name);
    RETURN n=1;
  END;
  PROCEDURE create_if_missing(p_name VARCHAR2, p_ddl CLOB) IS
  BEGIN
    IF NOT table_exists(p_name) THEN EXECUTE IMMEDIATE p_ddl; END IF;
  END;
BEGIN
  DECLARE n NUMBER; BEGIN
    SELECT COUNT(*) INTO n FROM user_sequences WHERE sequence_name='HIVE_EVENTS_SEQ';
    IF n=0 THEN EXECUTE IMMEDIATE 'CREATE SEQUENCE hive_events_seq START WITH 1 INCREMENT BY 1 NOCACHE'; END IF;
  END;
  create_if_missing('AGENTS', q'~CREATE TABLE agents (
    urn VARCHAR2(256) PRIMARY KEY, kind VARCHAR2(64) NOT NULL,
    host VARCHAR2(128) NOT NULL, session_id VARCHAR2(64) NOT NULL,
    pid NUMBER(10), runtime VARCHAR2(64), model VARCHAR2(128), provider VARCHAR2(64),
    cost_tier VARCHAR2(1), autonomy_level VARCHAR2(32),
    auth_method VARCHAR2(32) DEFAULT 'unknown' NOT NULL, plan_cap_usd NUMBER(12,4),
    plan_period_used_usd NUMBER(12,6) DEFAULT 0 NOT NULL,
    subscription_pools CLOB CHECK (subscription_pools IS JSON),
    capabilities CLOB CHECK (capabilities IS JSON), version VARCHAR2(64),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    status VARCHAR2(16) DEFAULT 'online' NOT NULL,
    metadata CLOB CHECK (metadata IS JSON))~');

  create_if_missing('JOBS', q'~CREATE TABLE jobs (
    id RAW(16) PRIMARY KEY, submitter_urn VARCHAR2(256) NOT NULL,
    parent_job_id RAW(16), kind VARCHAR2(64) NOT NULL, description CLOB,
    priority NUMBER(5) DEFAULT 0 NOT NULL, deadline TIMESTAMP WITH TIME ZONE,
    required_capabilities CLOB CHECK (required_capabilities IS JSON),
    eligible_kinds CLOB CHECK (eligible_kinds IS JSON), project VARCHAR2(256),
    max_cost_tier VARCHAR2(1) DEFAULT 'A' NOT NULL,
    preferred_providers CLOB CHECK (preferred_providers IS JSON),
    preferred_models CLOB CHECK (preferred_models IS JSON),
    mnemos_refs CLOB CHECK (mnemos_refs IS JSON), depends_on CLOB CHECK (depends_on IS JSON),
    status VARCHAR2(16) NOT NULL, claimed_by VARCHAR2(256),
    claimed_at TIMESTAMP WITH TIME ZONE, claimed_runtime VARCHAR2(64),
    claimed_model VARCHAR2(128), claimed_provider VARCHAR2(64), claimed_cost_tier VARCHAR2(1),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    retry_backoff_until TIMESTAMP WITH TIME ZONE, routed_at TIMESTAMP WITH TIME ZONE,
    routing_metadata CLOB CHECK (routing_metadata IS JSON), ended_at TIMESTAMP WITH TIME ZONE,
    result CLOB CHECK (result IS JSON), result_mnemos_id VARCHAR2(64),
    tokens_in NUMBER(15), tokens_out NUMBER(15), estimated_cost_usd NUMBER(12,6),
    retry_count NUMBER(5) DEFAULT 0 NOT NULL, max_retries NUMBER(5) DEFAULT 2 NOT NULL)~');

  create_if_missing('HIVE_MESSAGES', q'~CREATE TABLE hive_messages (
    id VARCHAR2(64) PRIMARY KEY, from_urn VARCHAR2(256) NOT NULL, to_urn VARCHAR2(256),
    in_reply_to VARCHAR2(64), topic VARCHAR2(256) NOT NULL,
    payload CLOB NOT NULL CHECK (payload IS JSON),
    ts TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL)~');
  create_if_missing('HIVE_EVENTS', q'~CREATE TABLE hive_events (
    id NUMBER(19) DEFAULT hive_events_seq.NEXTVAL PRIMARY KEY,
    ts TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    kind VARCHAR2(64) NOT NULL, payload CLOB NOT NULL CHECK (payload IS JSON),
    agent_urn VARCHAR2(256))~');
  create_if_missing('HIVE_CACHE', q'~CREATE TABLE hive_cache (
    cache_key VARCHAR2(500) PRIMARY KEY, result_json CLOB NOT NULL CHECK (result_json IS JSON),
    source_job_id RAW(16), result_mnemos_id VARCHAR2(64), hit_count NUMBER(15) DEFAULT 0 NOT NULL,
    cost_saved_usd NUMBER(12,6) DEFAULT 0 NOT NULL, model VARCHAR2(128), provider VARCHAR2(64),
    cached_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    last_hit_at TIMESTAMP WITH TIME ZONE)~');
  create_if_missing('HIVE_WORKER_KIND_STATS', q'~CREATE TABLE hive_worker_kind_stats (
    urn VARCHAR2(256) NOT NULL, kind VARCHAR2(64) NOT NULL,
    success_count NUMBER(15) DEFAULT 0 NOT NULL, fail_count NUMBER(15) DEFAULT 0 NOT NULL,
    cancelled_count NUMBER(15) DEFAULT 0 NOT NULL, total_tokens_in NUMBER(15) DEFAULT 0 NOT NULL,
    total_tokens_out NUMBER(15) DEFAULT 0 NOT NULL, total_cost_usd NUMBER(12,6) DEFAULT 0 NOT NULL,
    total_duration_sec NUMBER(15,3) DEFAULT 0 NOT NULL, last_run TIMESTAMP WITH TIME ZONE,
    CONSTRAINT pk_hive_worker_kind_stats PRIMARY KEY (urn,kind))~');
  create_if_missing('HIVE_SCHEDULED_JOBS', q'~CREATE TABLE hive_scheduled_jobs (
    id VARCHAR2(64) PRIMARY KEY, name VARCHAR2(256) NOT NULL,
    created_by_urn VARCHAR2(256) NOT NULL, interval_seconds NUMBER(10) NOT NULL,
    job_template CLOB NOT NULL CHECK (job_template IS JSON), enabled NUMBER(1) DEFAULT 1 NOT NULL,
    last_fired_at TIMESTAMP WITH TIME ZONE, next_fire_at TIMESTAMP WITH TIME ZONE NOT NULL,
    fire_count NUMBER(15) DEFAULT 0 NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL)~');
  create_if_missing('HIVE_SCHEMA_METADATA', q'~CREATE TABLE hive_schema_metadata (
    component VARCHAR2(64) PRIMARY KEY, schema_version NUMBER(10) NOT NULL,
    installed_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL)~');
END;
/

-- A prior 002 draft created JOBS.DESCRIPTION as VARCHAR2(4000). Make reruns
-- repair that populated shape losslessly before copying any remaining legacy
-- rows. Oracle cannot directly change VARCHAR2 to CLOB, so use a shadow column.
DECLARE
  n NUMBER;
  description_n NUMBER;
  shadow_n NUMBER;
  v_data_type user_tab_columns.data_type%TYPE;
BEGIN
  SELECT COUNT(*) INTO n FROM user_tables WHERE table_name='JOBS';
  IF n=1 THEN
    SELECT COUNT(*) INTO description_n FROM user_tab_columns
      WHERE table_name='JOBS' AND column_name='DESCRIPTION';
    SELECT COUNT(*) INTO shadow_n FROM user_tab_columns
      WHERE table_name='JOBS' AND column_name='DESCRIPTION_CLOB';
    IF description_n=1 THEN
      SELECT data_type INTO v_data_type FROM user_tab_columns
        WHERE table_name='JOBS' AND column_name='DESCRIPTION';
    END IF;
    IF description_n=1 AND v_data_type <> 'CLOB' THEN
      IF shadow_n=0 THEN EXECUTE IMMEDIATE 'ALTER TABLE jobs ADD (description_clob CLOB)'; END IF;
      EXECUTE IMMEDIATE 'UPDATE jobs SET description_clob=TO_CLOB(description)';
      EXECUTE IMMEDIATE 'ALTER TABLE jobs DROP COLUMN description';
      EXECUTE IMMEDIATE 'ALTER TABLE jobs RENAME COLUMN description_clob TO description';
    ELSIF description_n=0 AND shadow_n=1 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE jobs RENAME COLUMN description_clob TO description';
    END IF;
  END IF;
END;
/

-- Copy the actual mnemos-core legacy shapes. MERGE makes reruns safe after a
-- partially completed copy. Invalid legacy JSON aborts before archival/version.
DECLARE
  FUNCTION table_exists(p_name VARCHAR2) RETURN BOOLEAN IS n NUMBER;
  BEGIN SELECT COUNT(*) INTO n FROM user_tables WHERE table_name=UPPER(p_name); RETURN n=1; END;
  FUNCTION column_exists(p_table VARCHAR2, p_column VARCHAR2) RETURN BOOLEAN IS n NUMBER;
  BEGIN
    SELECT COUNT(*) INTO n FROM user_tab_columns
      WHERE table_name=UPPER(p_table) AND column_name=UPPER(p_column);
    RETURN n=1;
  END;
BEGIN
  IF table_exists('MEMORY_AGENTS') THEN
    IF column_exists('MEMORY_AGENTS','AGENT_URN') THEN
      EXECUTE IMMEDIATE q'~MERGE INTO agents d USING (
      SELECT agent_urn urn, kind, NVL(host,'legacy') host,
        'legacy-migration' session_id, 'unknown' runtime, capabilities,
        CAST(registered_at AS TIMESTAMP WITH TIME ZONE) started_at,
        CAST(NVL(last_heartbeat,registered_at) AS TIMESTAMP WITH TIME ZONE) last_heartbeat
      FROM memory_agents) s ON (d.urn=s.urn)
      WHEN NOT MATCHED THEN INSERT
        (urn,kind,host,session_id,runtime,auth_method,capabilities,started_at,last_heartbeat,status)
      VALUES (s.urn,s.kind,s.host,s.session_id,s.runtime,'unknown',s.capabilities,
        NVL(s.started_at,SYSTIMESTAMP),NVL(s.last_heartbeat,SYSTIMESTAMP),'offline')~';
    ELSIF column_exists('MEMORY_AGENTS','URN') THEN
      EXECUTE IMMEDIATE q'~MERGE INTO agents d USING (SELECT * FROM memory_agents) s ON (d.urn=s.urn)
      WHEN NOT MATCHED THEN INSERT
        (urn,kind,host,session_id,pid,runtime,model,provider,cost_tier,autonomy_level,
         auth_method,plan_cap_usd,plan_period_used_usd,capabilities,version,started_at,
         last_heartbeat,status,metadata)
      VALUES (s.urn,s.kind,s.host,s.session_id,s.pid,s.runtime,s.model,s.provider,s.cost_tier,
        s.autonomy_level,s.auth_method,s.plan_cap_usd,s.plan_period_used_usd,s.capabilities,
        s.version,s.started_at,s.last_heartbeat,s.status,s.metadata)~';
    ELSE
      RAISE_APPLICATION_ERROR(-20010,'Unrecognized MEMORY_AGENTS legacy shape');
    END IF;
  END IF;

  IF table_exists('MEMORY_JOBS') THEN
    IF column_exists('MEMORY_JOBS','CREATED_AT') THEN
      EXECUTE IMMEDIATE q'~MERGE INTO jobs d USING (
      SELECT id,submitter_urn,parent_job_id,kind,description,
        NVL(priority,0) priority,eligible_kinds,project,status,claimed_by,
        CAST(NVL(created_at,SYSTIMESTAMP) AS TIMESTAMP WITH TIME ZONE) queued_at,
        CAST(started_at AS TIMESTAMP WITH TIME ZONE) claimed_at,
        CAST(ended_at AS TIMESTAMP WITH TIME ZONE) ended_at,result,
        NVL(retry_count,0) retry_count,NVL(max_retries,2) max_retries
      FROM memory_jobs) s ON (d.id=s.id)
      WHEN MATCHED THEN UPDATE SET d.description=s.description
      WHEN NOT MATCHED THEN INSERT
        (id,submitter_urn,parent_job_id,kind,description,priority,eligible_kinds,project,
         max_cost_tier,status,claimed_by,claimed_at,started_at,ended_at,result,retry_count,max_retries)
      VALUES (s.id,s.submitter_urn,s.parent_job_id,s.kind,s.description,s.priority,s.eligible_kinds,
        s.project,'A',s.status,s.claimed_by,s.claimed_at,s.queued_at,s.ended_at,s.result,
        s.retry_count,s.max_retries)~';
    ELSIF column_exists('MEMORY_JOBS','REQUIRED_CAPABILITIES') THEN
      EXECUTE IMMEDIATE q'~MERGE INTO jobs d USING (SELECT * FROM memory_jobs) s ON (d.id=s.id)
      WHEN MATCHED THEN UPDATE SET d.description=s.description
      WHEN NOT MATCHED THEN INSERT
        (id,submitter_urn,parent_job_id,kind,description,priority,deadline,required_capabilities,
         eligible_kinds,project,max_cost_tier,preferred_providers,preferred_models,mnemos_refs,
         depends_on,status,claimed_by,claimed_at,claimed_runtime,claimed_model,claimed_provider,
         claimed_cost_tier,started_at,retry_backoff_until,ended_at,result,result_mnemos_id,
         tokens_in,tokens_out,estimated_cost_usd,retry_count,max_retries)
      VALUES (s.id,s.submitter_urn,s.parent_job_id,s.kind,s.description,s.priority,s.deadline,
        s.required_capabilities,s.eligible_kinds,s.project,s.max_cost_tier,s.preferred_providers,
        s.preferred_models,s.mnemos_refs,s.depends_on,s.status,s.claimed_by,s.claimed_at,
        s.claimed_runtime,s.claimed_model,s.claimed_provider,s.claimed_cost_tier,s.started_at,
        s.retry_backoff_until,s.ended_at,s.result,s.result_mnemos_id,s.tokens_in,s.tokens_out,
        s.estimated_cost_usd,s.retry_count,s.max_retries)~';
    ELSE
      RAISE_APPLICATION_ERROR(-20011,'Unrecognized MEMORY_JOBS legacy shape');
    END IF;
  END IF;

  IF table_exists('MEMORY_HIVE_CACHE') THEN
    IF column_exists('MEMORY_HIVE_CACHE','VALUE') THEN
      EXECUTE IMMEDIATE q'~MERGE INTO hive_cache d USING (
      SELECT cache_key, value result_json, CAST(created_at AS TIMESTAMP WITH TIME ZONE) cached_at
      FROM memory_hive_cache WHERE value IS JSON) s ON (d.cache_key=s.cache_key)
      WHEN NOT MATCHED THEN INSERT (cache_key,result_json,cached_at)
      VALUES (s.cache_key,s.result_json,NVL(s.cached_at,SYSTIMESTAMP))~';
    ELSE
      EXECUTE IMMEDIATE q'~MERGE INTO hive_cache d USING (SELECT * FROM memory_hive_cache) s
      ON (d.cache_key=s.cache_key) WHEN NOT MATCHED THEN INSERT
        (cache_key,result_json,source_job_id,result_mnemos_id,hit_count,cost_saved_usd,
         model,provider,cached_at,last_hit_at)
      VALUES (s.cache_key,s.result_json,s.source_job_id,s.result_mnemos_id,s.hit_count,
        s.cost_saved_usd,s.model,s.provider,s.cached_at,s.last_hit_at)~';
    END IF;
  END IF;
  IF table_exists('MEMORY_WORKER_KIND_STATS') THEN
    IF column_exists('MEMORY_WORKER_KIND_STATS','URN') THEN
      EXECUTE IMMEDIATE q'~MERGE INTO hive_worker_kind_stats d USING
      (SELECT * FROM memory_worker_kind_stats) s ON (d.urn=s.urn AND d.kind=s.kind)
      WHEN NOT MATCHED THEN INSERT
        (urn,kind,success_count,fail_count,cancelled_count,total_tokens_in,total_tokens_out,
         total_cost_usd,total_duration_sec,last_run)
      VALUES (s.urn,s.kind,s.success_count,s.fail_count,s.cancelled_count,s.total_tokens_in,
        s.total_tokens_out,s.total_cost_usd,s.total_duration_sec,s.last_run)~';
    ELSE
      EXECUTE IMMEDIATE q'~MERGE INTO hive_worker_kind_stats d USING (
      SELECT 'urn:agent:legacy:unknown:migrated' urn,kind,NVL(success_count,0) success_count,
        NVL(fail_count,0) fail_count,NVL(cancelled_count,0) cancelled_count,
        NVL(total_tokens_in,0) total_tokens_in,NVL(total_tokens_out,0) total_tokens_out,
        NVL(total_cost_usd,0) total_cost_usd,NVL(total_duration_sec,0) total_duration_sec,
        CAST(last_run AS TIMESTAMP WITH TIME ZONE) last_run FROM memory_worker_kind_stats) s
      ON (d.urn=s.urn AND d.kind=s.kind)
      WHEN NOT MATCHED THEN INSERT
        (urn,kind,success_count,fail_count,cancelled_count,total_tokens_in,total_tokens_out,
         total_cost_usd,total_duration_sec,last_run)
      VALUES (s.urn,s.kind,s.success_count,s.fail_count,s.cancelled_count,s.total_tokens_in,
        s.total_tokens_out,s.total_cost_usd,s.total_duration_sec,s.last_run)~';
    END IF;
  END IF;
  IF table_exists('MEMORY_MESSAGES') THEN
    EXECUTE IMMEDIATE q'~MERGE INTO hive_messages d USING (SELECT * FROM memory_messages) s
    ON (d.id=s.id) WHEN NOT MATCHED THEN INSERT
      (id,from_urn,to_urn,in_reply_to,topic,payload,ts)
    VALUES (s.id,s.from_urn,s.to_urn,s.in_reply_to,s.topic,s.payload,s.ts)~';
  END IF;
  IF table_exists('MEMORY_EVENTS') THEN
    EXECUTE IMMEDIATE q'~MERGE INTO hive_events d USING (SELECT * FROM memory_events) s
    ON (d.id=s.id) WHEN NOT MATCHED THEN INSERT (id,ts,kind,payload,agent_urn)
    VALUES (s.id,s.ts,s.kind,s.payload,s.agent_urn)~';
  END IF;
  DECLARE next_id NUMBER; BEGIN
    SELECT NVL(MAX(id),0)+1 INTO next_id FROM hive_events;
    EXECUTE IMMEDIATE 'ALTER SEQUENCE hive_events_seq RESTART START WITH '||TO_CHAR(next_id);
  END;
  IF table_exists('MEMORY_SCHEDULED_JOBS') THEN
    EXECUTE IMMEDIATE q'~MERGE INTO hive_scheduled_jobs d USING (SELECT * FROM memory_scheduled_jobs) s
    ON (d.id=s.id) WHEN NOT MATCHED THEN INSERT
      (id,name,created_by_urn,interval_seconds,job_template,enabled,last_fired_at,
       next_fire_at,fire_count,created_at)
    VALUES (s.id,s.name,s.created_by_urn,s.interval_seconds,s.job_template,s.enabled,
      s.last_fired_at,s.next_fire_at,s.fire_count,s.created_at)~';
  END IF;
END;
/

-- If the truncating draft already archived MEMORY_JOBS, restore the live CLOB
-- from that preserved source on rerun.
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM user_tables WHERE table_name='LEGACY_MEMORY_JOBS';
  IF n=1 THEN
    EXECUTE IMMEDIATE q'~MERGE INTO jobs d
      USING (SELECT id,description FROM legacy_memory_jobs) s ON (d.id=s.id)
      WHEN MATCHED THEN UPDATE SET d.description=s.description~';
  END IF;
END;
/

-- Do not archive the source unless every migrated job description has the
-- same length. This catches truncation from an interrupted/older 002 run.
DECLARE
  PROCEDURE validate_descriptions(p_source VARCHAR2) IS
    mismatch_count NUMBER;
  BEGIN
    EXECUTE IMMEDIATE
      'SELECT COUNT(*) FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_source)||' source '||
      'LEFT JOIN jobs target ON target.id=source.id '||
      'WHERE target.id IS NULL OR NVL(LENGTH(source.description),0) <> NVL(LENGTH(target.description),0)'
      INTO mismatch_count;
    IF mismatch_count <> 0 THEN
      RAISE_APPLICATION_ERROR(-20022,
        'Job description length mismatch after migration from '||p_source||': '||TO_CHAR(mismatch_count));
    END IF;
  END;
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM user_tables WHERE table_name='MEMORY_JOBS';
  IF n=1 THEN validate_descriptions('MEMORY_JOBS'); END IF;
  SELECT COUNT(*) INTO n FROM user_tables WHERE table_name='LEGACY_MEMORY_JOBS';
  IF n=1 THEN validate_descriptions('LEGACY_MEMORY_JOBS'); END IF;
END;
/

-- Validate every table and every column used by OracleHiveMindRepository.
DECLARE
  PROCEDURE require_columns(p_table VARCHAR2, p_columns SYS.ODCIVARCHAR2LIST) IS n NUMBER;
  BEGIN
    SELECT COUNT(*) INTO n FROM user_tables WHERE table_name=UPPER(p_table);
    IF n<>1 THEN RAISE_APPLICATION_ERROR(-20020,'Missing canonical table '||p_table); END IF;
    FOR i IN 1..p_columns.COUNT LOOP
      SELECT COUNT(*) INTO n FROM user_tab_columns
        WHERE table_name=UPPER(p_table) AND column_name=UPPER(p_columns(i));
      IF n<>1 THEN RAISE_APPLICATION_ERROR(-20021,'Missing '||p_table||'.'||p_columns(i)); END IF;
    END LOOP;
  END;
BEGIN
  require_columns('AGENTS',SYS.ODCIVARCHAR2LIST('URN','KIND','HOST','SESSION_ID','PID','RUNTIME','MODEL','PROVIDER','COST_TIER','AUTONOMY_LEVEL','AUTH_METHOD','PLAN_CAP_USD','PLAN_PERIOD_USED_USD','SUBSCRIPTION_POOLS','CAPABILITIES','VERSION','STARTED_AT','LAST_HEARTBEAT','STATUS','METADATA'));
  require_columns('JOBS',SYS.ODCIVARCHAR2LIST('ID','SUBMITTER_URN','PARENT_JOB_ID','KIND','DESCRIPTION','PRIORITY','DEADLINE','REQUIRED_CAPABILITIES','ELIGIBLE_KINDS','PROJECT','MAX_COST_TIER','PREFERRED_PROVIDERS','PREFERRED_MODELS','MNEMOS_REFS','DEPENDS_ON','STATUS','CLAIMED_BY','CLAIMED_AT','CLAIMED_RUNTIME','CLAIMED_MODEL','CLAIMED_PROVIDER','CLAIMED_COST_TIER','STARTED_AT','RETRY_BACKOFF_UNTIL','ROUTED_AT','ROUTING_METADATA','ENDED_AT','RESULT','RESULT_MNEMOS_ID','TOKENS_IN','TOKENS_OUT','ESTIMATED_COST_USD','RETRY_COUNT','MAX_RETRIES'));
  require_columns('HIVE_MESSAGES',SYS.ODCIVARCHAR2LIST('ID','FROM_URN','TO_URN','IN_REPLY_TO','TOPIC','PAYLOAD','TS'));
  require_columns('HIVE_EVENTS',SYS.ODCIVARCHAR2LIST('ID','TS','KIND','PAYLOAD','AGENT_URN'));
  require_columns('HIVE_CACHE',SYS.ODCIVARCHAR2LIST('CACHE_KEY','RESULT_JSON','SOURCE_JOB_ID','RESULT_MNEMOS_ID','HIT_COUNT','COST_SAVED_USD','MODEL','PROVIDER','CACHED_AT','LAST_HIT_AT'));
  require_columns('HIVE_WORKER_KIND_STATS',SYS.ODCIVARCHAR2LIST('URN','KIND','SUCCESS_COUNT','FAIL_COUNT','CANCELLED_COUNT','TOTAL_TOKENS_IN','TOTAL_TOKENS_OUT','TOTAL_COST_USD','TOTAL_DURATION_SEC','LAST_RUN'));
  require_columns('HIVE_SCHEDULED_JOBS',SYS.ODCIVARCHAR2LIST('ID','NAME','CREATED_BY_URN','INTERVAL_SECONDS','JOB_TEMPLATE','ENABLED','LAST_FIRED_AT','NEXT_FIRE_AT','FIRE_COUNT','CREATED_AT'));
END;
/

-- Preserve legacy data without leaving MEMORY_* names that the runtime treats
-- as an incomplete pre-v2 schema.
DECLARE
  PROCEDURE archive(p_old VARCHAR2, p_new VARCHAR2) IS old_n NUMBER; new_n NUMBER;
  BEGIN
    SELECT COUNT(*) INTO old_n FROM user_tables WHERE table_name=UPPER(p_old);
    SELECT COUNT(*) INTO new_n FROM user_tables WHERE table_name=UPPER(p_new);
    IF old_n=1 AND new_n=0 THEN
      EXECUTE IMMEDIATE 'RENAME '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_old)||' TO '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_new);
    ELSIF old_n=1 AND new_n=1 THEN
      RAISE_APPLICATION_ERROR(-20030,'Archive table already exists: '||p_new);
    END IF;
  END;
BEGIN
  archive('MEMORY_AGENTS','LEGACY_MEMORY_AGENTS');
  archive('MEMORY_JOBS','LEGACY_MEMORY_JOBS');
  archive('MEMORY_MESSAGES','LEGACY_MEMORY_MESSAGES');
  archive('MEMORY_EVENTS','LEGACY_MEMORY_EVENTS');
  archive('MEMORY_HIVE_CACHE','LEGACY_MEMORY_HIVE_CACHE');
  archive('MEMORY_WORKER_KIND_STATS','LEGACY_MEMORY_WORKER_STATS');
  archive('MEMORY_SCHEDULED_JOBS','LEGACY_MEMORY_SCHEDULES');
END;
/

MERGE INTO hive_schema_metadata d
USING (SELECT 'stiphos-oracle' component, 2 schema_version FROM dual) s
ON (d.component=s.component)
WHEN MATCHED THEN UPDATE SET d.schema_version=s.schema_version,d.installed_at=SYSTIMESTAMP
WHEN NOT MATCHED THEN INSERT (component,schema_version) VALUES (s.component,s.schema_version);

COMMIT;
