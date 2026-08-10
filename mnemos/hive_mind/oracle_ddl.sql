-- Canonical Oracle 23ai schema for STIPHOS Hive Mind.
-- Schema convention/version: stiphos-oracle/2.
--
-- The production ledger convention uses AGENTS and JOBS. Service-owned tables
-- use the HIVE_ prefix. Older MEMORY_* schemas are not accepted by the runtime;
-- apply migrations/oracle/002_memory_to_canonical.sql explicitly, then re-run
-- this file only for a fresh schema (this file is intentionally not a migrator).

CREATE TABLE hive_schema_metadata (
  component       VARCHAR2(64) PRIMARY KEY,
  schema_version  NUMBER(10) NOT NULL,
  installed_at    TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

INSERT INTO hive_schema_metadata (component, schema_version)
VALUES ('stiphos-oracle', 2);

CREATE TABLE agents (
  urn                    VARCHAR2(256) PRIMARY KEY,
  kind                   VARCHAR2(64) NOT NULL,
  host                   VARCHAR2(128) NOT NULL,
  session_id             VARCHAR2(64) NOT NULL,
  pid                    NUMBER(10),
  runtime                VARCHAR2(64),
  model                  VARCHAR2(128),
  provider               VARCHAR2(64),
  cost_tier              VARCHAR2(1) CHECK (cost_tier IN ('A','B','C')),
  autonomy_level         VARCHAR2(32) CHECK (autonomy_level IN ('autonomous','confirm-risky','interactive','unknown')),
  auth_method            VARCHAR2(32) DEFAULT 'unknown' NOT NULL CHECK (auth_method IN ('subscription','api','free','unknown')),
  plan_cap_usd           NUMBER(12,4),
  plan_period_used_usd   NUMBER(12,6) DEFAULT 0 NOT NULL,
  subscription_pools     CLOB CHECK (subscription_pools IS JSON),
  capabilities           CLOB CHECK (capabilities IS JSON),
  version                VARCHAR2(64),
  started_at             TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  last_heartbeat         TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  status                 VARCHAR2(16) DEFAULT 'online' NOT NULL CHECK (status IN ('online','idle','offline','error')),
  metadata               CLOB CHECK (metadata IS JSON)
);
CREATE INDEX idx_agents_status    ON agents(status);
CREATE INDEX idx_agents_kind      ON agents(kind);
CREATE INDEX idx_agents_runtime   ON agents(runtime);
CREATE INDEX idx_agents_cost_tier ON agents(cost_tier);
CREATE INDEX idx_agents_host_pid  ON agents(host, pid, status);

CREATE TABLE jobs (
  id                    RAW(16) PRIMARY KEY,
  submitter_urn         VARCHAR2(256) NOT NULL,
  parent_job_id         RAW(16),
  kind                  VARCHAR2(64) NOT NULL,
  description           VARCHAR2(4000),
  priority              NUMBER(5) DEFAULT 0 NOT NULL,
  deadline              TIMESTAMP WITH TIME ZONE,
  required_capabilities CLOB CHECK (required_capabilities IS JSON),
  eligible_kinds        CLOB CHECK (eligible_kinds IS JSON),
  project               VARCHAR2(256),
  max_cost_tier         VARCHAR2(1) DEFAULT 'A' NOT NULL CHECK (max_cost_tier IN ('A','B','C')),
  preferred_providers   CLOB CHECK (preferred_providers IS JSON),
  preferred_models      CLOB CHECK (preferred_models IS JSON),
  mnemos_refs           CLOB CHECK (mnemos_refs IS JSON),
  depends_on            CLOB CHECK (depends_on IS JSON),
  status                VARCHAR2(16) NOT NULL CHECK (status IN ('queued','offered','claimed','running','done','failed','cancelled')),
  claimed_by            VARCHAR2(256),
  claimed_at            TIMESTAMP WITH TIME ZONE,
  claimed_runtime       VARCHAR2(64),
  claimed_model         VARCHAR2(128),
  claimed_provider      VARCHAR2(64),
  claimed_cost_tier     VARCHAR2(1) CHECK (claimed_cost_tier IN ('A','B','C')),
  started_at            TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  retry_backoff_until   TIMESTAMP WITH TIME ZONE,
  routed_at             TIMESTAMP WITH TIME ZONE,
  routing_metadata      CLOB CHECK (routing_metadata IS JSON),
  ended_at              TIMESTAMP WITH TIME ZONE,
  result                CLOB CHECK (result IS JSON),
  result_mnemos_id      VARCHAR2(64),
  tokens_in             NUMBER(15),
  tokens_out            NUMBER(15),
  estimated_cost_usd    NUMBER(12,6),
  retry_count           NUMBER(5) DEFAULT 0 NOT NULL,
  max_retries           NUMBER(5) DEFAULT 2 NOT NULL
);
CREATE INDEX idx_jobs_queue     ON jobs(status, priority DESC, started_at ASC);
CREATE INDEX idx_jobs_submitter ON jobs(submitter_urn);
CREATE INDEX idx_jobs_claimed   ON jobs(claimed_by);
CREATE INDEX idx_jobs_parent    ON jobs(parent_job_id);
CREATE INDEX idx_jobs_tier      ON jobs(claimed_cost_tier);
CREATE INDEX idx_jobs_project   ON jobs(project);
CREATE INDEX idx_jobs_retry     ON jobs(status, retry_backoff_until);
CREATE INDEX idx_jobs_costs     ON jobs(ended_at, claimed_provider, claimed_model);

CREATE TABLE hive_messages (
  id           VARCHAR2(64) PRIMARY KEY,
  from_urn     VARCHAR2(256) NOT NULL,
  to_urn       VARCHAR2(256),
  in_reply_to  VARCHAR2(64),
  topic        VARCHAR2(256) NOT NULL,
  payload      CLOB NOT NULL CHECK (payload IS JSON),
  ts           TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE INDEX idx_hive_messages_to    ON hive_messages(to_urn);
CREATE INDEX idx_hive_messages_topic ON hive_messages(topic);
CREATE INDEX idx_hive_messages_ts    ON hive_messages(ts);

CREATE SEQUENCE hive_events_seq START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE TABLE hive_events (
  id          NUMBER(19) DEFAULT hive_events_seq.NEXTVAL PRIMARY KEY,
  ts          TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  kind        VARCHAR2(64) NOT NULL,
  payload     CLOB NOT NULL CHECK (payload IS JSON),
  agent_urn   VARCHAR2(256)
);
CREATE INDEX idx_hive_events_ts    ON hive_events(ts);
CREATE INDEX idx_hive_events_kind  ON hive_events(kind);
CREATE INDEX idx_hive_events_agent ON hive_events(agent_urn);

CREATE TABLE hive_cache (
  cache_key         VARCHAR2(64) PRIMARY KEY,
  result_json       CLOB NOT NULL CHECK (result_json IS JSON),
  source_job_id     RAW(16),
  result_mnemos_id  VARCHAR2(64),
  hit_count         NUMBER(15) DEFAULT 0 NOT NULL,
  cost_saved_usd    NUMBER(12,6) DEFAULT 0 NOT NULL,
  model             VARCHAR2(128),
  provider          VARCHAR2(64),
  cached_at         TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  last_hit_at       TIMESTAMP WITH TIME ZONE
);
CREATE INDEX idx_hive_cache_cached ON hive_cache(cached_at);
CREATE INDEX idx_hive_cache_hits   ON hive_cache(hit_count DESC, cost_saved_usd DESC);
CREATE INDEX idx_hive_cache_source ON hive_cache(source_job_id);

CREATE TABLE hive_worker_kind_stats (
  urn                 VARCHAR2(256) NOT NULL,
  kind                VARCHAR2(64) NOT NULL,
  success_count       NUMBER(15) DEFAULT 0 NOT NULL,
  fail_count          NUMBER(15) DEFAULT 0 NOT NULL,
  cancelled_count     NUMBER(15) DEFAULT 0 NOT NULL,
  total_tokens_in     NUMBER(15) DEFAULT 0 NOT NULL,
  total_tokens_out    NUMBER(15) DEFAULT 0 NOT NULL,
  total_cost_usd      NUMBER(12,6) DEFAULT 0 NOT NULL,
  total_duration_sec  NUMBER(15,3) DEFAULT 0 NOT NULL,
  last_run            TIMESTAMP WITH TIME ZONE,
  CONSTRAINT pk_hive_worker_kind_stats PRIMARY KEY (urn, kind)
);
CREATE INDEX idx_hive_wks_kind     ON hive_worker_kind_stats(kind);
CREATE INDEX idx_hive_wks_success  ON hive_worker_kind_stats(success_count DESC);
CREATE INDEX idx_hive_wks_last_run ON hive_worker_kind_stats(last_run);

CREATE TABLE hive_scheduled_jobs (
  id                VARCHAR2(64) PRIMARY KEY,
  name              VARCHAR2(256) NOT NULL,
  created_by_urn    VARCHAR2(256) NOT NULL,
  interval_seconds  NUMBER(10) NOT NULL,
  job_template      CLOB NOT NULL CHECK (job_template IS JSON),
  enabled           NUMBER(1) DEFAULT 1 NOT NULL CHECK (enabled IN (0,1)),
  last_fired_at     TIMESTAMP WITH TIME ZONE,
  next_fire_at      TIMESTAMP WITH TIME ZONE NOT NULL,
  fire_count        NUMBER(15) DEFAULT 0 NOT NULL,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE INDEX idx_hive_sched_due     ON hive_scheduled_jobs(enabled, next_fire_at);
CREATE INDEX idx_hive_sched_creator ON hive_scheduled_jobs(created_by_urn);
CREATE INDEX idx_hive_sched_created ON hive_scheduled_jobs(created_at);
