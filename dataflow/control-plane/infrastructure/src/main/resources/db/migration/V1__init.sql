-- Schema for control-plane metadata store.
-- All identifiers are lowercase per project convention.

CREATE TABLE source (
    name           varchar(64)    PRIMARY KEY,
    type           varchar(32)    NOT NULL,
    spec           jsonb          NOT NULL,
    created_at     timestamptz    NOT NULL DEFAULT now(),
    updated_at     timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE source_status (
    source_name     varchar(64)   PRIMARY KEY REFERENCES source(name) ON DELETE CASCADE,
    debezium_status jsonb,
    topics          jsonb         NOT NULL DEFAULT '[]'::jsonb,
    observed_at     timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE tract (
    name            varchar(64)   PRIMARY KEY,
    source_name     varchar(64)   NOT NULL REFERENCES source(name) ON DELETE RESTRICT,
    desired_state   varchar(16)   NOT NULL,
    current_version bigint        NOT NULL DEFAULT 1,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    updated_at      timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT desired_state_chk CHECK (
        desired_state IN ('DRAFTED', 'DEPLOYED', 'SUSPENDED', 'DELETED', 'FAILED')
    )
);

CREATE INDEX tract_source_idx ON tract(source_name);
CREATE INDEX tract_desired_state_idx ON tract(desired_state);

CREATE TABLE tract_spec (
    tract_name      varchar(64)   NOT NULL REFERENCES tract(name) ON DELETE CASCADE,
    version         bigint        NOT NULL,
    spec            jsonb         NOT NULL,
    raw_yaml        text          NOT NULL,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (tract_name, version)
);

CREATE TABLE tract_status (
    tract_name             varchar(64)  PRIMARY KEY REFERENCES tract(name) ON DELETE CASCADE,
    desired_state          varchar(16)  NOT NULL,
    reconciliation_status  varchar(16)  NOT NULL,
    flink_job_id           varchar(128),
    connector_status       jsonb,
    flink_status           jsonb,
    sinks_status           jsonb        NOT NULL DEFAULT '[]'::jsonb,
    topics_status          jsonb        NOT NULL DEFAULT '[]'::jsonb,
    last_error             text,
    observed_spec_version  bigint       NOT NULL DEFAULT 1,
    observed_at            timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE tract_command_log (
    id          uuid          PRIMARY KEY DEFAULT gen_random_uuid(),
    tract_name  varchar(64)   NOT NULL,
    command     varchar(32)   NOT NULL,
    issued_by   varchar(128)  NOT NULL,
    payload     jsonb,
    issued_at   timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX tract_command_log_tract_idx ON tract_command_log(tract_name, issued_at DESC);

CREATE TABLE reconciliation_log (
    id              uuid           PRIMARY KEY DEFAULT gen_random_uuid(),
    tract_name      varchar(64)    NOT NULL,
    step_name       varchar(64)    NOT NULL,
    outcome         varchar(16)    NOT NULL,
    detail          text,
    duration_ms     bigint         NOT NULL DEFAULT 0,
    recorded_at     timestamptz    NOT NULL DEFAULT now(),
    CONSTRAINT outcome_chk CHECK (outcome IN ('STARTED', 'SUCCEEDED', 'FAILED', 'SKIPPED'))
);

CREATE INDEX reconciliation_log_tract_idx ON reconciliation_log(tract_name, recorded_at DESC);
