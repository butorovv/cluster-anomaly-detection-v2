CREATE TABLE cad_archive.stream_executions (
 execution_id uuid PRIMARY KEY,
 source_sha256 text NOT NULL REFERENCES cad_archive.source(sha256),
 profile_sha256 text NOT NULL CHECK (profile_sha256 ~ '^[a-f0-9]{64}$'),
 cluster_id text NOT NULL,
 topic_id text NOT NULL,
 window_start timestamptz NOT NULL,
 window_end timestamptz NOT NULL CHECK (window_end > window_start)
);

CREATE TABLE cad_archive.positions (
 execution_id uuid NOT NULL REFERENCES cad_archive.stream_executions,
 partition_id integer NOT NULL CHECK (partition_id BETWEEN 0 AND 15),
 owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
 next_offset bigint NOT NULL CHECK (next_offset >= 0),
 next_tick timestamptz NOT NULL,
 PRIMARY KEY (execution_id, partition_id)
);

CREATE TABLE cad_archive.coverage (
 execution_id uuid NOT NULL,
 partition_id integer NOT NULL,
 tick timestamptz NOT NULL,
 -- Entry -1 means no record for that host; entry 0 means a record with no channels.
 masks integer[] NOT NULL CHECK (array_length(masks, 1) = 18),
 inserted integer NOT NULL CHECK (inserted >= 0),
 duplicates integer NOT NULL CHECK (duplicates >= 0),
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (execution_id, partition_id, tick),
 FOREIGN KEY (execution_id,partition_id) REFERENCES cad_archive.positions
);
