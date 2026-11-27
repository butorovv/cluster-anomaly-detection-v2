CREATE SCHEMA cad_archive;

-- A V1 database represents one source. Its measurement uniqueness key omits source identity.
CREATE TABLE cad_archive.source (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 sha256 text NOT NULL UNIQUE CHECK (sha256 ~ '^[a-f0-9]{64}$'),
 feature_schema text NOT NULL,
 topology_sha256 text NOT NULL CHECK (topology_sha256 ~ '^[a-f0-9]{64}$'),
 step_seconds integer NOT NULL CHECK (step_seconds = 10)
);

CREATE TABLE cad_archive.imports (
 source_sha256 text NOT NULL REFERENCES cad_archive.source(sha256),
 window_start timestamptz NOT NULL,
 window_end timestamptz NOT NULL CHECK (window_end > window_start),
 groups integer NOT NULL CHECK (groups > 0),
 hosts integer NOT NULL CHECK (hosts > 0),
 observations bigint NOT NULL CHECK (observations > 0),
 measurements bigint NOT NULL CHECK (measurements > 0),
 verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (source_sha256, window_start, window_end)
);
