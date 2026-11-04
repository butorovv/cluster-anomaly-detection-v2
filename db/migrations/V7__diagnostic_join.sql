-- P10 durable diagnostic fragments. These are explicitly NOT final group_result.
CREATE TABLE cad_v2.worker_tick_fragments (
 execution_id uuid NOT NULL, group_id text NOT NULL, event_time timestamptz NOT NULL,
 message_id cad_v2.sha256_hex NOT NULL, checkpoint_revision bigint NOT NULL,
 ordinal integer NOT NULL CHECK(ordinal>0), envelope jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), delivered_at timestamptz,
 PRIMARY KEY(execution_id,group_id,event_time), UNIQUE(execution_id,message_id),
 UNIQUE(execution_id,group_id,checkpoint_revision,ordinal),
 FOREIGN KEY(execution_id,group_id) REFERENCES cad_v2.execution_partitions
);
CREATE INDEX worker_fragments_pending ON cad_v2.worker_tick_fragments(execution_id,group_id,checkpoint_revision,ordinal) WHERE delivered_at IS NULL;
CREATE TABLE cad_v2.master_tick_fragments (
 execution_id uuid NOT NULL, group_id text NOT NULL, event_time timestamptz NOT NULL,
 message_id cad_v2.sha256_hex NOT NULL, envelope jsonb NOT NULL,
 PRIMARY KEY(execution_id,group_id,event_time), UNIQUE(execution_id,message_id),
 FOREIGN KEY(execution_id,group_id) REFERENCES cad_v2.execution_partitions
);
CREATE INDEX master_fragments_tick_idx ON cad_v2.master_tick_fragments(execution_id,event_time,group_id);
CREATE TABLE cad_v2.master_tick_joins (
 execution_id uuid NOT NULL REFERENCES cad_v2.executions, event_time timestamptz NOT NULL,
 joined_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(execution_id,event_time)
);
CREATE FUNCTION cad_v2.validate_tick_fragment(x uuid,m jsonb) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,cad_v2 AS $$
DECLARE r cad_v2.runs; t timestamptz;
BEGIN
 SELECT z.* INTO STRICT r FROM cad_v2.runs z JOIN cad_v2.executions e USING(run_id) WHERE e.execution_id=x;
 IF (m->>'schema_version'='2' AND m->>'kind'='tick_fragment' AND m->>'execution_id'=x::text AND m->>'run_id'=r.run_id AND m->>'config_id'=r.config_id AND m->>'message_id' ~ '^[0-9a-f]{64}$' AND m->>'revision'='1' AND m->>'terminal'='false' AND NOT(m ? 'event_id') AND jsonb_typeof(m->'payload')='object' AND m->>'event_time' ~ 'Z$' AND m#>>'{payload,raw,group}'=m->>'group_id' AND m#>>'{payload,temporal,group}'=m->>'group_id' AND (m#>'{payload,raw,if,outside_groups}')='null'::jsonb) IS NOT TRUE THEN RAISE EXCEPTION 'invalid diagnostic fragment' USING ERRCODE='22023'; END IF;
 IF NOT EXISTS(SELECT 1 FROM cad_v2.execution_partitions WHERE execution_id=x AND group_id=m->>'group_id') THEN RAISE EXCEPTION 'fragment group outside execution' USING ERRCODE='22023'; END IF;
 t:=(m->>'event_time')::timestamptz;
 IF NOT(isfinite(t) AND t>=r.window_start AND t<r.window_end AND mod(extract(epoch FROM t),10)=0) OR (m#>>'{payload,raw,timestamp}')::timestamptz IS DISTINCT FROM t OR (m#>>'{payload,temporal,timestamp}')::timestamptz IS DISTINCT FROM t THEN RAISE EXCEPTION 'invalid fragment time' USING ERRCODE='22023'; END IF;
END $$;
CREATE FUNCTION cad_v2.stage_tick_fragment(x uuid,g text,o uuid,e bigint,r bigint,n integer,m jsonb) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
DECLARE old cad_v2.worker_tick_fragments; cp cad_v2.group_checkpoints;
BEGIN
 PERFORM cad_v2.assert_owner(x,g,o,e); PERFORM cad_v2.validate_tick_fragment(x,m);
 SELECT * INTO STRICT cp FROM cad_v2.group_checkpoints WHERE execution_id=x AND group_id=g;
 IF cp.checkpoint_revision<>r OR cp.owner_epoch<>e OR (m->>'event_time')::timestamptz>cp.last_tick OR m->>'group_id'<>g OR n<1 THEN RAISE EXCEPTION 'fragment outside durable checkpoint' USING ERRCODE='22023'; END IF;
 SELECT * INTO old FROM cad_v2.worker_tick_fragments WHERE execution_id=x AND group_id=g AND event_time=(m->>'event_time')::timestamptz;
 IF FOUND THEN IF old.envelope IS DISTINCT FROM m THEN RAISE EXCEPTION 'conflicting fragment retry' USING ERRCODE='23505'; END IF; RETURN; END IF;
 INSERT INTO cad_v2.worker_tick_fragments(execution_id,group_id,event_time,message_id,checkpoint_revision,ordinal,envelope) VALUES(x,g,(m->>'event_time')::timestamptz,m->>'message_id',r,n,m);
END $$;
CREATE FUNCTION cad_v2.accept_tick_fragment(x uuid,m jsonb) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
DECLARE old cad_v2.master_tick_fragments;
BEGIN
 PERFORM cad_v2.validate_tick_fragment(x,m);
 -- One short transaction joins at most 16 groups; no full D2 RAM accumulation.
 PERFORM pg_advisory_xact_lock(hashtextextended(x::text || '/tick/' || (m->>'event_time'),0));
 SELECT * INTO old FROM cad_v2.master_tick_fragments WHERE execution_id=x AND group_id=m->>'group_id' AND event_time=(m->>'event_time')::timestamptz;
 IF FOUND THEN IF old.envelope IS DISTINCT FROM m THEN RAISE EXCEPTION 'conflicting Master fragment retry' USING ERRCODE='23505'; END IF; RETURN false; END IF;
 INSERT INTO cad_v2.master_tick_fragments VALUES(x,m->>'group_id',(m->>'event_time')::timestamptz,m->>'message_id',m);RETURN true;
END $$;
CREATE OR REPLACE FUNCTION cad_v2.read_outbox(x uuid,g text,o uuid,e bigint,r bigint,n integer,batch integer,pending boolean)
RETURNS TABLE(checkpoint_revision bigint,ordinal integer,envelope jsonb,created_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
BEGIN
 PERFORM cad_v2.assert_owner(x,g,o,e);
 IF batch IS NULL OR batch<1 OR batch>128 OR r<0 OR n<0 THEN RAISE EXCEPTION 'invalid page' USING ERRCODE='22023'; END IF;
 RETURN QUERY SELECT all_rows.checkpoint_revision,all_rows.ordinal,all_rows.envelope,all_rows.created_at FROM (
 SELECT m.checkpoint_revision,m.ordinal,m.envelope,m.created_at,m.delivered_at FROM cad_v2.outbox_messages m WHERE m.execution_id=x AND m.group_id=g
 UNION ALL SELECT m.checkpoint_revision,m.ordinal,m.envelope,m.created_at,m.delivered_at FROM cad_v2.worker_tick_fragments m WHERE m.execution_id=x AND m.group_id=g
 ) all_rows WHERE (all_rows.checkpoint_revision,all_rows.ordinal)>(r,n) AND (NOT pending OR all_rows.delivered_at IS NULL) ORDER BY all_rows.checkpoint_revision,all_rows.ordinal LIMIT batch;
END $$;
CREATE FUNCTION cad_v2.ack_delivery(x uuid,g text,o uuid,e bigint,id text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
BEGIN
 PERFORM cad_v2.assert_owner(x,g,o,e);
 IF EXISTS(SELECT 1 FROM cad_v2.outbox_messages WHERE execution_id=x AND group_id=g AND message_id=id) THEN PERFORM cad_v2.ack_outbox(x,g,o,e,id);RETURN; END IF;
 UPDATE cad_v2.worker_tick_fragments SET delivered_at=coalesce(delivered_at,clock_timestamp()) WHERE execution_id=x AND group_id=g AND message_id=id;
 IF NOT FOUND THEN RAISE EXCEPTION 'unknown delivery message' USING ERRCODE='22023'; END IF;
END $$;
CREATE OR REPLACE FUNCTION cad_v2.ack_poll_cursor(x uuid,g text,o uuid,e bigint,r bigint,n integer) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
DECLARE durable cad_v2.master_poll_cursors; m record;
BEGIN
 PERFORM cad_v2.assert_owner(x,g,o,e);
 SELECT * INTO durable FROM cad_v2.master_poll_cursors WHERE execution_id=x AND group_id=g;
 IF NOT FOUND OR (durable.checkpoint_revision,durable.ordinal)<(r,n) THEN RETURN; END IF;
 FOR m IN SELECT z.message_id FROM (
 SELECT a.message_id,a.checkpoint_revision,a.ordinal FROM cad_v2.outbox_messages a WHERE a.execution_id=x AND a.group_id=g AND a.delivered_at IS NULL
 UNION ALL SELECT a.message_id,a.checkpoint_revision,a.ordinal FROM cad_v2.worker_tick_fragments a WHERE a.execution_id=x AND a.group_id=g AND a.delivered_at IS NULL
 ) z WHERE (z.checkpoint_revision,z.ordinal)<=(r,n) ORDER BY z.checkpoint_revision,z.ordinal LIMIT 128 LOOP PERFORM cad_v2.ack_delivery(x,g,o,e,m.message_id); END LOOP;
END $$;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cad_v2 FROM PUBLIC;
