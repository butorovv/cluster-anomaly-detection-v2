-- P09 transport metadata and progress. V1-V5 remain byte-for-byte immutable.
CREATE TABLE cad_v2.worker_endpoints (
 execution_id uuid NOT NULL, group_id text NOT NULL, owner_id uuid NOT NULL,
 epoch bigint NOT NULL, endpoint text NOT NULL CHECK(endpoint ~ '^https?://'),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(execution_id,group_id),
 FOREIGN KEY(execution_id,group_id) REFERENCES cad_v2.execution_partitions
);
CREATE TABLE cad_v2.master_poll_cursors (
 execution_id uuid NOT NULL, group_id text NOT NULL,
 checkpoint_revision bigint NOT NULL CHECK(checkpoint_revision>=0), ordinal integer NOT NULL CHECK(ordinal>=0),
 PRIMARY KEY(execution_id,group_id), FOREIGN KEY(execution_id,group_id) REFERENCES cad_v2.execution_partitions
);
CREATE TABLE cad_v2.result_topic_bindings (
 execution_id uuid PRIMARY KEY REFERENCES cad_v2.executions,
 cluster_id text NOT NULL, topic_id text NOT NULL, consumer_group text NOT NULL,
 CHECK(length(cluster_id)>0 AND length(topic_id)>0 AND length(consumer_group)>0)
);
CREATE TABLE cad_v2.result_consumer_offsets (
 execution_id uuid NOT NULL REFERENCES cad_v2.result_topic_bindings,
 partition_id integer NOT NULL CHECK(partition_id BETWEEN 0 AND 15),
 next_offset bigint NOT NULL CHECK(next_offset>=0), PRIMARY KEY(execution_id,partition_id)
);
CREATE FUNCTION cad_v2.advertise_worker(x uuid,g text,o uuid,e bigint,url text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
BEGIN
 PERFORM cad_v2.assert_owner(x,g,o,e);
 IF url IS NULL OR url !~ '^https?://' OR length(url)>2048 THEN RAISE EXCEPTION 'invalid endpoint' USING ERRCODE='22023'; END IF;
 INSERT INTO cad_v2.worker_endpoints VALUES(x,g,o,e,url,clock_timestamp())
 ON CONFLICT(execution_id,group_id) DO UPDATE SET owner_id=o,epoch=e,endpoint=url,updated_at=clock_timestamp();
END $$;
CREATE FUNCTION cad_v2.read_outbox(x uuid,g text,o uuid,e bigint,r bigint,n integer,batch integer,pending boolean)
RETURNS TABLE(checkpoint_revision bigint,ordinal integer,envelope jsonb,created_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
BEGIN
 PERFORM cad_v2.assert_owner(x,g,o,e);
 IF batch IS NULL OR batch<1 OR batch>128 OR r<0 OR n<0 THEN RAISE EXCEPTION 'invalid page' USING ERRCODE='22023'; END IF;
 RETURN QUERY SELECT m.checkpoint_revision,m.ordinal,m.envelope,m.created_at FROM cad_v2.outbox_messages m
 WHERE m.execution_id=x AND m.group_id=g AND (m.checkpoint_revision,m.ordinal)>(r,n) AND (NOT pending OR m.delivered_at IS NULL)
 ORDER BY m.checkpoint_revision,m.ordinal LIMIT batch;
END $$;
CREATE FUNCTION cad_v2.advance_poll_cursor(x uuid,g text,r bigint,n integer) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
BEGIN
 IF r IS NULL OR n IS NULL OR r<1 OR n<1 THEN RAISE EXCEPTION 'invalid delivery cursor' USING ERRCODE='22023'; END IF;
 INSERT INTO cad_v2.master_poll_cursors VALUES(x,g,r,n) ON CONFLICT(execution_id,group_id)
 DO UPDATE SET checkpoint_revision=r,ordinal=n
 WHERE (cad_v2.master_poll_cursors.checkpoint_revision,cad_v2.master_poll_cursors.ordinal)<(r,n);
END $$;
CREATE FUNCTION cad_v2.ack_poll_cursor(x uuid,g text,o uuid,e bigint,r bigint,n integer) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
DECLARE durable cad_v2.master_poll_cursors; m record;
BEGIN
 PERFORM cad_v2.assert_owner(x,g,o,e);
 SELECT * INTO durable FROM cad_v2.master_poll_cursors WHERE execution_id=x AND group_id=g;
 IF NOT FOUND OR (durable.checkpoint_revision,durable.ordinal)<(r,n) THEN RETURN; END IF;
 FOR m IN SELECT message_id FROM cad_v2.outbox_messages WHERE execution_id=x AND group_id=g AND delivered_at IS NULL AND (checkpoint_revision,ordinal)<=(r,n) ORDER BY checkpoint_revision,ordinal LIMIT 128 LOOP
  PERFORM cad_v2.ack_outbox(x,g,o,e,m.message_id);
 END LOOP;
END $$;
CREATE FUNCTION cad_v2.bind_result_source(x uuid,c text,t text,cg text,starts jsonb) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
DECLARE saved cad_v2.result_topic_bindings; i integer; off bigint;
BEGIN
 PERFORM 1 FROM cad_v2.executions WHERE execution_id=x FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'execution absent' USING ERRCODE='22023'; END IF;
 SELECT * INTO saved FROM cad_v2.result_topic_bindings WHERE execution_id=x;
 IF FOUND THEN
  IF (saved.cluster_id,saved.topic_id,saved.consumer_group) IS DISTINCT FROM (c,t,cg) THEN RAISE EXCEPTION 'results source incarnation mismatch' USING ERRCODE='55000'; END IF;
  RETURN;
 END IF;
 INSERT INTO cad_v2.result_topic_bindings VALUES(x,c,t,cg);
 FOR i IN 0..15 LOOP
  off:=(starts->>i::text)::bigint;
  IF off IS NULL OR off<0 THEN RAISE EXCEPTION 'explicit results start offsets required' USING ERRCODE='22023'; END IF;
  INSERT INTO cad_v2.result_consumer_offsets VALUES(x,i,off);
 END LOOP;
END $$;
CREATE FUNCTION cad_v2.advance_result_offset(x uuid,t text,p integer,expected bigint,next bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
DECLARE old bigint;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM cad_v2.result_topic_bindings WHERE execution_id=x AND topic_id=t) THEN RAISE EXCEPTION 'results topic identity mismatch' USING ERRCODE='55000'; END IF;
 SELECT next_offset INTO STRICT old FROM cad_v2.result_consumer_offsets WHERE execution_id=x AND partition_id=p FOR UPDATE;
 IF old<>expected OR next<>expected+1 THEN RAISE EXCEPTION 'results progress discontinuity' USING ERRCODE='55000'; END IF;
 UPDATE cad_v2.result_consumer_offsets SET next_offset=next WHERE execution_id=x AND partition_id=p;
END $$;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cad_v2 FROM PUBLIC;
