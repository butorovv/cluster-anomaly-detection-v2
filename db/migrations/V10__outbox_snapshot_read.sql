-- P16-R1: a page authorizes a read of already committed, immutable output.
-- It does not claim/delete/ACK output or extend the lease. Late delivery may
-- finish after takeover; all mutations still use V3 assert_owner FOR UPDATE.
-- STABLE gives owner and both message tables the calling command's snapshot.
-- The lease is checked at command start, not transaction start or delivery time.
-- Require a new READ COMMITTED snapshot per call; an old repeatable snapshot
-- must not authorize a new page after a committed takeover.
-- Deployment: quiesce Worker input/output/HTTP requests and finish their TXs;
-- apply V10, refresh runtime grants and deploy the new Worker before resuming.
-- Legacy read_outbox remains unchanged for historical/admin verification.
CREATE FUNCTION cad_v2.read_outbox_page(
  x uuid, g text, o uuid, e bigint, r bigint, n integer, batch integer, pending boolean
) RETURNS TABLE(checkpoint_revision bigint, ordinal integer, envelope jsonb, created_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
DECLARE owner_row cad_v2.partition_ownership;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'outbox page requires READ COMMITTED' USING ERRCODE = '0A000';
  END IF;
  SELECT * INTO STRICT owner_row FROM cad_v2.partition_ownership
    WHERE execution_id = x AND group_id = g;
  IF o IS NULL OR e IS NULL OR owner_row.owner_id IS DISTINCT FROM o
     OR owner_row.epoch <> e OR owner_row.lease_until IS NULL
     OR owner_row.lease_until <= statement_timestamp() THEN
    RAISE EXCEPTION 'stale or expired owner' USING ERRCODE = '55000';
  END IF;
  IF batch IS NULL OR batch < 1 OR batch > 128
     OR r IS NULL OR n IS NULL OR r < 0 OR n < 0 OR pending IS NULL THEN
    RAISE EXCEPTION 'invalid page' USING ERRCODE = '22023';
  END IF;
  RETURN QUERY
    SELECT m.checkpoint_revision, m.ordinal, m.envelope, m.created_at
    FROM (
      SELECT a.checkpoint_revision, a.ordinal, a.envelope, a.created_at, a.delivered_at
        FROM cad_v2.outbox_messages a WHERE a.execution_id = x AND a.group_id = g
      UNION ALL
      SELECT f.checkpoint_revision, f.ordinal, f.envelope, f.created_at, f.delivered_at
        FROM cad_v2.worker_tick_fragments f WHERE f.execution_id = x AND f.group_id = g
    ) m
    WHERE (m.checkpoint_revision, m.ordinal) > (r, n)
      AND (NOT pending OR m.delivered_at IS NULL)
    ORDER BY m.checkpoint_revision, m.ordinal LIMIT batch;
END $$;
REVOKE EXECUTE ON FUNCTION cad_v2.read_outbox_page(uuid,text,uuid,bigint,bigint,integer,integer,boolean) FROM PUBLIC;
