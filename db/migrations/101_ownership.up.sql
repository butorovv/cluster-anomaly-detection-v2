-- Each acquisition changes epoch, including reacquisition by the same instance.
-- Kafka assignment authorization is external: call ONLY for assigned partitions.
/* Assignment: BEGIN → acquire → прочитать checkpoint → немедленно COMMIT.
   Checkpoint отсутствует: start_offset. Иначе проверить hash/version/codec/все
   frozen bindings, Restore полного DTO вне длинной tx, seek на DB next_offset.
   Поддерживать lease во время Restore. При mismatch — остановка, не пустой Journal.
   Kafka committed offset не авторитет. UUID относится к инстанции процесса.
   Renew не оживляет expired lease; после release/expiry каждый acquire повышает epoch.

   Все writer API держат ownership row FOR UPDATE до COMMIT/ROLLBACK. Время берётся
   clock_timestamp ПОСЛЕ ожидания lock. Если TTL истёк между возвратом функции и
   COMMIT, новый epoch всё равно получит lock ПОСЛЕ старой tx. Старый commit
   сериализован перед takeover, wall-clock check точно в момент COMMIT не обещается.
   Длинная tx задерживает takeover. Выставить lock_timeout, statement_timeout,
   idle_in_transaction_session_timeout; не выполнять сеть внутри tx.
   READ COMMITTED baseline; 40001/40P01 → rollback и новое чтение. Одна group на tx;
   при нескольких группах согласовать общий порядок блокировок.
   Graceful revoke: остановить чтение, commit допустимого complete boundary,
   остановить output loop, release. Старый epoch после takeover не пишет state/output.
*/
CREATE FUNCTION cad_v2.acquire_partition(p_execution uuid, p_group text, p_owner uuid, p_ttl interval)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
DECLARE o cad_v2.partition_ownership; t timestamptz;
BEGIN
  IF p_owner IS NULL OR p_ttl IS NULL OR p_ttl <= interval '0' OR p_ttl > interval '5 minutes' THEN
    RAISE EXCEPTION 'invalid owner or lease TTL' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO STRICT o FROM cad_v2.partition_ownership
    WHERE execution_id = p_execution AND group_id = p_group FOR UPDATE;
  t := clock_timestamp(); -- sample AFTER lock wait, never transaction_timestamp()
  IF o.owner_id IS NOT NULL AND o.lease_until > t THEN
    RAISE EXCEPTION 'partition is leased' USING ERRCODE = '55000';
  END IF;
  UPDATE cad_v2.partition_ownership SET owner_id = p_owner, epoch = o.epoch + 1,
    lease_until = t + p_ttl, updated_at = t
    WHERE execution_id = p_execution AND group_id = p_group;
  RETURN o.epoch + 1;
END $$;

-- Internal helper: lock is held until COMMIT/ROLLBACK by every caller.
CREATE FUNCTION cad_v2.assert_owner(p_execution uuid, p_group text, p_owner uuid, p_epoch bigint)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, cad_v2 AS $$
DECLARE o cad_v2.partition_ownership;
BEGIN
  SELECT * INTO STRICT o FROM cad_v2.partition_ownership
    WHERE execution_id = p_execution AND group_id = p_group FOR UPDATE;
  IF p_owner IS NULL OR p_epoch IS NULL OR o.owner_id IS DISTINCT FROM p_owner
     OR o.epoch <> p_epoch OR o.lease_until IS NULL OR o.lease_until <= clock_timestamp() THEN
    RAISE EXCEPTION 'stale or expired owner' USING ERRCODE = '55000';
  END IF;
END $$;

CREATE FUNCTION cad_v2.renew_partition(p_execution uuid, p_group text, p_owner uuid, p_epoch bigint, p_ttl interval)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
BEGIN
  IF p_ttl IS NULL OR p_ttl <= interval '0' OR p_ttl > interval '5 minutes' THEN
    RAISE EXCEPTION 'invalid lease TTL' USING ERRCODE = '22023';
  END IF;
  PERFORM cad_v2.assert_owner(p_execution, p_group, p_owner, p_epoch);
  UPDATE cad_v2.partition_ownership SET lease_until = clock_timestamp() + p_ttl, updated_at = clock_timestamp()
    WHERE execution_id = p_execution AND group_id = p_group;
END $$;

CREATE FUNCTION cad_v2.release_partition(p_execution uuid, p_group text, p_owner uuid, p_epoch bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
BEGIN
  PERFORM cad_v2.assert_owner(p_execution, p_group, p_owner, p_epoch);
  UPDATE cad_v2.partition_ownership SET owner_id = NULL, lease_until = NULL, updated_at = clock_timestamp()
    WHERE execution_id = p_execution AND group_id = p_group;
END $$;

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cad_v2 FROM PUBLIC;
