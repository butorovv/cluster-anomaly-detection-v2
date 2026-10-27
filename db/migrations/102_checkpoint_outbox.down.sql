DROP FUNCTION cad_v2.ack_outbox(uuid, text, uuid, bigint, text);
DROP FUNCTION cad_v2.commit_checkpoint(uuid, text, uuid, bigint, bigint, bigint, timestamptz, integer, text, bytea, cad_v2.sha256_hex, boolean, jsonb);
DROP FUNCTION cad_v2.validate_envelope(uuid, jsonb);
