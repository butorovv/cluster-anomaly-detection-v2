-- Parameters: $1 node_id (integer), $2 logical timestamp (timestamptz).
-- Missing coordinates stay NULL. Timestamp is the requested logical timestamp.
SELECT n.hostname, g.code AS group_id, n.node_index, $2::timestamptz AS timestamp,
       (max(t.value) FILTER (WHERE ms.source_key = 'psu0_power'))::double precision AS psu0_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'psu1_power'))::double precision AS psu1_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'cpu0_power'))::double precision AS cpu0_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'cpu1_power'))::double precision AS cpu1_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu0_power'))::double precision AS gpu0_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu1_power'))::double precision AS gpu1_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu2_power'))::double precision AS gpu2_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu3_power'))::double precision AS gpu3_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu4_power'))::double precision AS gpu4_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu5_power'))::double precision AS gpu5_power,
       (max(t.value) FILTER (WHERE ms.source_key = 'cpu0_mean_temp'))::double precision AS cpu0_mean_temp,
       (max(t.value) FILTER (WHERE ms.source_key = 'cpu1_mean_temp'))::double precision AS cpu1_mean_temp,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu0_core_temp'))::double precision AS gpu0_core_temp,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu1_core_temp'))::double precision AS gpu1_core_temp,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu2_core_temp'))::double precision AS gpu2_core_temp,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu3_core_temp'))::double precision AS gpu3_core_temp,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu4_core_temp'))::double precision AS gpu4_core_temp,
       (max(t.value) FILTER (WHERE ms.source_key = 'gpu5_core_temp'))::double precision AS gpu5_core_temp
FROM project.node n
JOIN project.node_group g ON g.id = n.node_group_id
JOIN project.component c ON c.node_id = n.id
JOIN project.metric_source ms ON ms.component_id = c.id
LEFT JOIN project.telemetry t ON t.metric_source_id = ms.id AND t.ts = $2::timestamptz
WHERE n.id = $1
GROUP BY n.id, n.hostname, g.code, n.node_index;
