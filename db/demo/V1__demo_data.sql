-- Optional teaching data; never apply to the Summit database.
set search_path = project;
insert into node_group (code, name, group_type, description)
values ('SUMMIT', 'Summit (демонстрационный корень)', 'cluster', 'DEMO: пример корня топологии');
insert into node_group (parent_id, code, name, group_type, description)
select id, 'a01', 'Топологическая группа a01', 'topological', 'DEMO: не утверждение о физическом шкафе'
from node_group where code = 'SUMMIT';
insert into node (node_group_id, hostname, node_index, description)
select id, 'a01n01', 1, 'DEMO: пример узла' from node_group where code = 'a01';
insert into component (node_id, kind, local_index)
select n.id, c.kind, i.local_index
from node n
cross join (values ('cpu', 1), ('gpu', 5), ('psu', 1)) as c(kind, max_index)
cross join lateral generate_series(0, c.max_index) as i(local_index)
where n.hostname = 'a01n01';



-- 18 источников: мощность PSU/CPU/GPU и температура CPU/GPU.
insert into metric_source (component_id, metric_type_id, source_key, description)
select c.id, mt.id,
    case mt.code
        when 'PSU_INPUT_POWER' then format('ps%s_input_power', c.local_index)
        when 'CPU_POWER' then format('p%s_power', c.local_index)
        when 'CPU_MEAN_TEMP' then format('p%s_mean_temp', c.local_index)
        when 'GPU_POWER' then format('p%s_gpu%s_power', c.local_index / 3, c.local_index)
        when 'GPU_CORE_TEMP' then format('gpu%s_core_temp', c.local_index)
    end,
    'DEMO: источник F18 после preprocessing'
from component c
join node n on n.id = c.node_id and n.hostname = 'a01n01'
join metric_type mt on mt.component_kind = c.kind;

-- В 09:01 отсутствует измерение температуры GPU5.
insert into telemetry (metric_source_id, ts, value)
select ms.id, t.ts,
    (case mt.code
        when 'PSU_INPUT_POWER' then 700.00 + c.local_index * 10
        when 'CPU_POWER' then 130.10 + c.local_index * 5
        when 'CPU_MEAN_TEMP' then 51.875 + c.local_index
        when 'GPU_POWER' then 210.40 + c.local_index * 3
        when 'GPU_CORE_TEMP' then 60.75 + c.local_index
    end) + t.delta
from metric_source ms
join component c on c.id = ms.component_id
join node n on n.id = c.node_id and n.hostname = 'a01n01'
join metric_type mt on mt.id = ms.metric_type_id
cross join (values
    ('2026-10-01 09:00:00+00'::timestamptz, 0::numeric),
    ('2026-10-01 09:01:00+00'::timestamptz, 0.25::numeric)
) as t(ts, delta)
where not (mt.code = 'GPU_CORE_TEMP' and c.local_index = 5
           and t.ts = '2026-10-01 09:01:00+00'::timestamptz);

insert into event (node_id, occurred_at, severity, event_type, message)
select id, '2026-10-01 10:00:00+00', 'info', 'maintenance', 'DEMO: учебный осмотр, не реальное событие Summit'
from node where hostname = 'a01n01';
insert into engineer (tab_no, full_name, email)
values ('DEMO-001', 'Демонстрационный инженер', 'demo.engineer@example.org');
insert into maintenance (node_id, engineer_id, performed_at, maintenance_type, notes)
select n.id, e.id, '2026-10-01 10:00:00+00', 'inspection', 'DEMO: синтетический осмотр, не реальные работы Summit'
from node n cross join engineer e where n.hostname = 'a01n01' and e.tab_no = 'DEMO-001';
insert into resource (code, name, resource_type, description)
values ('DEMO-WIPE', 'Салфетка для очистки', 'consumable', 'DEMO: учётная единица — штука');
insert into maintenance_resource (maintenance_id, resource_id, qty)
select m.id, r.id, 2 from maintenance m
join node n on n.id = m.node_id and n.hostname = 'a01n01'
join engineer e on e.id = m.engineer_id and e.tab_no = 'DEMO-001'
cross join resource r
where m.performed_at = '2026-10-01 10:00:00+00'::timestamptz and r.code = 'DEMO-WIPE';
insert into document (node_id, document_type, title, url)
select id, 'maintenance_report', 'DEMO: пример ссылки на отчёт осмотра', 'https://example.org/demo/a01n01/inspection'
from node where hostname = 'a01n01';
