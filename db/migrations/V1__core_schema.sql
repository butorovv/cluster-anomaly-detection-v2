-- Canonical database core shared with the DB/KB project.
-- Source:
-- https://github.com/butorovv/kdbd-cluster-monitoring
-- Original path:
-- seminar02/migrations/V1__init.sql
-- Imported as the V1 database contract for NIR/KR.
-- Source commit: 901091cf143942dbf8c883d233d390787e3c51d1
create schema if not exists project;
set search_path = project;

-- Схема мониторинга вычислительного кластера.
create table node_group (
    id int generated always as identity primary key,
    parent_id int references node_group(id) on delete restrict,
    code text not null unique,
    name text not null,
    group_type text not null,
    description text,
    constraint node_group_type_check check (group_type in ('cluster', 'topological')),
    constraint node_group_parent_check check (
        (group_type = 'cluster' and parent_id is null) or
        (group_type = 'topological' and parent_id is not null)
    ),
    constraint node_group_not_self_check check (parent_id <> id)
);
comment on table node_group is 'Иерархия кластера и топологических групп; группа не обязательно физический шкаф';
comment on column node_group.group_type is 'cluster — корень без родителя; topological — группа с обязательным родителем';
comment on column node_group.parent_id is 'Родитель; прямой цикл запрещён, произвольные циклы проверяет загрузчик';

create table node (
    id int generated always as identity primary key,
    node_group_id int not null references node_group(id) on delete restrict,
    hostname text not null unique,
    node_index smallint not null,
    description text,
    constraint node_index_check check (node_index between 1 and 18)
);
comment on table node is 'Вычислительный узел, непосредственно принадлежащий одной группе';
comment on column node.node_index is 'Позиционный индекс узла 1..18; hostname — глобальный естественный ключ';

create table component (
    id int generated always as identity primary key,
    node_id int not null references node(id) on delete restrict,
    kind text not null,
    local_index smallint not null,
    model text,
    constraint component_natural_key unique (node_id, kind, local_index),
    constraint component_kind_check check (kind in ('cpu', 'gpu', 'psu')),
    constraint component_index_check check (
        (kind = 'cpu' and local_index between 0 and 1) or
        (kind = 'gpu' and local_index between 0 and 5) or
        (kind = 'psu' and local_index between 0 and 1)
    )
);
comment on table component is 'CPU, GPU и PSU — строки единой таблицы компонентов узла';
comment on column component.local_index is 'Номер внутри вида и узла: cpu/psu 0..1, gpu 0..5';

create table metric_type (
    id int generated always as identity primary key,
    code text not null unique,
    component_kind text not null,
    quantity text not null,
    unit text not null,
    description text,
    constraint metric_type_kind_check check (component_kind in ('cpu', 'gpu', 'psu')),
    constraint metric_type_quantity_check check (quantity in ('power', 'temperature')),
    constraint metric_type_definition_check check (
        (code = 'PSU_INPUT_POWER' and component_kind = 'psu' and quantity = 'power') or
        (code = 'CPU_POWER' and component_kind = 'cpu' and quantity = 'power') or
        (code = 'CPU_MEAN_TEMP' and component_kind = 'cpu' and quantity = 'temperature') or
        (code = 'GPU_POWER' and component_kind = 'gpu' and quantity = 'power') or
        (code = 'GPU_CORE_TEMP' and component_kind = 'gpu' and quantity = 'temperature')
    )
);
comment on table metric_type is 'Пять рабочих типов метрик F18: совместимый вид компонента, величина и единица';
comment on column metric_type.component_kind is 'Допустимый вид компонента; классификация типа неизменяема';
comment on column metric_type.unit is 'Единица значения; переобозначение единицы без пересчёта истории недопустимо';
comment on column metric_type.code is 'CPU_MEAN_TEMP — среднее валидных CPU core temperatures, вычисленное до ingestion';

create table metric_source (
    id int generated always as identity primary key,
    component_id int not null references component(id) on delete restrict,
    metric_type_id int not null references metric_type(id) on delete restrict,
    source_key text not null,
    description text,
    constraint metric_source_natural_key unique (component_id, metric_type_id)
);
comment on table metric_source is 'Источник рабочей метрики F18: один тип на компонент; CPU температура уже усреднена';
comment on column metric_source.source_key is 'Ключ preprocessing: например p0_mean_temp, p0_gpu0_power, ps0_input_power; индивидуальные CPU cores не загружаются';

create function check_metric_source_compatibility() returns trigger
language plpgsql
set search_path = project, pg_temp
as $$
declare
    actual_kind text;
    required_kind text;
begin
    select kind into actual_kind from project.component where id = NEW.component_id;
    select component_kind into required_kind from project.metric_type where id = NEW.metric_type_id;
    -- Отсутствующие ссылки и NULL проверяются FK и NOT NULL.
    if actual_kind is not null and required_kind is not null and actual_kind <> required_kind then
        raise exception 'Компонент % имеет вид %, метрика % требует %',
            NEW.component_id, actual_kind, NEW.metric_type_id, required_kind
            using errcode = '23514', constraint = 'metric_source_component_kind_check';
    end if;
    return NEW;
end;
$$;
comment on function check_metric_source_compatibility() is 'Проверка component.kind = metric_type.component_kind при INSERT/UPDATE источника';
create trigger metric_source_compatibility
before insert or update of component_id, metric_type_id on metric_source
for each row execute function check_metric_source_compatibility();

-- Классификация сохраняется для всех связанных источников и измерений.
create function protect_component_classification() returns trigger
language plpgsql
set search_path = project, pg_temp
as $$
begin
    if (to_jsonb(NEW) ->> TG_ARGV[0]) is distinct from (to_jsonb(OLD) ->> TG_ARGV[0]) then
        raise exception 'Классификация %.% неизменяема; создайте новую сущность', TG_TABLE_NAME, TG_ARGV[0]
            using errcode = '23514', constraint = 'component_classification_immutable';
    end if;
    return NEW;
end;
$$;
comment on function protect_component_classification() is 'Защищает классификацию компонентов и типов метрик от изменения, нарушающего совместимость и смысл истории';
create trigger component_kind_immutable before update of kind on component
for each row execute function protect_component_classification('kind');
create trigger metric_type_kind_immutable before update of component_kind on metric_type
for each row execute function protect_component_classification('component_kind');

create table telemetry (
    id bigint generated always as identity primary key,
    metric_source_id int not null references metric_source(id) on delete restrict,
    ts timestamptz not null,
    value numeric not null,
    constraint telemetry_source_ts_key unique (metric_source_id, ts)
);
comment on table telemetry is 'Рабочая телеметрия F18 после preprocessing, long-format: источник, время, значение';
comment on column telemetry.ts is 'Момент измерения; ETL явно определяет часовой пояс исходного времени';
comment on column telemetry.value is 'Numeric без физических границ; для CPU_MEAN_TEMP хранится результат preprocessing';

create table event (
    id int generated always as identity primary key,
    node_id int not null references node(id) on delete restrict,
    occurred_at timestamptz not null,
    severity text not null,
    event_type text not null,
    message text not null,
    constraint event_severity_check check (severity in ('info', 'warning', 'alarm', 'critical')),
    constraint event_type_check check (event_type in ('hardware', 'thermal', 'power', 'communication', 'maintenance', 'other'))
);
comment on table event is 'Предметный эксплуатационный факт на узле; не ML result и не anomaly_score';

create table engineer (
    id int generated always as identity primary key,
    tab_no text not null unique,
    full_name text not null,
    email text
);
comment on table engineer is 'Исполнитель обслуживания; естественный ключ — табельный номер';

create table maintenance (
    id int generated always as identity primary key,
    node_id int not null references node(id) on delete restrict,
    engineer_id int not null references engineer(id) on delete restrict,
    performed_at timestamptz not null,
    maintenance_type text not null,
    notes text,
    constraint maintenance_type_check check (maintenance_type in ('inspection', 'preventive', 'repair', 'replacement'))
);
comment on table maintenance is 'Факт обслуживания всего узла одним ответственным инженером (node-level V1)';
comment on column maintenance.performed_at is 'Момент выполнения работ, не расписание будущего обслуживания';

create table resource (
    id int generated always as identity primary key,
    code text not null unique,
    name text not null,
    resource_type text not null,
    description text,
    constraint resource_type_check check (resource_type in ('spare_part', 'consumable', 'replacement_component'))
);
comment on table resource is 'Справочник запчастей, расходников и компонентов для замены';

create table maintenance_resource (
    maintenance_id int not null references maintenance(id) on delete restrict,
    resource_id int not null references resource(id) on delete restrict,
    qty numeric not null,
    primary key (maintenance_id, resource_id),
    constraint maintenance_resource_qty_check check (qty > 0)
);
comment on table maintenance_resource is 'Связь M:N обслуживания и ресурсов с количеством использованного ресурса';
comment on column maintenance_resource.qty is 'Положительное количество в учётной единице ресурса; допускается дробное';

create table document (
    id int generated always as identity primary key,
    node_id int not null references node(id) on delete restrict,
    document_type text not null,
    title text not null,
    url text not null unique,
    added_at timestamptz not null default now(),
    constraint document_type_check check (document_type in ('manual', 'datasheet', 'maintenance_report', 'other'))
);
comment on table document is 'Ссылка на документацию узла; содержимое хранится вне базы';
comment on column document.added_at is 'Время регистрации ссылки в БД, не дата создания документа';


-- Canonical metric dictionary (required reference data).
insert into metric_type (code, component_kind, quantity, unit, description) values
    ('PSU_INPUT_POWER', 'psu', 'power', 'W', 'Входная мощность одного PSU'),
    ('CPU_POWER', 'cpu', 'power', 'W', 'Мощность одного CPU'),
    ('CPU_MEAN_TEMP', 'cpu', 'temperature', 'degC', 'Среднее валидных core temperatures одного CPU до ingestion'),
    ('GPU_POWER', 'gpu', 'power', 'W', 'Мощность одного GPU'),
    ('GPU_CORE_TEMP', 'gpu', 'temperature', 'degC', 'Температура одного GPU');
