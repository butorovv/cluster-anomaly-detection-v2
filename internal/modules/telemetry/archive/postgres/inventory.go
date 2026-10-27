package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/jackc/pgx/v5"
)

type featureSource struct {
	kind   string
	index  int16
	metric string
}

// Same order as the existing F18 domain; names/source_key are not lookup keys.
var featureSources = [domain.FeatureCount]featureSource{
	{"psu", 0, "PSU_INPUT_POWER"}, {"psu", 1, "PSU_INPUT_POWER"},
	{"cpu", 0, "CPU_POWER"}, {"cpu", 1, "CPU_POWER"},
	{"gpu", 0, "GPU_POWER"}, {"gpu", 1, "GPU_POWER"}, {"gpu", 2, "GPU_POWER"},
	{"gpu", 3, "GPU_POWER"}, {"gpu", 4, "GPU_POWER"}, {"gpu", 5, "GPU_POWER"},
	{"cpu", 0, "CPU_MEAN_TEMP"}, {"cpu", 1, "CPU_MEAN_TEMP"},
	{"gpu", 0, "GPU_CORE_TEMP"}, {"gpu", 1, "GPU_CORE_TEMP"}, {"gpu", 2, "GPU_CORE_TEMP"},
	{"gpu", 3, "GPU_CORE_TEMP"}, {"gpu", 4, "GPU_CORE_TEMP"}, {"gpu", 5, "GPU_CORE_TEMP"},
}

// Bootstrap validates and creates only missing reference rows. Existing labels,
// source_key spellings and known hardware models are preserved. New models are
// NULL: logical component positions are not claims about physical Summit models.
func Bootstrap(ctx context.Context, db DB) error {
	if db == nil {
		return fmt.Errorf("%w: database required", ErrInvalidRecord)
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return classify(err)
	}
	defer rollback(tx)
	if err = bootstrap(ctx, tx); err != nil {
		return classify(err)
	}
	if _, err = loadSources(ctx, tx); err != nil {
		return classify(err)
	}
	return classify(tx.Commit(ctx))
}

func bootstrap(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(726249201, 104)`); err != nil {
		return err
	}
	// Also exclude non-cooperating inventory edits during validation and loading.
	if _, err := tx.Exec(ctx, `LOCK TABLE project.node_group,project.node,project.component,project.metric_type,project.metric_source IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return err
	}
	var count int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM project.metric_type WHERE
 (code,component_kind,quantity,unit) IN (
 ('PSU_INPUT_POWER','psu','power','W'),('CPU_POWER','cpu','power','W'),
 ('GPU_POWER','gpu','power','W'),('CPU_MEAN_TEMP','cpu','temperature','degC'),
 ('GPU_CORE_TEMP','gpu','temperature','degC'))`).Scan(&count)
	if err != nil {
		return err
	}
	if count != 5 {
		return fmt.Errorf("%w: five canonical metric definitions required", ErrSchema)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO project.node_group(code,name,group_type) VALUES ('SUMMIT','SUMMIT','cluster') ON CONFLICT(code) DO NOTHING`); err != nil {
		return err
	}
	var rootID int32
	if err = tx.QueryRow(ctx, `SELECT id FROM project.node_group WHERE code='SUMMIT' AND group_type='cluster' AND parent_id IS NULL`).Scan(&rootID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: incompatible SUMMIT root", ErrSchema)
		}
		return err
	}
	var groups, hosts, nodeGroups []string
	var indices []int16
	for _, g := range topology.CanonicalGroups() {
		groups = append(groups, g.Code)
		for _, n := range g.Nodes {
			hosts = append(hosts, n.Hostname)
			nodeGroups = append(nodeGroups, g.Code)
			indices = append(indices, int16(n.NodeIndex))
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO project.node_group(parent_id,code,name,group_type)
 SELECT $1,code,code,'topological' FROM unnest($2::text[]) AS x(code) ON CONFLICT(code) DO NOTHING`, rootID, groups); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM project.node_group WHERE code=ANY($1::text[]) AND parent_id=$2 AND group_type='topological'`, groups, rootID).Scan(&count); err != nil {
		return err
	}
	if count != len(groups) {
		return fmt.Errorf("%w: incompatible canonical group parent/type", ErrSchema)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO project.node(node_group_id,hostname,node_index)
 SELECT g.id,x.host,x.idx FROM unnest($1::text[],$2::text[],$3::smallint[]) AS x(host,grp,idx)
 JOIN project.node_group g ON g.code=x.grp ON CONFLICT(hostname) DO NOTHING`, hosts, nodeGroups, indices); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM unnest($1::text[],$2::text[],$3::smallint[]) AS x(host,grp,idx)
 JOIN project.node n ON n.hostname=x.host AND n.node_index=x.idx
 JOIN project.node_group g ON g.id=n.node_group_id AND g.code=x.grp`, hosts, nodeGroups, indices).Scan(&count); err != nil {
		return err
	}
	if count != len(hosts) {
		return fmt.Errorf("%w: canonical node assigned to a different group/index", ErrSchema)
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM project.node n JOIN project.node_group g ON n.node_group_id=g.id WHERE g.code=ANY($1::text[])`, groups).Scan(&count); err != nil {
		return err
	}
	if count != len(hosts) {
		return fmt.Errorf("%w: unexpected nodes in canonical groups", ErrSchema)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO project.component(node_id,kind,local_index)
 SELECT n.id,k.kind,i.idx FROM project.node n CROSS JOIN (VALUES ('cpu',1),('gpu',5),('psu',1)) AS k(kind,last_idx)
 CROSS JOIN LATERAL generate_series(0,k.last_idx) AS i(idx)
 WHERE n.hostname=ANY($1::text[]) ON CONFLICT(node_id,kind,local_index) DO NOTHING`, hosts); err != nil {
		return err
	}
	var kinds, metrics, keys []string
	var locals []int16
	for i, f := range featureSources {
		kinds = append(kinds, f.kind)
		locals = append(locals, f.index)
		metrics = append(metrics, f.metric)
		keys = append(keys, domain.FeatureNames[i])
	}
	_, err = tx.Exec(ctx, `INSERT INTO project.metric_source(component_id,metric_type_id,source_key)
 SELECT c.id,m.id,x.key FROM unnest($1::text[],$2::smallint[],$3::text[],$4::text[]) AS x(kind,idx,metric,key)
 JOIN project.metric_type m ON m.code=x.metric
 JOIN project.component c ON c.kind=x.kind AND c.local_index=x.idx
 JOIN project.node n ON n.id=c.node_id WHERE n.hostname=ANY($5::text[])
 ON CONFLICT(component_id,metric_type_id) DO NOTHING`, kinds, locals, metrics, keys, hosts)
	return err
}

func loadSources(ctx context.Context, tx pgx.Tx) (map[string][domain.FeatureCount]int32, error) {
	sources := make(map[string][domain.FeatureCount]int32, 288)
	seen := make(map[string][domain.FeatureCount]bool, 288)
	var hosts []string
	for _, g := range topology.CanonicalGroups() {
		for _, n := range g.Nodes {
			hosts = append(hosts, n.Hostname)
		}
	}
	rows, err := tx.Query(ctx, `SELECT n.hostname,c.kind,c.local_index,m.code,s.id FROM project.metric_source s
 JOIN project.component c ON c.id=s.component_id JOIN project.node n ON n.id=c.node_id
 JOIN project.metric_type m ON m.id=s.metric_type_id WHERE n.hostname=ANY($1::text[])`, hosts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var host string
		var f featureSource
		var id int32
		if err = rows.Scan(&host, &f.kind, &f.index, &f.metric, &id); err != nil {
			return nil, err
		}
		feature := -1
		for i, want := range featureSources {
			if f == want {
				feature = i
				break
			}
		}
		if feature < 0 {
			return nil, fmt.Errorf("%w: unexpected source definition", ErrSchema)
		}
		ids := sources[host]
		present := seen[host]
		if present[feature] {
			return nil, fmt.Errorf("%w: ambiguous source definition", ErrSchema)
		}
		ids[feature] = id
		present[feature] = true
		seen[host] = present
		sources[host] = ids
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, host := range hosts {
		for _, present := range seen[host] {
			if !present {
				return nil, fmt.Errorf("%w: incomplete sources for %s", ErrSchema, host)
			}
		}
	}
	return sources, nil
}
