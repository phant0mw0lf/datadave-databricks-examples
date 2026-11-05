# Part 4 - the same seven tables, three ways, on Databricks

Companion code for [Metadata-driven Pipeline Orchestration, part 4](https://datadave.dev/tags/pipeline-engineering/).
The pipeline is the example from parts 1 and 2 of the series: three CSV sources,
bronze, silver, a gold dimension and a fact (`pipeline/pipeline.yaml`, verbatim from
[pipetree](https://github.com/phant0mw0lf/pipetree)'s `examples/`).

| variant | folder | who owns the dependency tree | what runs per table |
| --- | --- | --- | --- |
| A - the package | `a-package/` | pipetree, inside one notebook task | pipetree's merge |
| B - a job with tasks | `b-job-tasks/` | the Databricks job (`depends_on`, one task per table) | pipetree's merge, for that one table (`select=`) |
| C - Declarative Pipeline | `c-declarative/` | Lakeflow, derived from the SQL | AUTO CDC flows, materialized views |

A and B share `shared/run_pipeline.py` and differ only in the job definition.
`b-job-tasks/databricks.yml` is generated (`generate.py`) from `pipeline.yaml` - the
post is about what that file looks like at 7, 50 and 170 tables. C is written by hand;
the three scd flows in `pipeline.sql` are what pipetree's AUTO CDC translation renders,
the rest is what it does not translate.

## Run it

Needs a Unity Catalog catalog, the Volume `<catalog>.pipetree.files` with the CSVs
under `data/`, a `databricks` CLI profile, and nothing else. pipetree comes from PyPI.

```bash
databricks fs mkdir dbfs:/Volumes/<catalog>/pipetree/files/data
for f in customer orders employee; do
  databricks fs cp pipeline/data/$f.csv dbfs:/Volumes/<catalog>/pipetree/files/data/$f.csv      # A, B: the file
  databricks fs mkdir dbfs:/Volumes/<catalog>/pipetree/files/landing/$f
  databricks fs cp pipeline/data/$f.csv dbfs:/Volumes/<catalog>/pipetree/files/landing/$f/$f.csv  # C: a folder per source
done

export BUNDLE_VAR_catalog=<catalog>
export BUNDLE_VAR_volume_path=/Volumes/<catalog>/pipetree/files

(cd a-package     && databricks bundle deploy -t dev && databricks bundle run pipetree_a_package -t dev)
(cd b-job-tasks   && databricks bundle deploy -t dev && databricks bundle run pipetree_b_job_tasks -t dev)
(cd c-declarative && databricks bundle deploy -t dev && databricks bundle run pipetree_c_declarative -t dev)
```

A and B write `bronze.*`, `silver.*`, `gold.*`; C writes `ldp_bronze.*`, `ldp_silver.*`,
`ldp_gold.*` in the same catalog.

## The failure scenario of part 2

A throttled source (429, twice) and a malformed export. For A and B the faults are
injected by pipetree's `FaultInjectingAdapter`, passed as a job parameter:

```bash
FAULTS='{"bronze.orders": {"kind": "throttle", "count": 2, "message": "429 Too Many Requests"},
         "bronze.customer": {"kind": "fail", "message": "malformed export file"}}'
databricks bundle run pipetree_a_package -t dev --params faults="$FAULTS"
databricks bundle run pipetree_b_job_tasks -t dev --params faults="$FAULTS"
```

For C the malformed export is a real one (`faults/customer.csv` copied over the Volume
file); a throttled source has no counterpart in a declarative pipeline - the post
explains why.
