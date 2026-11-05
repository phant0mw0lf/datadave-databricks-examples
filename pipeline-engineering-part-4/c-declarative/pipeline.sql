-- Variant C - the same seven tables as a Lakeflow Declarative Pipeline.
--
-- The platform owns the dependency graph: it is derived from what each
-- statement reads. The scd flows are what pipetree's AUTO CDC translation
-- renders from pipeline.yaml (`pipetree.declarative.autocdc`); the rest is
-- what pipetree does not translate and the post says so: Auto Loader landing
-- tables for the sources, materialized views for the `replace` tables, and a
-- snapshot flow for the gold dimension.
--
-- ${catalog} and ${volume_path} come from the pipeline's configuration
-- (databricks.yml). Tables land in ldp_bronze / ldp_silver / ldp_gold so they
-- never collide with the package's bronze / silver / gold in the same catalog.

-- ---- landing: one streaming table per source file ---------------------------
-- pipetree reads the CSV directly in the merge; a flow needs a streaming
-- source, so each file first lands in its own append-only table. Auto Loader
-- watches a folder, not a file ("Input path is not a directory"), so each
-- source gets a folder under landing/. The file time stands in as the
-- sequence for the two sources that have none.

CREATE OR REFRESH STREAMING TABLE ${catalog}.ldp_bronze.customer_raw
AS SELECT *, _metadata.file_modification_time AS _loaded_at
FROM STREAM read_files('${volume_path}/landing/customer/', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE ${catalog}.ldp_bronze.orders_raw
AS SELECT *, _metadata.file_modification_time AS _loaded_at
FROM STREAM read_files('${volume_path}/landing/orders/', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE ${catalog}.ldp_bronze.employee_raw
AS SELECT *, _metadata.file_modification_time AS _loaded_at
FROM STREAM read_files('${volume_path}/landing/employee/', format => 'csv', header => true);

-- ---- bronze: the scd flows, as pipetree renders them --------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.ldp_bronze.customer;

CREATE FLOW bronze_customer_flow AS AUTO CDC INTO
  ${catalog}.ldp_bronze.customer
FROM STREAM ${catalog}.ldp_bronze.customer_raw
KEYS
  (accountid)
APPLY AS DELETE WHEN
  isdelete = true
SEQUENCE BY
  versionnumber
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (modifiedon, _loaded_at);

-- scd1 without a sequence in pipeline.yaml: AUTO CDC wants one, the file time
-- is the only candidate.

CREATE OR REFRESH STREAMING TABLE ${catalog}.ldp_bronze.orders;

CREATE FLOW bronze_orders_flow AS AUTO CDC INTO
  ${catalog}.ldp_bronze.orders
FROM STREAM ${catalog}.ldp_bronze.orders_raw
KEYS
  (order_id)
SEQUENCE BY
  _loaded_at
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE ${catalog}.ldp_bronze.employee;

CREATE FLOW bronze_employee_flow AS AUTO CDC INTO
  ${catalog}.ldp_bronze.employee
FROM STREAM ${catalog}.ldp_bronze.employee_raw
KEYS
  (employee_id)
SEQUENCE BY
  _loaded_at
STORED AS SCD TYPE 1;

-- ---- silver: `replace` tables are plain materialized views -------------------
-- The logic files of pipeline.yaml, with one edit each: the scd2 flag is
-- `__END_AT IS NULL` here, `_is_current = true` in pipetree.

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.ldp_silver.customer_enriched
AS SELECT accountid, name
FROM ${catalog}.ldp_bronze.customer
WHERE __END_AT IS NULL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.ldp_silver.orders
AS SELECT order_id, customer_accountid, amount, order_date
FROM ${catalog}.ldp_bronze.orders;

-- ---- gold: scd2 on top of a recomputed view = a snapshot flow -----------------
-- silver.customer_enriched is recomputed each update, not appended to, so it
-- cannot feed `FROM STREAM`. AUTO CDC FROM SNAPSHOT diffs the whole view per
-- update instead. pipeline.yaml's `unknown_member: true` (the -1 row) has no
-- counterpart here; fact_sales coalesces to -1 itself.

CREATE OR REFRESH STREAMING TABLE ${catalog}.ldp_gold.dim_customer;

CREATE FLOW gold_dim_customer_flow AS AUTO CDC INTO
  ${catalog}.ldp_gold.dim_customer
FROM SNAPSHOT (
  SELECT accountid, name FROM ${catalog}.ldp_silver.customer_enriched
)
KEYS
  (accountid)
STORED AS SCD TYPE 2;

-- gold.fact_sales is Python in pipeline.yaml (notebooks/gold/fact_sales.py)
-- and stays Python here: fact_sales.py in this folder.
