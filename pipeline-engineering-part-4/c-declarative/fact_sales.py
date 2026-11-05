# Variant C - gold.fact_sales as a materialized view in Python.
#
# The same join as pipeline.yaml's notebooks/gold/fact_sales.py. There, the
# file assigns its DataFrame to `result` and pipetree writes it; here it is a
# decorated function and the pipeline writes it. `spark.table` becomes
# `spark.read.table` and the dimension's "current row" flag is `__END_AT`.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")  # noqa: F821 - `spark` is a pipeline global


@dp.materialized_view(name=f"{catalog}.ldp_gold.fact_sales")
def fact_sales():
    orders = spark.read.table(f"{catalog}.ldp_silver.orders")  # noqa: F821
    customer = spark.read.table(f"{catalog}.ldp_gold.dim_customer").where(
        "__END_AT IS NULL"
    )
    # a missing dim_customer match resolves to -1 instead of a NULL foreign key
    return orders.join(
        customer, orders.customer_accountid == customer.accountid, "left"
    ).select(
        orders.order_id,
        orders.amount,
        orders.order_date,
        F.coalesce(customer.accountid, F.lit(-1)).alias("customer_accountid"),
    )
