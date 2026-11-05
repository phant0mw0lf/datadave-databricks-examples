# The faults of part 2

- **A throttled source** (`bronze.orders`, 429 twice, then it lets go) and **a malformed
  export** (`bronze.customer`) for variants A and B: injected by pipetree's
  `FaultInjectingAdapter`, see the folder README. No file changes.
- **The malformed export, for real**, for variant C: `customer.csv` here is a second
  delivery whose rows do not parse (a `;` delimited line, an unterminated quote). Copy it
  into the landing folder and run the pipeline:

  ```bash
  databricks fs cp faults/customer.csv dbfs:/Volumes/<catalog>/pipetree/files/landing/customer/customer_2.csv
  (cd c-declarative && databricks bundle run pipetree_c_declarative -t dev)
  ```

  Remove `customer_2.csv` afterwards. A throttled source has no counterpart in a
  declarative pipeline: there is no source call to throttle, the platform reads
  the landing folder.
