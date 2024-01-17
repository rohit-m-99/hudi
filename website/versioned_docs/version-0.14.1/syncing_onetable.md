---
title: OneTable
keywords: [onetable, hudi, delta-lake, iceberg, sync]
---

Hudi (tables created from 0.14.0 onwards) supports syncing to [OneTable](https://onetable.dev/), providing users with the option to interoperate with other table formats like Delta Lake and Apache Iceberg.

## Interoperating with OneTable

If you have tables in one of the supported formats (Delta/Iceberg), you can use OneTable to translate the existing metadata to read as a Hudi table and vice versa.

### Installation

You can work with OneTable by either building the jar from the [source](https://github.com/onetable-io/onetable) or by downloading from [GitHub packages](https://github.com/onetable-io/onetable/packages/1986830).

:::tip Note
If you're using one of the JVM languages to work with Hudi/Delta/Iceberg, you can directly use OneTable as a [dependency](https://github.com/onetable-io/onetable/packages/1986830) in your project.
This is highlighted in this [demo](https://onetable.dev/docs/demo/docker).
:::

### Syncing to OneTable

Once you have the jar, you can simply run it against a Hudi/Delta/Iceberg table to add target table format metadata to the table.
Below is an example configuration to translate a Hudi table to Delta & Iceberg table.

```shell md title="my_config.yaml"
sourceFormat: HUDI
targetFormats:
  - DELTA
  - ICEBERG
datasets:
  -
    tableBasePath: path/to/hudi/table
    tableName: tableName
    partitionSpec: partition_field_name:VALUE
```

```shell md title="shell"
java -jar path/to/bundled-onetable.jar --datasetConfig path/to/my_config.yaml
```

### Hudi Streamer Extensions
If you want to use OneTable with Hudi Streamer to sync each commit into other table formats, you have to

1. Add the [extensions jar](https://github.com/onetable-io/onetable/tree/main/hudi-support/extensions) `hudi-extensions-0.1.0-SNAPSHOT-bundled.jar` to your class path.
2. Add `io.onetable.hudi.sync.OneTableSyncTool` to your list of sync classes
3. Set the following configurations based on your preferences:

   ```
   hoodie.onetable.formats: "ICEBERG,DELTA" 
   hoodie.onetable.target.metadata.retention.hr: 168
   ```

For more examples, you can refer to the [OneTable docs](https://onetable.dev/docs/how-to).