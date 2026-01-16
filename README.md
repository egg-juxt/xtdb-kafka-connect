XTDB Kafka Sink Connector

This plugin for Kafka Connect allows for the ingestion of data into XTDB.

It provides the following components:

**XtdbSinkConnector**
: A Sink Connector that inserts record data into XTDB tables.

**SchemaDrivenXtdbEncoder**
: A Kafka Connect Transformation that you can use to transform your record data to appropriate XTDB data types, which are inferred from the record schema.

## User Guide

### Simple insert

In its simplest form, you can start an XTDB connector in Kafka Connect with the following configuration:

```json
{
  "tasks.max": "1",
  "topics": "readings",
  "connector.class": "xtdb.kafka.connect.XtdbSinkConnector",
  "connection.url": "jdbc:xtdb://xtdb_host:5432/xtdb",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

This allows inserting plain JSON records with no corresponding schema.

Send the following record to your Kafka topic to have it inserted into XTDB:

```json
{
  "_id": 1,
  "_valid_from": "2025-08-25T11:15:00Z",
  "metric": "Humidity",
  "measurement": 0.8
}
```

Notice that we are setting the XTDB identifier field `_id` and the special temporal field `_valid_from`. You should be able to see your record by querying XTDB:

```sql
SELECT *, _valid_from FROM readings FOR VALID_TIME ALL
```

### Adapting the shape of your records

Sometimes your record field names and types don't match XTDB's special columns. Let's say your records have the following shape:

```json
{
  "reading_id": 1,
  "t": 1756120500000,
  "metric": "Humidity",
  "measurement": 0.8
}
```

Notice that the record identifier is in the `reading_id` field, and we want to store `t` as XTDB's `_valid_from`. In addition, `t` is expressed in UNIX time in milliseconds.

One way to fix this is by applying transforms. For example, we can use built-in Kafka Connect transforms, extending the connector configuration as follows:

```json
{
  "transforms": "timeToString, xtdbRenames",
  "transforms.timeToString.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
  "transforms.timeToString.field": "t",
  "transforms.timeToString.target.type": "Timestamp",
  "transforms.xtdbRenames.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
  "transforms.xtdbRenames.renames": "reading_id:_id, t:_valid_from"
}
```

> **Note**
>
> Remember that you can check the types of XTDB columns by running a query such as:
>
> ```sql
> SELECT column_name, data_type
> FROM information_schema.columns
> WHERE table_name = 'readings'
> ```

### Reshaping based on a schema

Let's say our readings are a bit more complicated:

```json
{
  "_id": 1,
  "_valid_from": "2025-08-25T11:15:00Z",
  "metric": "Humidity",
  "measurement": 0.8,
  "span": "PT30S"
}
```

We have an additional `span` field that we'd want to store using the `interval` type in XTDB. Furthermore, we want to store `measurement` as a 32-bit float, rather than a 64-bit float (commonly known as "double").

Configure your connector to apply the XTDB encoder transform:

```json
{
  "transforms": "xtdbEncode",
  "transforms.xtdbEncode.type": "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"
}
```

Depending on the kind of schema you want to use (JSON Schema / Avro Schema / Connect Schema) refer to one of the following sections:

#### Using JSON Schema

Define your schema as follows:

```json
{
  "type": "object",
  "properties": {
    "_id": { "type": "integer" },
    "_valid_from": { "type": "string" },
    "metric": { "type": "string" },
    "measurement": {
      "type": "number",
      "connect.type": "float32"
    },
    "span": {
      "type": "string",
      "connect.parameters": { "xtdb.type": "interval" }
    }
  }
}
```

We are using a standard Kafka Connect type for defining `measurement` as a `float32`. For `span`, we use the custom parameter `xtdb.type` to define the XTDB-specific data type `interval`.

For the above to work, you will need to configure your connector's value converter as follows:

```json
{
  "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter.schemas.enable": "true"
}
```

#### Using Avro Schema

Define your schema as follows:

```json
{
  "type": "record",
  "name": "Reading",
  "fields": [
    { "name": "_id", "type": "long" },
    { "name": "_valid_from", "type": "string" },
    { "name": "metric", "type": "string" },
    { "name": "measurement", "type": "float" },
    {
      "name": "span",
      "type": {
        "type": "string",
        "connect.parameters": {
          "xtdb.type": "interval"
        }
      }
    }
  ]
}
```

We are using a standard Avro type to define `measurement` as a `float`. For `span`, we use the custom parameter `xtdb.type` to define the XTDB-specific data type `interval`.

For the above to work, you will need to configure your connector's value converter as follows:

```json
{
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schemas.enable": "true",
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter.connect.meta.data": "true"
}
```

#### Using an in-band Connect Schema

Kafka Connect offers the possibility of sending your data payload and its corresponding schema together in your record value.

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      { "field": "_id", "type": "int64", "optional": false },
      { "field": "_valid_from", "type": "string", "optional": false },
      { "field": "metric", "type": "string", "optional": false },
      { "field": "measurement", "type": "float", "optional": false },
      {
        "field": "span",
        "type": "string",
        "parameters": {
          "xtdb.type": "interval"
        },
        "optional": false
      }
    ]
  },
  "payload": {
    "_id": 1,
    "_valid_from": "2025-08-25T11:15:00Z",
    "metric": "Humidity",
    "measurement": 0.8,
    "span": "PT30S"
  }
}
```

We are using a standard Connect type to define `measurement` as a `float`. For `span`, we use the custom parameter `xtdb.type` to define the XTDB-specific data type `interval`.

For the above to work, you will need to configure your connector's value converter as follows:

```json
{
  "value.converter": "org.apache.kafka.connect.json.JsonConverter"
}
```

## Fault tolerance

The XTDB Kafka Connector will handle errors depending on their kind:

* Transient errors in the XTDB connection (such as connection closed): will be retried indefinitely.
* Errors in record data found by the connector: records will be retried individually (see below).
* Other errors while executing SQL statements: will be retried up to `max.retries`. Then, records will be retried individually (see below).
* Other unexpected errors: the connector will stop.

When records are retried for individual ingestion, any errors found will be reported to the Kafka Connect framework. It will move errant records to a dead-letter queue if configured to do so. Example configuration:

```json
{
  "errors.tolerance": "all",
  "errors.log.enable": true,
  "errors.log.include.messages": true,
  "errors.deadletterqueue.topic.name": "dlq",
  "errors.deadletterqueue.topic.replication.factor": 1,
  "errors.deadletterqueue.context.headers.enable": true
}
```

## Reference

### Version compatibility notes

XTDB version 2.1 or higher is recommended.

This connector integrates through the Kafka Connect API version 3.9.1.

### XTDB Sink Connector

Ingests Kafka records into XTDB tables.

Configuration options:

`connection.url`
: **Required**. Must point to XTDB's PostgreSQL-compatible port. <br> Example: `"jdbc:xtdb://my_host:5432/xtdb"`

`insert.mode`
: The insertion mode to use. Supported modes are: <br> `insert`: (default) <br> `patch`: An upsert by id. Fields with a NULL value are kept unchanged, rather than set to NULL.

`id.mode`
: Where to get the `_id` from. One of: <br> `record_value`: (default) The `_id` field of the record value will be used. <br> `record_key`: (required if you want [tombstones](https://kafka.apache.org/documentation/#design_compactionbasics) to delete records) The record key must be either a Struct or a primitive value. If the key is a struct, its `_id` field will be used.

`table.name.format`
: A format string for the destination table name, which may contain `${topic}` as a placeholder for the originating topic name.

`table.name.map`
: Mapping between topics and destination table names, formatted using CSV as shown: `topic1:table1,topic2:table2`. This option has precedence over `table.name.format`.

`max.retries`
: The maximum number of times to retry on non-transient errors before failing the task. Well-known transient errors are retried indefinitely.

`retry.backoff.ms`
: The time in milliseconds to wait following an error before a retry attempt is made.
```

### SchemaDrivenXtdbEncoder

Transforms each field of a record value into the appropriate XTDB type, based on the record value schema.

Configure by defining a transform of `type: xtdb.kafka.connect.SchemaDrivenXtdbEncoder`.

The XTDB type for each field is derived from:

*   Its type in the schema
*   An optional custom parameter `xtdb.type`. How this custom parameter is defined depends on the schema type. See the User Guide above.

If defined, `xtdb.type` has preference.

Supported `xtdb.type` values are:

*   `interval`
*   `timestamptz`
*   and any fully-qualified Transit type supported by XTDB

> **Note**
>
> `SchemaDrivenXtdbEncoder` transforms a Struct record into a value of type Map and dismisses the record value schema, as the value no longer complies with it.

### Logging

The XTDB Kafka Connector uses SLF4J for logging. All its logging is confined to a parent logger `xtdb.kafka.connect`. DEBUG-level logging can provide useful troubleshooting info.

> **Warning**
>
> The TRACE level can be enabled for further detail, but beware that it will output record data to the logs.

For XTDB logging configuration, refer to the XTDB documentation on [troubleshooting](https://docs.xtdb.com/ops/troubleshooting.html).

## For developers

### Publishing

This project publishes 2 packages to Github Releases:

*   An Uber-JAR that can be included by Kafka Connect by using the `plugin.path` option. (Gradle task: `:shadowJar`)
*   A ZIP package that can be installed as a Confluent Component. (Gradle task: `:archive`)

You will need a Github token with release-publishing permissions. For convenience, store it in your `~/.gradle/gradle.properties` file:

```
github.token=...
```

To publish a new version:

1.  Increment the `version` property inside `gradle.properties`.
2.  Push `main` to Github. This version will be tagged with the version.
3.  Run Gradle tasks for the 2 packages above: `:shadowJar` and `:archive`.
4.  Run the Gradle task for publishing: `:githubRelease`.

If something goes wrong, you can delete the release in Github and publish again.

### Tests

There is a dedicated subproject for `integration-tests`. This could allow using a Java version different from the one used for compiling the main Connector project. (This was needed at some point).

All tests can be run with the Gradle task `test`.

### Clojure REPL

You can run Gradle tasks `:clojureRepl` and `:integration-tests:clojureRepl`, depending on your needs.
