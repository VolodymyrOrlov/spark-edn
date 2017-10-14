# Extensible Data Notation Data Source for Apache Spark

This library provides support for reading an 
[Extensible Data Notation](https://github.com/edn-format/edn)-formatted files

## Quick Start

Install and start a spark shell:

```
$ sbt install
$ spark-shell --packages com.levelmoney:spark-edn:0.0.1-SNAPSHOT
```

You can register EDN file table and run SQL queries against it, or query with the Spark SQL DSL.
The schema will be inferred by sampling items in the file, or you can provide your own schema.

```
val path = "src/test/resources/users.edn"
// Read files from a given location.
val users = sqlContext.read.edn(path)

// Query with SQL.
users.registerTempTable("users")
val data = sqlContext.sql("select * from users where `:user_name` like 'tc_%'")

```

## Schemas

If no schema is provided, the schema will be inferred from a sample of items in the file.
If items with multiple schemas are stored you may choose to provide the schema:


```
val schema = StructType {
    Seq(
      StructField("user_id", LongType),
      StructField("username", StringType)
    )
  }
  
spark.read.schema(schema).edn(path)
```

## Configuration

| Option | Description |
| --- | --- |
| `path` | A path to the collection of EDN files |