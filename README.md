# Spark JDBC driver

## Build

I haven't gotten chance to publish it into Maven Central. For now, please use the following command to build a fat Jar 

````
./gradlew clean shadowJar
````

## Documentation

Spark JDBC driver is a read-only JDBC driver that uses Spark SQL as database tables.

First, need to create a configuration file like this:

```json
{
  "tables": [
    {
      "name": "people",
      "path": "SPARK_HOME/examples/src/main/resources/people.csv",
      "format": "csv",
      "options": {
        "header": "true",
        "inferSchema": "true",
        "delimiter": ";"
      }
    },
    {
      "name": "users",
      "path": "SPARK_HOME/examples/src/main/resources/users.orc",
      "format": "orc"
    }
  ]
}
```

Then, you can get a JDBC connection with URL like below:

```
# Local mode
jdbc:spark:local?config=<path_to_file>

# Use a cluster
jdbc:spark://localhost:7077?config=<path_to_file>
```
