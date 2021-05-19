# GeoSpark JDBC driver
This repository is forked from [Spark-Jdbc](https://github.com/takezoe/spark-jdbc) developed by  Takezoe. [Apache Sedona](https://sedona.apache.org/) is integrated to support geospatial data processing and analysis. I intend to include this JDBC inside [Ontop](https://ontop-vkg.org/) to expose distributed geospatial datasets as RDF.   
## Build

Please use the following command to build a fat Jar 

````
./gradlew clean shadowJar
````

## Documentation

GeoSpark JDBC driver is a read-only JDBC driver that uses Spark SQL as database tables.


First, create a configuration file like this:

```json
{
  "tables": [
    {
      "name": "ais",
      "path": "SPARK_HOME/examples/src/main/resources/ais.csv",
      "format": "csv",
      "options": {
        "header": "true",
        "inferSchema": "true",
        "delimiter": ";"
      }
    },
    {
      "name": "ship",
      "path": "SPARK_HOME/examples/src/main/resources/ship.csv",
      "format": "csv",
      "options": {
        "header": "true",
        "inferSchema": "true",
        "delimiter": ";"
      }
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

Supported GeoSPARK functions can be found [here](https://sedona.apache.org/api/sql/Function/)