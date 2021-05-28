# GeoSpark JDBC driver
This repository is forked from [Spark-Jdbc](https://github.com/takezoe/spark-jdbc) developed by  Takezoe. [Apache Sedona](https://sedona.apache.org/) is integrated to support geospatial data processing and analysis. The views can be now customized by adding the columns name, columns type and eventually executing a SQL query.
The tool (Sedona-Ontop branch) is used within [Ontop](https://ontop-vkg.org/) to expose distributed geospatial datasets as RDF.   
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
	"tables": [{
			"name": "ais",
			"path": "/home/huy/ais.csv", 
			"sql": "SELECT id, mmsi, timestamp, ST_Point(longitude, latitude) as geom FROM ais",
			"columnName": ["id","timestamp","mmsi","latitude","longitude"],
      "columnType": ["int", "string", "int", "double", "double" ],
			"format": "csv",
			"options": {
				"header": "true",
				"inferSchema": "false",
				"delimiter": ","
			}
		},
		{
			"name": "ship",
			"path": "/home/huy/ship.csv",
			"sql": "",
			"columnName": ["mmsi","name","shiptype","cargotype","width","length"],
      "columnType": ["int", "string", "string", "string", "string", "string" ],
			"format": "csv",
			"options": {
				"header": "true",
				"inferSchema": "false",
				"delimiter": ","
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
