package com.zensolution.jdbc.spark.internal;

import com.zensolution.jdbc.spark.internal.config.Table;
import com.zensolution.jdbc.spark.provider.SparkConfProvider;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.sedona.sql.utils.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
//import org.apache.spark.sql.types.*;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Geometry;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkService {
    private ConnectionInfo connectionInfo;
    private SparkSession spark;
    private static Map<TableSchema, StructType> metaData = new ConcurrentHashMap();
    private static Map<String, Integer> cachedTables = new HashMap<String, Integer>();
    private static final Logger LOGGER = Logger.getLogger(SparkService.class.getName());
  

    public SparkService(ConnectionInfo info) throws SQLException {
        this.connectionInfo = info;
        this.spark = buildSparkSession();
    }

    private SparkSession buildSparkSession() throws SQLException {
        final SparkSession.Builder builder = SparkSession.builder().master(connectionInfo.getMaster()).appName("spark-jdbc-driver")
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "8g")
        .config("spark.cores.max", "6");
    
                
                //.config("spark.driver.host", "192.168.16.4") 
                //.config("spark.driver.bindAddress", "192.168.16.4");
        //        .config("spark.driver.host","192.168.16.1");
        // TODO for spark
        Map<String, String> options = connectionInfo.getConfig().getOptions(); //getOptions(connectionInfo.getProperties(), "spark", true);

        Optional<SparkConfProvider> sparkConfProvider = SparkConfProvider.getSparkConfProvider(connectionInfo);
        if ( sparkConfProvider.isPresent() ) {
            options.putAll(sparkConfProvider.get().getSparkConf(connectionInfo));
        }
        options.entrySet().stream()
                .forEach(entry-> builder.config(entry.getKey(), entry.getValue()));
        SparkSession sparkSession = builder.getOrCreate();
        SedonaSQLRegistrator.registerAll(sparkSession);
        sparkSession.sparkContext().setLogLevel("OFF");
        return sparkSession;
    }

    
    
    public Dataset<Row> createEmptyDataset(){
        return spark.emptyDataFrame();
        
    }

   

    public Dataset<Row> executeQuery(String sqlText) throws SQLException, ParseException {
       // prepareTempView(sqlText);
        return spark.sql(sqlText);
    }

     public void prepareTempView() {
        //Map<String, String> options = getOptions(connectionInfo.getProperties(), connectionInfo.getFormat().name(), false);
        LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Preparing all declared tables");
        for (Map.Entry<String, Integer> entry : cachedTables.entrySet()) {
            LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Tables " + entry.getKey() + ": " + entry.getValue().toString());
           
        }
        
        
        for (Table table: connectionInfo.getConfig().getTables()) {
            LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Preparing table " + table.getName());
          
        
        
                //SupportedFormat format = tableConfig.get().getFormat();
                
                if(cachedTables.getOrDefault(table.getName(), 0) == 0)
                {
                LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Loading table metadata " + table);
                List<String> columnName =  table.getColumnName();
                List<String> columnType =  table.getColumnType();
                List<StructField> listOfStructField = new ArrayList<>();
                CatalystSqlParser parser = new CatalystSqlParser();
                for (int i = 0; i < columnName.size(); i++) {
                    listOfStructField.add(DataTypes.createStructField(columnName.get(i), 
                            parser.parseDataType(columnType.get(i)), true));
                    
                 }
                StructType schema=DataTypes.createStructType(listOfStructField);
           
                String tablePath = table.getPath(); //format.getSparkPath(tableConfig.get().getPath(), connectionInfo.isPartitionDiscovery(), table);
                LOGGER.log(Level.INFO, "+++++++ GeoSparkDriver: Loading table " + table.getName()); 
                Dataset<Row> ds = spark.read().format(table.getFormat())
                        .options(table.getOptions())
                        .schema(schema)
                        .load(tablePath);
                        
                
                ds.createOrReplaceTempView(table.getName());
                String sql=table.getSQL();
                if(sql!="")
                {
                LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Execute sql=" + sql);    
                Dataset<Row> rs = spark.sql(sql);
                rs.createOrReplaceTempView(table.getName());
                rs.persist(StorageLevel.MEMORY_AND_DISK());
                metaData.put(new TableSchema(tablePath, table.getName()), rs.schema());
                LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Schema=" + rs.schema().treeString()); 
                }
                else
                {
                ds.persist(StorageLevel.MEMORY_AND_DISK());
                metaData.put(new TableSchema(tablePath, table.getName()), ds.schema());  
                LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Schema=" + ds.schema().treeString()); 
                }
            
                cachedTables.putIfAbsent(table.getName(), 1);
                }
                else
                {
                 LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Table " + table.getName() + " is persited.");
                }
                
                
            } 
        }
    
     
     
    public void prepareTempView(String sqlText) throws SQLException, ParseException{
        //Map<String, String> options = getOptions(connectionInfo.getProperties(), connectionInfo.getFormat().name(), false);
        LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Preparing sql=" + sqlText);
        for (Map.Entry<String, Integer> entry : cachedTables.entrySet()) {
            LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Tables " + entry.getKey() + ": " + entry.getValue().toString());
           
        }
        
        Set<String> tables = getRelations(spark.sessionState().sqlParser().parsePlan(sqlText));
        for (String table: tables) {
            LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Preparing table " + table);
          
            Optional<Table> tableConfig = connectionInfo.getConfig().findTable(table);
            if (tableConfig.isPresent()) {
                //SupportedFormat format = tableConfig.get().getFormat();
                
                if(cachedTables.getOrDefault(table, 0) == 0)
                {
                LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Loading table metadata " + table);
                List<String> columnName =  tableConfig.get().getColumnName();
                List<String> columnType =  tableConfig.get().getColumnType();
                List<StructField> listOfStructField = new ArrayList<>();
                CatalystSqlParser parser = new CatalystSqlParser();
                for (int i = 0; i < columnName.size(); i++) {
                    listOfStructField.add(DataTypes.createStructField(columnName.get(i), 
                            parser.parseDataType(columnType.get(i)), true));
                    
                 }
                StructType schema=DataTypes.createStructType(listOfStructField);
           
                String tablePath = tableConfig.get().getPath(); //format.getSparkPath(tableConfig.get().getPath(), connectionInfo.isPartitionDiscovery(), table);
                LOGGER.log(Level.INFO, "+++++++ GeoSparkDriver: Loading table " + table); 
                Dataset<Row> ds = spark.read().format(tableConfig.get().getFormat())
                        .options(tableConfig.get().getOptions())
                        .schema(schema)
                        .load(tablePath);
                        
                
                ds.createOrReplaceTempView(table);
                String sql=tableConfig.get().getSQL();
                if(sql!="")
                {
                Dataset<Row> rs = spark.sql(sql);
                rs.createOrReplaceTempView(table);
                rs.persist(StorageLevel.MEMORY_AND_DISK());
                metaData.put(new TableSchema(tablePath, table), rs.schema());
                }
                else
                {
                ds.persist(StorageLevel.MEMORY_AND_DISK());
                metaData.put(new TableSchema(tablePath, table), ds.schema());   
                }
            
                cachedTables.putIfAbsent(table, 1);
                }
                else
                {
                 LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: Table " + table + " is persited.");
                }
                
                
            } else {
                throw new SQLException(String.format("table %s doesn't exist", table));
            }
        }
    }

    private Map<String, String> getOptions(Properties info, String prefix, boolean keepPrefix) {
        return info.entrySet().stream()
                .filter(e->e.getKey().toString().toLowerCase(Locale.getDefault())
                        .startsWith(prefix.toLowerCase(Locale.getDefault())+"."))
                .collect(
                        Collectors.toMap(
                                e -> keepPrefix ? e.getKey().toString() : e.getKey().toString().substring(prefix.length()+1),
                                e -> e.getValue().toString()
                        )
                );
    }

    private Set<String> getRelations(LogicalPlan plan) {
        return scala.collection.JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava()
                .stream()
                .map(logicalPlan -> {
                    if (logicalPlan instanceof UnresolvedRelation) {
                        return ((UnresolvedRelation) logicalPlan).tableName();
                    }
                    return "";
                }).collect(Collectors.toSet());
    }

    public Dataset<Row> getTables() {
        spark.catalog().listTables().select("name").show();
        List<Row> tables = metaData.entrySet().stream()
//                .filter(entry -> entry.getKey().getPath().equalsIgnoreCase(connectionInfo.getPath()))
                .map(entry -> entry.getKey().getTable())
                .map(table -> RowFactory.create(table, "", "", ""))
                .collect(Collectors.toList());

        List<StructField> listOfStructField = new ArrayList<>();
        listOfStructField.add(DataTypes.createStructField("TABLE_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_TYPE", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_SCHEM", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_CAT", DataTypes.StringType, true));

        StructType structType=DataTypes.createStructType(listOfStructField);
        return spark.createDataFrame(tables, structType);
    }

    public Dataset<Row> getColumns(String table) throws SQLException {
        StructField[] fields = metaData.entrySet().stream()
//                .filter(entry -> entry.getKey().getPath().equalsIgnoreCase(connectionInfo.getPath()))
                .filter(entry -> entry.getKey().getTable().equalsIgnoreCase(table))
                .map(Map.Entry::getValue)
                .map(StructType::fields)
                .findFirst()
                .orElseThrow(() -> new SQLException(table + " has not been loaded."));

        List<Row> columns = new ArrayList<>();
        for (int i=0; i<fields.length; i++) {
               //LOGGER.log(Level.INFO, "+++++++++ GeoSparkDriver: " + fields[i].dataType().sql() 
               //       + fields[i].dataType().simpleString() +    fields[i].dataType().typeName());
            
            if(!fields[i].dataType().simpleString().equals("geometry"))
            {
                JdbcType jdbcType = JdbcUtils.getCommonJDBCType(fields[i].dataType()).get();
                columns.add(RowFactory.create("", "", table, fields[i].name(), jdbcType.jdbcNullType(),
                    jdbcType.databaseTypeDefinition(), 0, null));
            }
            else
                columns.add(RowFactory.create("", "", table, fields[i].name(), (int) -157,
                    "geometry", 0, null));
        }
        List<StructField> listOfStructField = new ArrayList<>();
        listOfStructField.add(DataTypes.createStructField("TABLE_CAT", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_SCHEM", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("COLUMN_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("DATA_TYPE", DataTypes.IntegerType, true));
        listOfStructField.add(DataTypes.createStructField("TYPE_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("NULLABLE", DataTypes.IntegerType, true));
        listOfStructField.add(DataTypes.createStructField("COLUMN_SIZE", DataTypes.IntegerType, true));
       


        StructType structType=DataTypes.createStructType(listOfStructField);
        return spark.createDataFrame(columns, structType);
    }


    public Dataset<Row> getPrimaryKeys(String table) throws SQLException {
        StructField[] fields = metaData.entrySet().stream()
//                .filter(entry -> entry.getKey().getPath().equalsIgnoreCase(connectionInfo.getPath()))
                .filter(entry -> entry.getKey().getTable().equalsIgnoreCase(table))
                .map(Map.Entry::getValue)
                .map(StructType::fields)
                .findFirst()
                .orElseThrow(() -> new SQLException(table + " has not been loaded."));

        List<Row> columns = new ArrayList<>();
        
            JdbcType jdbcType = JdbcUtils.getCommonJDBCType(fields[0].dataType()).get();
            columns.add(RowFactory.create("", "", table, fields[0].name(), jdbcType.jdbcNullType(),
                    jdbcType.databaseTypeDefinition(), table + "_pk", (short) 1));
        
        List<StructField> listOfStructField = new ArrayList<>();
        listOfStructField.add(DataTypes.createStructField("TABLE_CAT", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_SCHEM", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("COLUMN_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("DATA_TYPE", DataTypes.IntegerType, true));
        listOfStructField.add(DataTypes.createStructField("TYPE_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("PK_NAME", DataTypes.StringType, true));
         listOfStructField.add(DataTypes.createStructField("KEY_SEQ", DataTypes.ShortType, true));

        StructType structType=DataTypes.createStructType(listOfStructField);
        return spark.createDataFrame(columns, structType);
    }

}
