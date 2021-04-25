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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SparkService implements Service {
    private ConnectionInfo connectionInfo;
    private SparkSession spark;
    private static Map<TableSchema, StructType> metaData = new ConcurrentHashMap();

    public SparkService(ConnectionInfo info) throws SQLException {
        this.connectionInfo = info;
        this.spark = buildSparkSession();
    }

    private SparkSession buildSparkSession() throws SQLException {
        final SparkSession.Builder builder = SparkSession.builder().master(connectionInfo.getMaster()).appName("spark-jdbc-driver")
                .config("spark.sql.session.timeZone", "UTC");

        // TODO for spark
        Map<String, String> options = connectionInfo.getConfig().getOptions(); //getOptions(connectionInfo.getProperties(), "spark", true);

        Optional<SparkConfProvider> sparkConfProvider = SparkConfProvider.getSparkConfProvider(connectionInfo);
        if ( sparkConfProvider.isPresent() ) {
            options.putAll(sparkConfProvider.get().getSparkConf(connectionInfo));
        }
        options.entrySet().stream()
                .forEach(entry-> builder.config(entry.getKey(), entry.getValue()));

        return builder.getOrCreate().newSession();
    }

    public SparkResult executeQuery(String sqlText) throws SQLException, ParseException {
        prepareTempView(sqlText);
        Dataset<Row> df = spark.sql(sqlText);
        return new SparkResult(df.collectAsList(), df.schema());
    }

    private void prepareTempView(String sqlText) throws SQLException, ParseException {
        //Map<String, String> options = getOptions(connectionInfo.getProperties(), connectionInfo.getFormat().name(), false);

        Set<String> tables = getRelations(spark.sessionState().sqlParser().parsePlan(sqlText));
        for (String table: tables) {
            Optional<Table> tableConfig = connectionInfo.getConfig().findTable(table);
            if (tableConfig.isPresent()) {
                //SupportedFormat format = tableConfig.get().getFormat();
                String tablePath = tableConfig.get().getPath(); //format.getSparkPath(tableConfig.get().getPath(), connectionInfo.isPartitionDiscovery(), table);
                Dataset<Row> ds = spark.read().format(tableConfig.get().getFormat())
                        .options(tableConfig.get().getOptions())
                        .load(tablePath);
                ds.createOrReplaceTempView(table);
                metaData.put(new TableSchema(tablePath, table), ds.schema());
            } else {
                throw new SQLException(String.format("table %s doesn't exist", table));
            }
        }
    }

//    private Map<String, String> getOptions(Properties info, String prefix, boolean keepPrefix) {
//        return info.entrySet().stream()
//                .filter(e->e.getKey().toString().toLowerCase(Locale.getDefault())
//                        .startsWith(prefix.toLowerCase(Locale.getDefault())+"."))
//                .collect(
//                        Collectors.toMap(
//                                e -> keepPrefix ? e.getKey().toString() : e.getKey().toString().substring(prefix.length()+1),
//                                e -> e.getValue().toString()
//                        )
//                );
//    }

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

    public SparkResult getTables() throws SQLException {
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
        Dataset<Row> ds = spark.createDataFrame(tables, structType);
        return new SparkResult(ds.collectAsList(), ds.schema());
    }

    public SparkResult getColumns(String table) throws SQLException {
        StructField[] fields = metaData.entrySet().stream()
//                .filter(entry -> entry.getKey().getPath().equalsIgnoreCase(connectionInfo.getPath()))
                .filter(entry -> entry.getKey().getTable().equalsIgnoreCase(table))
                .map(Map.Entry::getValue)
                .map(StructType::fields)
                .findFirst()
                .orElseThrow(() -> new SQLException(table + " has not been loaded."));

        List<Row> columns = new ArrayList<>();
        for (int i=0; i<fields.length; i++) {
            JdbcType jdbcType = JdbcUtils.getCommonJDBCType(fields[i].dataType()).get();
            columns.add(RowFactory.create("", "", table, fields[i].name(), jdbcType.jdbcNullType(),
                    jdbcType.databaseTypeDefinition()));
        }
        List<StructField> listOfStructField = new ArrayList<>();
        listOfStructField.add(DataTypes.createStructField("TABLE_CAT", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_SCHEM", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("TABLE_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("COLUMN_NAME", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("DATA_TYPE", DataTypes.IntegerType, true));
        listOfStructField.add(DataTypes.createStructField("TYPE_NAME", DataTypes.StringType, true));

        StructType structType=DataTypes.createStructType(listOfStructField);
        Dataset<Row> ds = spark.createDataFrame(columns, structType);
        return new SparkResult(ds.collectAsList(), ds.schema());
    }

    @Override
    public void close()
    {
        spark.close();
    }
}
