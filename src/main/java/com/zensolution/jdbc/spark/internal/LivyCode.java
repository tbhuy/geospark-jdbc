package com.zensolution.jdbc.spark.internal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LivyCode
{
    private static SparkSession spark;

    // Build from ConnectionInfo
    private static List<Map<String, Object>> tables = new ArrayList<>();

    private static Set<String> getRelations(LogicalPlan plan) {
        return scala.collection.JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava()
                .stream()
                .map(logicalPlan -> {
                    if (logicalPlan instanceof UnresolvedRelation) {
                        return ((UnresolvedRelation) logicalPlan).tableName();
                    }
                    return "";
                }).collect(Collectors.toSet());
    }

    private static Optional<Map<String, Object>> findTable(String table) {
        return tables.stream().filter(e -> e.containsKey(table)).findFirst();
    }

    private static void prepareTempView(String sqlText) throws SQLException, ParseException
    {
        Set<String> tables = getRelations(spark.sessionState().sqlParser().parsePlan(sqlText));
        for (String table: tables) {
            Optional<Map<String, Object>> tableConfig = findTable(table);
            if (tableConfig.isPresent()) {
                String tablePath = (String) tableConfig.get().get("path");
                Dataset<Row> ds = spark.read().format((String) tableConfig.get().get("format"))
                        .options(tableConfig.get().get("options") == null ? new HashMap<>() : (Map<String, String>) tableConfig.get().get("options"))
                        .load(tablePath);
                ds.createOrReplaceTempView(table);
                //metaData.put(new TableSchema(tablePath, table), ds.schema());
            } else {
                throw new SQLException(String.format("table %s doesn't exist", table));
            }
        }
    }

    public static Map<String, Object> executeQuery(String sqlText) throws SQLException, ParseException {
        prepareTempView(sqlText);
        Dataset<Row> df = spark.sql(sqlText);
        Map<String, Object> result = new HashMap<>();
        result.put("count", df.count());
        result.put("data", df.collectAsList());
        return result;
    }
}
