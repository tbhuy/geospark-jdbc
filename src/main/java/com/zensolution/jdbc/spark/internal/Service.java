package com.zensolution.jdbc.spark.internal;

import org.apache.spark.sql.catalyst.parser.ParseException;

import java.sql.SQLException;

public interface Service
{
    SparkResult executeQuery(String sqlText) throws SQLException, ParseException;
    SparkResult getTables() throws SQLException;
    SparkResult getColumns(String table) throws SQLException;
    void close();
}
