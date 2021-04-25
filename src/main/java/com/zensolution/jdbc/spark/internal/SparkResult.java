package com.zensolution.jdbc.spark.internal;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class SparkResult implements Serializable
{
    private static final long serialVersionUID = -1;

    public List<Row> list;
    public StructType schema;

    public SparkResult(List<Row> list, StructType schema) {
        this.list = list;
        this.schema = schema;
    }

    public Iterator<Row> toLocalIterator() {
        return this.list.iterator();
    }

    public StructType schema() {
        return this.schema;
    }

    public int count() {
        return this.list.size();
    }
}
