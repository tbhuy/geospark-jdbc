package com.zensolution.jdbc.spark.internal.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table
{
    private String name;
    private String path;
    private String format;
    private String sql;
    private List<String> columnType = new ArrayList<String>();
    private List<String> columnName = new ArrayList<String>();
    private Map<String, String> options = new HashMap<>();

    public String getName() {
        return name;
    }

    public String getSQL() {
        return sql;
    }

     
    public String getPath() {
        return path;
    }

    public String getFormat() {
        return format;
    }
    
     public List<String> getColumnType() {
        return columnType;
    }
     
      public List<String> getColumnName() {
        return columnName;
    }
      

    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public String toString()
    {
        return "Table{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", format='" + format + '\'' +
                ", options=" + options +
                '}';
    }
}
