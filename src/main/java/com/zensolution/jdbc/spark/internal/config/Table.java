package com.zensolution.jdbc.spark.internal.config;

import java.util.HashMap;
import java.util.Map;

public class Table
{
    private String name;
    private String path;
    private String format;
    private Map<String, String> options = new HashMap<>();

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public String getFormat() {
        return format;
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
