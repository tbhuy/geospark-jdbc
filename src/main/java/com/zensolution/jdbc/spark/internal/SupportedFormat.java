package com.zensolution.jdbc.spark.internal;

import java.io.File;

public enum SupportedFormat {
    PARQUET, CSV, JSON, ORC, AVRO, DELTA;

    public String getSparkPath(String path, String table) {
        switch (this) {
            case PARQUET:
            case CSV:
            case JSON:
            case ORC:
            case AVRO:
                return new File(path, table + "." + name().toLowerCase()).getAbsolutePath();
            case DELTA:
                return path.endsWith("/") ? path + table : path + "/" + table;
            default:
                throw new RuntimeException("Unknown path " + path);
        }
    }
}
