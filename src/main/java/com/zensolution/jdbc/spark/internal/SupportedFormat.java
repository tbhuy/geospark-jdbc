package com.zensolution.jdbc.spark.internal;

public enum SupportedFormat {
    PARQUET, CSV, JSON, ORC, AVRO, DELTA;

    public String getSparkPath(String path, boolean partitionDiscovery, String table) {
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        switch (this) {
            case PARQUET:
            case CSV:
            case JSON:
            case ORC:
            case AVRO:
                if (partitionDiscovery) {
                    return path + table;
                } else {
                    return path + table + "." + name().toLowerCase();
                }
            case DELTA:
                return path + table;
            default:
                throw new RuntimeException("Unknown path " + path);
        }
    }
}
