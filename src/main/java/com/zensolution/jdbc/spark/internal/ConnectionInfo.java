package com.zensolution.jdbc.spark.internal;

import java.util.Locale;
import java.util.Properties;

public class ConnectionInfo {

    private String master;
    private String path;
    private SupportedFormat format;
    private boolean partitionDiscovery;
    private Properties prop;

    public ConnectionInfo(String master, Properties info) {
        // validate argument(s)
        String path = info.getProperty("path");
        if (path == null || path.length() == 0) {
            throw new IllegalArgumentException("Unknown Path");
        }

        boolean partitionDiscovery = false;
        if (info.getProperty("partitionDiscovery") != null) {
            partitionDiscovery = Boolean.parseBoolean(info.getProperty("partitionDiscovery"));
        }

        this.master = master;
        this.path = path;
        this.partitionDiscovery = partitionDiscovery;
        this.prop = info;
        this.format = parseFormat(info.getProperty("format"));
    }

    private SupportedFormat parseFormat(String format) {
        return format == null ? SupportedFormat.PARQUET : SupportedFormat.valueOf(format.toUpperCase(Locale.getDefault()));
    }

    public String getMaster() {
        return master;
    }

    public String getPath() {
        return path;
    }

    public boolean isPartitionDiscovery() {
        return partitionDiscovery;
    }

    public Properties getProperties() {
        return prop;
    }

    public SupportedFormat getFormat() {
        return format;
    }
}
