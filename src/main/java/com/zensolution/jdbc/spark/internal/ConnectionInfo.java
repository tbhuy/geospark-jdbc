package com.zensolution.jdbc.spark.internal;

import java.util.Locale;
import java.util.Properties;

public class ConnectionInfo {

    private String master;
    private String path;
    private SupportedFormat format;
    private Properties prop;

    public ConnectionInfo(String master, String path, Properties info) {
        this.master = master;
        this.path = path;
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

    public Properties getProperties() {
        return prop;
    }

    public SupportedFormat getFormat() {
        return format;
    }
}
