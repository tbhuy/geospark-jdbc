package com.zensolution.jdbc.spark.internal;

import com.zensolution.jdbc.spark.internal.config.Config;

public class ConnectionInfo {

    private String master;
    private Config config;

    public ConnectionInfo(String master, Config config) {
        this.master = master;
        this.config = config;
    }

    public String getMaster() {
        return master;
    }

    public Config getConfig() {
        return config;
    }
}
