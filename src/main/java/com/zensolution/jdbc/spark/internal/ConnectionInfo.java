package com.zensolution.jdbc.spark.internal;

import com.zensolution.jdbc.spark.internal.config.Config;

public class ConnectionInfo {

    private boolean useLivy;
    private String master;
    private Config config;

    public ConnectionInfo(boolean useLivy, String master, Config config) {
        this.useLivy = useLivy;
        this.master = master;
        this.config = config;
    }

    public boolean useLivy() {
        return this.useLivy;
    }

    public String getMaster() {
        return master;
    }

    public Config getConfig() {
        return config;
    }
}
