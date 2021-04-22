package com.zensolution.jdbc.spark.internal.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Config
{
    private Map<String, String> options = new HashMap<>();
    private List<Table> tables = new ArrayList<>();

    public Map<String, String> getOptions() {
        return options;
    }

    public List<Table> getTables() {
        return tables;
    }

    public Optional<Table> findTable(String table) {
        return tables.stream().filter(t -> table.equals(t.getName())).findFirst();
    }

    public static Config load(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(new File(path), Config.class);
        return config;
    }

    @Override
    public String toString()
    {
        return "Config{" +
                "options=" + options +
                ", tables=" + tables +
                '}';
    }
}
