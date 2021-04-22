package com.zensolution.jdbc.spark.internal.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ConfigTest
{
    @Test
    public void testLoad() throws IOException
    {
        Config config = Config.load("src/test/resources/samples/spark-jdbc.json");
        System.out.println(config);
    }
}
