package com.zensolution.jdbc.spark.internal;

import com.zensolution.jdbc.spark.internal.config.Config;

public class LivyTest
{
    public static void main(String[] args) throws Exception
    {
        LivyService service = new LivyService(new ConnectionInfo(true, "http://localhost:8998", new Config()));
        service.executeQuery("");
    }
}
