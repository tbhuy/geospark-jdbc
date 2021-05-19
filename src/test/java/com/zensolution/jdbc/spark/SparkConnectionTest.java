package com.zensolution.jdbc.spark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SparkConnectionTest {

    @Test
    public void testConnectionInfo() throws Exception {
//        SparkConnection conn = (SparkConnection)DriverManager
//                .getConnection("jdbc:spark:local?path=/Users/foobar/temp/console");
//        Assertions.assertEquals("/Users/foobar/temp/console", conn.getConnectionInfo().getPath());
//        Assertions.assertEquals(1, conn.getConnectionInfo().getProperties().size());
//        Assertions.assertEquals("/Users/foobar/temp/console", conn.getConnectionInfo().getProperties().getProperty("path"));
//        Assertions.assertEquals(SupportedFormat.PARQUET, conn.getConnectionInfo().getFormat());
//
//        conn = (SparkConnection)DriverManager
//                .getConnection("jdbc:spark:local?path=/Users/foobar/temp/console&format=csv&timezone=GMT");
//        Assertions.assertEquals("/Users/foobar/temp/console", conn.getConnectionInfo().getPath());
//        Assertions.assertEquals(3, conn.getConnectionInfo().getProperties().size());
//        Assertions.assertEquals("/Users/foobar/temp/console", conn.getConnectionInfo().getProperties().getProperty("path"));
//        Assertions.assertEquals("csv", conn.getConnectionInfo().getProperties().getProperty("format"));
//        Assertions.assertEquals("GMT", conn.getConnectionInfo().getProperties().getProperty("timezone"));
//        Assertions.assertEquals(SupportedFormat.CSV, conn.getConnectionInfo().getFormat());
//
//
//        assertThrows(IllegalArgumentException.class, () -> {
//            DriverManager.getConnection("jdbc:spark:local?path=/Users/foobar/temp/console&format=NA&timezone=GMT");
//        });
    }

    
    @Test
    public void testConnection() throws Exception {
        File root = new File(this.getClass().getClassLoader().getResource("samples/userdata1").toURI());

        Connection conn = DriverManager.getConnection("jdbc:spark:local?config="+root.getAbsolutePath());
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from userdata1");
        Assertions.assertEquals(13, rs.getMetaData().getColumnCount());
        int count = 0;
        while ( rs.next() ) {
            if ( rs.getInt("id") == 10 ) {
                Assertions.assertEquals("Emily", rs.getString("first_name"));
                Assertions.assertEquals("Stewart", rs.getString("last_name"));
                Assertions.assertEquals("", rs.getString("comments"));
                Assertions.assertEquals(Timestamp.valueOf("2016-02-03 18:29:47"), rs.getTimestamp("registration_dttm"));
            }
            count++;

        }
        Assertions.assertEquals(1000, count);
    }
}

