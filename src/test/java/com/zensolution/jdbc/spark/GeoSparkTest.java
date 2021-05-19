package com.zensolution.jdbc.spark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class SedonaTest {
    
    @Test
    public void testPoint() throws Exception {
        File sedonaConf = new File(this.getClass().getClassLoader().getResource("samples/sedona.conf").toURI());

        Connection conn = DriverManager.getConnection("jdbc:spark:local?config="+sedonaConf.getAbsolutePath());
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select ST_Transform(ST_Point(cast(Latitude as Decimal(24,20)), cast(Longitude as Decimal(24,20))), 'epsg:4326','epsg:2163', true)  as location  from ais limit 100");

        Assertions.assertEquals(1, rs.getMetaData().getColumnCount());
        int count = 0;
        while ( rs.next() ) {
           Assertions.assertTrue(rs.getString("location").contains("POINT"));      
            count++;

        }
        Assertions.assertEquals(100, count);
      
    }
}

