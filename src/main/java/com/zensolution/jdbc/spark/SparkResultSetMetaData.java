package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.jdbc.AbstractJdbcResultSetMetaData;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkResultSetMetaData extends AbstractJdbcResultSetMetaData {

    private List<StructField> structFields = new ArrayList<>();
    private static final Logger LOGGER = Logger.getLogger(SparkResultSetMetaData.class.getName());

    public SparkResultSetMetaData(StructType structType) {
        
        for (int i = 0; i < structType.fields().length; i++) {
            this.structFields.add(structType.fields()[i]);
            LOGGER.log(Level.INFO, "GeoSparkDriver: Metadata [" + i + ": " + structType.fields()[i].name() + " (" +  structType.fields()[i].dataType().typeName()+")]");

        }
    }
    
   
    public int indexOf(String column) throws SQLException {
         LOGGER.log(Level.INFO, "GeoSparkDriver: indexOf " + column);

        //column = column.replaceAll("'NULLABLE' ", "");
        //if (column.toLowerCase().contains("nullable"))
        //    return 6;
        for (int i = 0; i < structFields.size(); i++ ) {
            if (structFields.get(i).name().equalsIgnoreCase(column)) {
                return i+1;
            }
        }
        throw new SQLException("GeoSpark driver: Unknown column '" + column + "'");
    }

    @Override
    public int getColumnCount() throws SQLException {
        LOGGER.log(Level.INFO, "GeoSparkDriver: Count count={}",structFields.size());
        return structFields.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return this.structFields.get(column-1).nullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 20;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        LOGGER.log(Level.INFO, "GeoSparkDriver: getColumnName {}", this.structFields.get(column-1).name());
        return this.structFields.get(column-1).name();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "sparkschem";
    }


    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }


    @Override
    public int getColumnType(int column) throws SQLException {
        return JdbcUtils.getCommonJDBCType(this.structFields.get(column-1).dataType()).get().jdbcNullType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return JdbcUtils.getCommonJDBCType(this.structFields.get(column - 1).dataType()).get().databaseTypeDefinition();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("GeoSparkDriver: Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
