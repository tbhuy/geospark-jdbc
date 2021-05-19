package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.internal.ConnectionInfo;
import com.zensolution.jdbc.spark.internal.SparkService;
import com.zensolution.jdbc.spark.internal.config.Config;
import com.zensolution.jdbc.spark.jdbc.AbstractJdbcConnection;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SparkConnection extends AbstractJdbcConnection {
    
    /**
     * Directory where the Parquet files to use are located
     */
    private ConnectionInfo connectionInfo;

    /**
     * Directory where the Parquet files to use are located
     */
    private Properties info;

    /**
     * Stores whether this Connection is closed or not
     */
    private boolean closed = false;

    /*
     *  Unified interface to access spark
     */
    private SparkService sparkService;

    /**
     * Collection of all created Statements
     */
    private List<Statement> statements = new ArrayList<Statement>();

    /**
     * Creates a new CsvConnection that takes the supplied path
     */
    protected SparkConnection(String master, Config config) throws SQLException {
        this.connectionInfo = new ConnectionInfo(master, config);
        this.sparkService = new SparkService(connectionInfo);
    }


    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }


    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        SparkStatement statement = new SparkStatement(this, sparkService);     
        statements.add(statement);
        return statement;
    }


    @Override
    public Statement createStatement()  throws SQLException {
        checkOpen();
        SparkStatement statement = new SparkStatement(this, sparkService); 
        statements.add(statement);   
        return statement;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void close() throws SQLException {
        if ( !this.closed ) {
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public String getSchema() throws SQLException {
        return "";
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection has been closed.");
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new SparkDatabaseMetaData(this, this.sparkService);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return TRANSACTION_NONE;
    }

    // TODO need to be fixed
    protected String getURL() {
        return SparkDriver.URL_PREFIX + "xxxx";
    }
}
