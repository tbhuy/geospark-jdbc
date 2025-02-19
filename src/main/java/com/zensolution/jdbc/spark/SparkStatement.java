package com.zensolution.jdbc.spark;

import com.zensolution.jdbc.spark.internal.SparkService;
import com.zensolution.jdbc.spark.jdbc.AbstractJdbcStatement;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkStatement extends AbstractJdbcStatement {

    private static final Logger LOGGER = Logger.getLogger(SparkStatement.class.getName());

    private SparkConnection connection;
    private SparkService sparkService;
    private int fetchSize = 1;
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    private int maxRows = 0;
    private boolean closed;
    private SparkResultSet resultSet;

    protected SparkStatement(SparkConnection connection, SparkService sparkService) {
        LOGGER.log(Level.INFO, "SparkStatement: connection=" + connection.getConnectionInfo());
        this.connection = connection;
        this.sparkService = sparkService;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        LOGGER.log(Level.INFO, "SparkStatement: executeQuery() sql=" + sql);
        try {
            // For ontop
            sql = sql.replaceAll("\"", "");
            sql = sql.replaceAll("AS TEXT", "as string");
            String pattern = "'(POLYGON\\(\\(.*\\)\\))'";
            sql = sql.replaceAll(pattern, "ST_GeomFromWKT('$1')");
            pattern = "'(POINT\\(.*\\))'";
            sql = sql.replaceAll(pattern, "ST_GeomFromWKT('$1')");
            LOGGER.log(Level.INFO, "SparkStatement: executeQuery() modified sql=" + sql);   
            resultSet = new SparkResultSet(connection.getConnectionInfo(), sql, sparkService);
            LOGGER.log(Level.INFO, "SparkStatement: resultSet count=" +  resultSet.getCount());     
            return resultSet;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkOpen();
        maxRows = max;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        LOGGER.log(Level.INFO, "GeoSparkDriver: execute() - sql={}", sql);
        try {
            resultSet = new SparkResultSet(connection.getConnectionInfo(), sql, sparkService);
        } catch (ParseException e) {
            throw new SQLException(e);
        }
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return resultSet.getCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        //TODO
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    protected void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement closed");
        }
    }
}
