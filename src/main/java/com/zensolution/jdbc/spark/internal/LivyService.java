package com.zensolution.jdbc.spark.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LivyService implements Service  {
    static final String SESSIONS_URI = "/sessions";
    private static final String APPLICATION_JSON = "application/json";
    private SessionInfo sessionInfo;

    private ConnectionInfo connectionInfo;

    private final CloseableHttpClient client = HttpClientBuilder.create()
            .setUserAgent("livy-client-http")
            .build();

    private final ObjectMapper mapper = new ObjectMapper();

    public LivyService(ConnectionInfo info) throws SQLException {
        this.connectionInfo = info;

        try {
            CreateSessionRequest request = new CreateSessionRequest("spark", null, new HashMap<>());
            sessionInfo = mapper.readValue(callRestAPI("/sessions", "POST", mapper.writeValueAsString(request)), SessionInfo.class);
            while (!sessionInfo.isReady()) {
                Thread.sleep(1000);
                sessionInfo = getSessionInfo(sessionInfo.id);
                if (sessionInfo.isFinished()) {
                    String msg = "Session " + sessionInfo.id + " is finished, appId: " + sessionInfo.appId;
                    throw new SQLException(msg);
                }
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private SessionInfo getSessionInfo(int sessionId) throws Exception {
        return mapper.readValue(callRestAPI("/sessions/" + sessionId, "GET", null), SessionInfo.class);
    }

    private void closeSession(int sessionId) {
        try {
            callRestAPI("/sessions/" + sessionId, "DELETE", null);
        } catch (Exception e) {
//            LOGGER.error(String.format("Error closing session for user with session ID: %s",
//                    sessionId), e);
        }
    }

    @Override
    public SparkResult executeQuery(String sqlText)
            throws SQLException, ParseException
    {
        try {
            String code =
                    "import org.json4s._\n" +
                    "val df = spark.read.json(\"employees.json\")\n" +
                    "val fn = df.schema.fieldNames\n" +
                    "val rows = df.rdd.map(row => fn.map(field => field -> row.getAs(field)).toMap).collect()\n" +
                    "implicit val formats: Formats = DefaultFormats\n" +
                    "org.json4s.jackson.Serialization.write(rows)";
            ExecuteRequest request = new ExecuteRequest(code, "spark");
            StatementInfo stmtInfo = mapper.readValue(
                    callRestAPI("/sessions/" + sessionInfo.id + "/statements", "POST", mapper.writeValueAsString(request)),
                    StatementInfo.class
            );
            while (!stmtInfo.isAvailable()) {
                Thread.sleep(1000);
                stmtInfo = getStatementInfo(stmtInfo.id);
            }
            System.out.println(getResultFromStatementInfo(stmtInfo));
        } catch (Exception ex) {
            throw new SQLException(ex);
        }

        return null;
    }

    private StatementInfo getStatementInfo(int statementId) throws Exception {
        return mapper.readValue(callRestAPI("/sessions/" + sessionInfo.id + "/statements/" + statementId , "GET", null), StatementInfo.class);
    }

    private String getResultFromStatementInfo(StatementInfo stmtInfo) throws Exception {
        return stmtInfo.output.data.get("text/plain");
    }

    @Override
    public SparkResult getTables()
            throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SparkResult getColumns(String table)
            throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        closeSession(sessionInfo.id);
        try {
            client.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String callRestAPI(String targetURL, String method, String jsonData)
            throws SQLException {
        try {
            URI uri = new URI(connectionInfo.getMaster() + targetURL);
            System.out.println(uri);
            HttpRequestBase request = null;
            if (method.equals("POST")) {
                request = new HttpPost(uri);
                ((HttpPost) request).setEntity(new StringEntity(jsonData));
            }
            else if (method.equals("GET")) {
                request = new HttpGet(uri);
            }
            else if (method.equals("DELETE")) {
                request = new HttpDelete(uri);
            }

            request.addHeader("Content-Type", APPLICATION_JSON);
            request.addHeader("X-Requested-By", "zeppelin");
            CloseableHttpResponse resonse = client.execute(request);

            String content = IOUtils.toString(resonse.getEntity().getContent(), "UTF-8");
            resonse.close();

            return content;

        } catch (Exception e) {
            throw new SQLException(e);
        }
    }


    /*
     * We create these POJO here to accommodate livy 0.3 which is not released yet. livy rest api has
     * some changes from version to version. So we create these POJO in zeppelin side to accommodate
     * incompatibility between versions. Later, when livy become more stable, we could just depend on
     * livy client jar.
     */
    private static class CreateSessionRequest {
        public final String kind;
        public final String proxyUser;
        public final Map<String, String> conf;

        CreateSessionRequest(String kind, String proxyUser, Map<String, String> conf) {
            this.kind = kind;
            this.proxyUser = proxyUser;
            this.conf = conf;
        }
    }

    /**
     *
     */
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class SessionInfo {

        public int id;
        public String name;
        public String appId;
        public String webUIAddress;
        public String owner;
        public String proxyUser;
        public String state;
        public String kind;
        public Map<String, String> appInfo;
        public List<String> log;

//        public SessionInfo(int id, String appId, String owner, String proxyUser, String state,
//                String kind, Map<String, String> appInfo, List<String> log) {
//            this.id = id;
//            this.appId = appId;
//            this.owner = owner;
//            this.proxyUser = proxyUser;
//            this.state = state;
//            this.kind = kind;
//            this.appInfo = appInfo;
//            this.log = log;
//        }

        public boolean isReady() {
            return state.equals("idle");
        }

        public boolean isFinished() {
            return state.equals("error") || state.equals("dead") || state.equals("success");
        }
    }

    private static class SessionLog {
        public int id;
        public int from;
        public int size;
        public List<String> log;
    }

    static class ExecuteRequest {
        public final String code;
        public final String kind;

        ExecuteRequest(String code, String kind) {
            this.code = code;
            this.kind = kind;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown=true)
    private static class StatementInfo {
        public Integer id;
        public String state;
        public double progress;
        public StatementOutput output;

        public boolean isAvailable() {
            return state.equals("available") || state.equals("cancelled");
        }

        public boolean isCancelled() {
            return state.equals("cancelled");
        }

        @JsonIgnoreProperties(ignoreUnknown=true)
        private static class StatementOutput {
            public String status;
            public String executionCount;
            public Map<String, String> data;
            public String ename;
            public String evalue;
            public String[] traceback;
            //public TableMagic tableMagic;

            public boolean isError() {
                return status.equals("error");
            }

//            private static class Data {
//                @SerializedName("text/plain")
//                public String plainText;
//                @SerializedName("image/png")
//                public String imagePng;
//                @SerializedName("application/json")
//                public String applicationJson;
//                @SerializedName("application/vnd.livy.table.v1+json")
//                public TableMagic applicationLivyTableJson;
//            }
//
//            private static class TableMagic {
//                @SerializedName("headers")
//                List<Map> headers;
//
//                @SerializedName("data")
//                List<List> records;
//            }
        }
    }

    private static class LivyVersionResponse {
        public String url;
        public String branch;
        public String revision;
        public String version;
        public String date;
        public String user;
    }
}
