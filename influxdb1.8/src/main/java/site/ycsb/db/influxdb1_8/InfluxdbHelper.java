package site.ycsb.db.influxdb1_8;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import javax.net.ssl.*;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * InfluxDB 1.8 Helper.
 */
public class InfluxdbHelper {

  private static Logger log = LogManager.getLogger(InfluxdbHelper.class);

  private String url;

  private String username;

  private String password;

  private InfluxDB influxDB;

  private boolean debug;

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public static InfluxdbHelper build(String url, String username, String password) {
    return new InfluxdbHelper(url, username, password);
  }

  public InfluxdbHelper(String url, String username, String password) {
    this.url = url;
    this.username = username;
    this.password = password;

    OkHttpClient.Builder client = new OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .readTimeout(10, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true);

    client.sslSocketFactory(defaultSslSocketFactory(), defaultTrustManager());
    client.hostnameVerifier(noopHostnameVerifier());
    // 超过阈值的idle连接会由连接池关闭，关闭后sockets进入TIME_WAIT状态等待x系统回收，该参数需根据实际连接数适当调整
    client.connectionPool(new ConnectionPool(5, 30, TimeUnit.SECONDS));

    this.influxDB = InfluxDBFactory.connect(url, username, password, client);
  }

  private static SSLSocketFactory defaultSslSocketFactory() {
    try {
      SSLContext sslContext = SSLContexts.createDefault();

      sslContext.init(null, new TrustManager[]{
          defaultTrustManager()
      }, new SecureRandom());
      return sslContext.getSocketFactory();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static X509TrustManager defaultTrustManager() {
    return new X509TrustManager() {
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }

      public void checkClientTrusted(X509Certificate[] certs, String authType) {
      }

      public void checkServerTrusted(X509Certificate[] certs, String authType) {
      }
    };
  }

  private static HostnameVerifier noopHostnameVerifier() {
    return new HostnameVerifier() {
      @Override
      public boolean verify(final String s, final SSLSession sslSession) {
        // true 表示使用ssl方式，但是不校验ssl证书，建议使用这种方式
        return true;
      }
    };
  }

  public void close() {
    if (influxDB == null) {
      return;
    }
    influxDB.close();
  }

  public void setDefaultDatabase(String database) {
    influxDB.setDatabase(database);
  }

  public void enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit) {
    influxDB.enableBatch(actions, flushDuration, flushDurationTimeUnit);
  }

  public boolean databaseExists(String database) {
    return influxDB.databaseExists(database);
  }

  public void createDatabase(String database) {
    influxDB.createDatabase(database);
  }

  public void alterReplicationFactor(String database, String rpName, int replicationFactor) {
    String sql = String.format("ALTER RETENTION POLICY \"%s\" ON \"%s\" REPLICATION %d",
        rpName, database, replicationFactor);
    QueryResult queryResult = influxDB.query(new Query(sql));
    if (queryResult.hasError()) {
      throw new RuntimeException(queryResult.getError());
    }
  }

  public void insert(String database, String rpName, String measurement, Long timestamp,
                     Map<String, String> tags, Map<String, Object> fields) {
    Point.Builder pointBuilder = Point.measurement(measurement);

    if (timestamp == null) {
      pointBuilder.time(nanoTimestamp(), TimeUnit.NANOSECONDS);
    }

    pointBuilder.tag(tags);
    pointBuilder.fields(fields);

    influxDB.write(database, rpName, pointBuilder.build());
  }

  public void delete(String database, String rpName, String measurement, Map<String, String> where) {
    String sql = String.format("delete from \"%s\".\"%s\".\"%s\"", database, rpName, measurement);
    String whereSql = where.entrySet().stream()
        .map(entry -> String.format("%s = '%s'", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(" and "));
    if (!whereSql.isEmpty()) {
      sql += " where " + whereSql;
    }
    influxDB.query(new Query(sql));
  }

  public List<Map<String, Object>> select(String database, String rpName, String measurement,
                                          Set<String> fields, Map<String, String> where) {
    String fieldStr = fields == null || fields.isEmpty() ? "*" : String.join(", ", fields);

    String sql = String.format("select %s from \"%s\".\"%s\".\"%s\" where time <= %sms",
        fieldStr, database, rpName, measurement, System.currentTimeMillis());
    String whereSql = where.entrySet().stream()
        .map(entry -> String.format("%s = '%s'", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(" and "));
    if (!whereSql.isEmpty()) {
      sql += " and " + whereSql;
    }

    if (debug) {
      log.info("select SQL: {}", sql);
    }
    Query query = new Query(sql);
    QueryResult queryResult = influxDB.query(query);
    return queryResultToList(queryResult);
  }

  public List<Map<String, Object>> scan(String database, String rpName, String measurement,
                                        Set<String> fields, long startTimeNs, int recordcount) {
    String fieldStr = fields == null || fields.isEmpty() ? "*" : String.join(", ", fields);
    String sql = String.format("select %s from \"%s\".\"%s\".\"%s\" where time >= %s limit %d",
        fieldStr, database, rpName, measurement, startTimeNs, recordcount);

    if (debug) {
      log.info("scan SQL: {}", sql);
    }
    Query query = new Query(sql);
    QueryResult queryResult = influxDB.query(query);
    List<Map<String, Object>> result = queryResultToList(queryResult);
    if (debug) {
      log.info("scan result size: {}", result.size());
    }
    return result;
  }

  public List<Map<String, Object>> queryResultToList(QueryResult queryResult) {
    if (queryResult.hasError()) {
      throw new RuntimeException(queryResult.getError());
    }
    if (queryResult.getResults().isEmpty()) {
      return Collections.emptyList();
    }
    List<Map<String, Object>> selectResult = new ArrayList<>();
    for (int i = 0; i < queryResult.getResults().size(); i++) {
      QueryResult.Result qrr = queryResult.getResults().get(i);
      if (qrr.hasError()) {
        throw new RuntimeException(qrr.getError());
      }
      if (qrr.getSeries() == null) {
        continue;
      }
      for (int j = 0; j < qrr.getSeries().size(); j++) {
        QueryResult.Series series = qrr.getSeries().get(j);
        for (int k = 0; k < series.getValues().size(); k++) {
          List<Object> fieldValues = series.getValues().get(k);
          Map<String, Object> row = new HashMap<>();
          if (series.getTags() != null && !series.getTags().isEmpty()) {
            row.putAll(series.getTags());
          }
          for (int l = 0; l < fieldValues.size(); l++) {
            row.put(series.getColumns().get(l), fieldValues.get(l));
          }
          // if (row.containsKey("time")) {
          //   log.info("time class: {}, time value: {}", row.get("time").getClass(), row.get("time"));
          // }
          selectResult.add(row);
        }
      }
    }
    return selectResult;
  }

  private long nanoTimestamp() {
    // 获取当前时间的纳秒级时间戳
    return Instant.now().toEpochMilli() * 1_000_000L + Instant.now().getNano();
  }

}
