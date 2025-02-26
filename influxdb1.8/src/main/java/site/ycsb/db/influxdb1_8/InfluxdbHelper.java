package site.ycsb.db.influxdb1_8;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * InfluxDB 1.8 Helper.
 */
public class InfluxdbHelper {

  private String url;

  private String username;

  private String password;

  private InfluxDB influxDB;

  public static InfluxdbHelper build(String url, String username, String password) {
    return new InfluxdbHelper(url, username, password);
  }

  public InfluxdbHelper(String url, String username, String password) {
    this.url = url;
    this.username = username;
    this.password = password;
    this.influxDB = InfluxDBFactory.connect(url, username, password);
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

  public void insert(String database, String rpName, String measurement,
                     Map<String, String> tags, Map<String, Object> fields) {
    Point.Builder pointBuilder = Point.measurement(measurement)
        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    pointBuilder.tag(tags);
    pointBuilder.fields(fields);

    influxDB.write(database, rpName, pointBuilder.build());
  }

  public void delete(String database, String rpName, String measurement, Map<String, String> tags) {
    String sql = String.format("delete from \"%s\".\"%s\".\"%s\"", database, rpName, measurement);
    String where = tags.entrySet().stream()
        .map(entry -> String.format("\"%s\" = \"%s\"", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(" and "));
    if (!where.isEmpty()) {
      sql += " where " + where;
    }
    influxDB.query(new Query(sql));
  }
}
