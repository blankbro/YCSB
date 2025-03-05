package site.ycsb.db.influxdb1_8;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * InfluxDB 1.8 Helper.
 */
public class InfluxdbHelper {

  private static final Logger LOG = LogManager.getLogger(InfluxdbHelper.class);

  private InfluxDB influxDB;

  private boolean debug;

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public InfluxdbHelper(String url, String username, String password, int batchSize, int batchIntervalMs) {
    InfluxDBPoolFactory poolFactory = new InfluxDBPoolFactory(url, username, password);
    poolFactory.enableBatch(batchSize, batchIntervalMs, TimeUnit.MILLISECONDS);
    try {
      this.influxDB = poolFactory.create();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public void close() {
    this.influxDB.flush();
    this.influxDB.close();
  }

  public boolean databaseExists(String database) throws Exception {
    return influxDB.databaseExists(database);
  }

  public void createDatabase(String database) throws Exception {
    influxDB.createDatabase(database);
  }

  public void alterReplicationFactor(String database, String rpName, int replicationFactor) throws Exception {
    String sql = String.format("ALTER RETENTION POLICY \"%s\" ON \"%s\" REPLICATION %d",
            rpName, database, replicationFactor);

    QueryResult queryResult = influxDB.query(new Query(sql));
    if (queryResult.hasError()) {
      throw new RuntimeException(queryResult.getError());
    }
  }

  public void insert(String database, String rpName, String measurement,
                     Long timestamp, TimeUnit timeUnit,
                     Map<String, String> tags, Map<String, Object> fields) throws Exception {
    Point.Builder pointBuilder = Point.measurement(measurement);

    if (timestamp == null) {
      pointBuilder.time(nanoTimestamp(), TimeUnit.NANOSECONDS);
    } else {
      pointBuilder.time(timestamp, timeUnit);
    }

    if (debug) {
      LOG.info(String.format("Inserting %s, tags: %s, fields: %s", measurement, tags, fields));
    }

    pointBuilder.tag(tags);
    pointBuilder.fields(fields);

    influxDB.write(database, rpName, pointBuilder.build());
  }

  public void delete(String database, String rpName, String measurement, Map<String, String> where) throws Exception {
    String sql = String.format("delete from \"%s\".\"%s\".\"%s\"", database, rpName, measurement);
    String whereSql = where.entrySet().stream()
            .map(entry -> String.format("\"%s\" = '%s'", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(" and "));
    if (!whereSql.isEmpty()) {
      sql += " where " + whereSql;
    }
    influxDB.query(new Query(sql));
  }

  public List<Map<String, Object>> select(String database, String rpName, String measurement,
                                          Set<String> fields, Map<String, String> where) throws Exception {
    String fieldStr = fields == null || fields.isEmpty() ? "*" : String.join(", ", fields);

    String sql = String.format("select /*+ full_series */ %s from \"%s\".\"%s\".\"%s\" where time <= now()",
            fieldStr, database, rpName, measurement);
    String whereSql = where.entrySet().stream()
            .map(entry -> String.format("\"%s\" = '%s'", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(" and "));
    if (!whereSql.isEmpty()) {
      sql += " and " + whereSql;
    }

    if (debug) {
      LOG.info("select SQL: {}", sql);
    }
    Query query = new Query(sql);
    QueryResult queryResult = influxDB.query(query);
    return queryResultToList(queryResult);
  }

  public List<Map<String, Object>> scan(String database, String rpName, String measurement,
                                        Set<String> fields, Map<String, String> where,
                                        long startTime, long endTime) throws Exception {

    String fieldStr = fields == null || fields.isEmpty() ? "*" : String.join(", ", fields);
    String sql = String.format(
            "select /*+ full_series */ %s from \"%s\".\"%s\".\"%s\" where time >= %sms and time <= %sms",
            fieldStr, database, rpName, measurement, startTime, endTime);

    String whereSql = where.entrySet().stream()
            .map(entry -> String.format("\"%s\" = '%s'", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(" and "));
    if (!whereSql.isEmpty()) {
      sql += " and " + whereSql;
    }

    if (debug) {
      LOG.info("scan SQL: {}", sql);
    }

    Query query = new Query(sql);
    QueryResult queryResult = influxDB.query(query);
    List<Map<String, Object>> result = queryResultToList(queryResult);

    if (debug) {
      LOG.info("scan result size: {}", result.size());
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
