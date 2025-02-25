package site.ycsb.db.influxdb1_8;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import site.ycsb.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Influxdb 1.8 client.
 * <p>
 * See {@code influxdb1.8/README.md} for details.
 */
public class InfluxDB18Client extends site.ycsb.DB {

  private static Logger log = LogManager.getLogger(InfluxDB18Client.class);

  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  private static final String TAG_NAME = "rowkey";

  private InfluxdbHelper influxdbHelper;

  private InfluxDB influxDB;

  private String database;

  private int batchSize;

  private int batchInterval;

  @Override
  public void init() throws DBException {
    final Properties props = getProperties();
    final String url = props.getProperty("url", "http://localhost:8086");
    database = props.getProperty("database", "benchmark-ycsb");
    final String username = props.getProperty("username", "username");
    final String password = props.getProperty("password", "password");
    batchSize = Integer.parseInt(props.getProperty("batch_size", "1"));
    batchInterval = Integer.parseInt(props.getProperty("batch_interval_ms", "1"));

    THREAD_COUNT.getAndIncrement();
    synchronized (THREAD_COUNT) {
      if (influxdbHelper == null) {
        influxdbHelper = InfluxdbHelper.build(url, username, password);
        if (!influxdbHelper.databaseExists(database)) {
          influxdbHelper.createDatabase(database);
        }
        influxdbHelper.setDefaultDatabase(database);
        influxdbHelper.alterReplicationFactor(database, "autogen", 3);

        if (batchSize > 1) {
          influxdbHelper.enableBatch(batchSize, batchInterval, TimeUnit.MILLISECONDS);
        }
      }
    }

  }


  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String fieldStr = String.join(", ", fields);
    Query query = new Query(String.format("select %s from %s", fieldStr, table));
    QueryResult queryResult = influxDB.query(query);
    if (queryResult.hasError()) {
      return Status.ERROR;
    }
    if (queryResult.getResults().isEmpty()) {
      return Status.NOT_FOUND;
    }
    QueryResult.Result firstQueryResult = queryResult.getResults().get(0);
    if (firstQueryResult.getSeries().isEmpty()) {
      return Status.NOT_FOUND;
    }
    QueryResult.Series series = firstQueryResult.getSeries().get(0);
    for (int i = 0; i < series.getValues().size(); i++) {
      List<Object> values = series.getValues().get(i);
      for (int j = 0; j < values.size(); j++) {
        Object value = values.get(j);
        ByteIterator valueByteIterator = null;
        if (value instanceof String) {
          valueByteIterator = new StringByteIterator((String) value);
        } else if (value instanceof Integer) {
          valueByteIterator = new NumericByteIterator((Integer) value);
        } else if (value instanceof Double) {
          valueByteIterator = new NumericByteIterator((Double) value);
        } else if (value instanceof Long) {
          valueByteIterator = new NumericByteIterator((Long) value);
        } else if (value instanceof Float) {
          valueByteIterator = new NumericByteIterator((Float) value);
        } else if (value instanceof Boolean) {
          valueByteIterator = new NumericByteIterator(Boolean.TRUE.equals(value) ? 1 : 0);
        }
        result.put(series.getColumns().get(j), valueByteIterator);
      }
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Point.Builder pointBuilder = Point.measurement(table)
          .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
          .tag(TAG_NAME, key);

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        pointBuilder.addField(entry.getKey(), entry.getValue().toString());
      }

      influxDB.write(pointBuilder.build());
      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      influxDB.query(new Query("DROP SERIES FROM " + table + " WHERE " + TAG_NAME + "='" + key + "'"));
      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    influxDB.close();
  }
}
