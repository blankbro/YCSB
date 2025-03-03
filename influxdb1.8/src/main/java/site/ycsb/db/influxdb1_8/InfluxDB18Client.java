package site.ycsb.db.influxdb1_8;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Influxdb 1.8 client.
 * <p>
 * See {@code influxdb1.8/README.md} for details.
 */
public class InfluxDB18Client extends site.ycsb.DB {

  private static Logger log = LogManager.getLogger(InfluxDB18Client.class);

  private static final String TAG_NAME = "tag0";

  private static final String KEY_NAME = "key0";

  private InfluxdbHelper influxdbHelper = null;

  private String database;

  private String rpName;

  private Integer replicationFactor;

  private int batchSize;

  private int batchInterval;

  private boolean debug;

  @Override
  public void init() throws DBException {
    final Properties props = getProperties();
    final String url = props.getProperty("url", "http://localhost:8086");
    database = props.getProperty("database", "ycsb");
    rpName = props.getProperty("rp_name", "autogen");
    replicationFactor = Integer.parseInt(props.getProperty("replication_factor", "1"));
    final String username = props.getProperty("username", "username");
    final String password = props.getProperty("password", "password");
    batchSize = Integer.parseInt(props.getProperty("batch_size", "5000"));
    batchInterval = Integer.parseInt(props.getProperty("batch_interval_ms", "5000"));
    debug = getProperties().getProperty("debug", "false").compareTo("true") == 0;

    if (influxdbHelper == null) {
      influxdbHelper = InfluxdbHelper.build(url, username, password);
      influxdbHelper.setDebug(debug);
      if (!influxdbHelper.databaseExists(database)) {
        influxdbHelper.createDatabase(database);
      }
      influxdbHelper.setDefaultDatabase(database);
      if (replicationFactor > 1) {
        influxdbHelper.alterReplicationFactor(database, rpName, replicationFactor);
      }

      if (batchSize > 1) {
        influxdbHelper.enableBatch(batchSize, batchInterval, TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      if (debug) {
        log.info("Reading {}.{} fields: {},", table, key, fields);
      }
      Map<String, String> where = new HashMap<>();
      where.put(TAG_NAME, TAG_NAME);
      where.put(KEY_NAME, key);
      List<Map<String, Object>> selectResult = influxdbHelper.select(database, rpName, table, fields, where);
      if (selectResult.isEmpty()) {
        return Status.NOT_FOUND;
      }
      if (selectResult.size() != 1) {
        log.error("InfluxDB query returned {} results", selectResult.size());
        return Status.ERROR;
      }
      Map<String, Object> row = selectResult.get(0);
      if (debug) {
        log.info("select result: {}", row);
      }
      objectToByteIterator(row, result);
      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  public void objectToByteIterator(Map<String, Object> row, Map<String, ByteIterator> result) {
    row.forEach(new BiConsumer<String, Object>() {
      @Override
      public void accept(String fieldName, Object value) {
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
        result.put(fieldName, valueByteIterator);
      }
    });
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      Map<String, String> where = new HashMap<>();
      where.put(TAG_NAME, TAG_NAME);
      where.put(KEY_NAME, startkey);
      List<Map<String, Object>> startResult = influxdbHelper.select(database, rpName, table, fields, where);
      if (startResult.isEmpty()) {
        return Status.NOT_FOUND;
      }
      Map<String, Object> startRow = startResult.get(0);
      String startTime = (String) startRow.get("time");

      List<Map<String, Object>> scanResult = influxdbHelper.scan(database, rpName, table, fields,
          startTime, recordcount);
      if (scanResult.isEmpty()) {
        return Status.NOT_FOUND;
      }

      scanResult.forEach(new Consumer<Map<String, Object>>() {
        @Override
        public void accept(Map<String, Object> row) {
          HashMap<String, ByteIterator> rowByte = new HashMap<>();
          objectToByteIterator(row, rowByte);
          result.add(rowByte);
        }
      });

      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (debug) {
        log.info("update {}", key);
      }
      // 1. select 这条记录的时间戳
      Map<String, String> where = new HashMap<>();
      where.put(TAG_NAME, TAG_NAME);
      where.put(KEY_NAME, key);
      List<Map<String, Object>> startResult = influxdbHelper.select(database, rpName, table, null, where);
      if (startResult.isEmpty()) {
        return Status.NOT_FOUND;
      }
      Map<String, Object> row = startResult.get(0);
      Instant time = Instant.parse((String) row.get("time"));

      // 2. 重新 insert 相同时间戳数据
      Map<String, String> tags = new HashMap<>();
      tags.put(TAG_NAME, TAG_NAME);

      Map<String, Object> fields = new HashMap<>();
      fields.put(KEY_NAME, key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fields.put(entry.getKey(), entry.getValue().toString());
      }

      influxdbHelper.insert(database, rpName, table,
          time.toEpochMilli() * 1_000_000L + time.getNano(), tags, fields);

      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Map<String, String> tags = new HashMap<>();
      tags.put(TAG_NAME, TAG_NAME);

      Map<String, Object> fields = new HashMap<>();
      fields.put(KEY_NAME, key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fields.put(entry.getKey(), entry.getValue().toString());
      }

      influxdbHelper.insert(database, rpName, table, null, tags, fields);
      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (debug) {
        log.info("Deleting {}", key);
      }
      Map<String, String> where = new HashMap<>();
      where.put(TAG_NAME, TAG_NAME);
      where.put(KEY_NAME, key);

      influxdbHelper.delete(database, rpName, table, where);
      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (influxdbHelper != null) {
      influxdbHelper.close();
      influxdbHelper = null;
    }
  }
}
