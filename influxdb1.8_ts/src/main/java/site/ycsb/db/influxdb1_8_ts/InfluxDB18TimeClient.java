package site.ycsb.db.influxdb1_8_ts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Influxdb 1.8 client.
 * <p>
 * See {@code influxdb1.8/README.md} for details.
 */
public class InfluxDB18TimeClient extends TimeseriesDB {

  private static Logger log = LogManager.getLogger(InfluxDB18TimeClient.class);

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
    super.init();
    final Properties props = getProperties();
    final String url = props.getProperty("url", "http://localhost:8086");
    database = props.getProperty("database", "ycsb-ts");
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
  protected Status read(String metric, long timestamp, Map<String, List<String>> tags) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  protected Status scan(String metric, long startTs, long endTs, Map<String, List<String>> tags,
                        AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  protected Status insert(String metric, long timestamp, long value, Map<String, ByteIterator> tags) {
    return insert(metric, timestamp, (double) value, tags);
  }

  @Override
  protected Status insert(String metric, long timestamp, double value, Map<String, ByteIterator> tags) {
    try {
      if (debug) {
        log.info("Inserting into {}, timestamp: {}, double value: {}, tags: {} ", metric, timestamp, value, tags);
      }

      Map<String, String> myTags = new HashMap<>();
      for (Map.Entry<String, ByteIterator> entry : tags.entrySet()) {
        myTags.put(entry.getKey(), entry.getValue().toString());
      }

      Map<String, Object> values = new HashMap<>();
      values.put(valueKey, value);

      influxdbHelper.insert(database, rpName, metric, timestamp, TimeUnit.MILLISECONDS, myTags, values);
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
  public void cleanup() throws DBException {
    if (influxdbHelper != null) {
      influxdbHelper.close();
      influxdbHelper = null;
    }
  }
}
