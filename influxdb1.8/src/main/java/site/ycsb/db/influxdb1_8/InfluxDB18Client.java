package site.ycsb.db.influxdb1_8;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Influxdb 1.8 client.
 * <p>
 * See {@code influxdb1.8/README.md} for details.
 */
public class InfluxDB18Client extends site.ycsb.DB {

  private static final AtomicInteger THREAD_NUM = new AtomicInteger(0);

  private static final Logger LOG = LogManager.getLogger(InfluxDB18Client.class);

  private static final String TAG_NAME = "tag0";

  private int recordCount;

  private int tagValueCount;

  private InfluxdbHelper influxdbHelper;

  private String database;

  private String rpName;

  private boolean debug;

  private long startTimestampMs;

  private long nextTimestampMs;

  private long totalDataIntervalMs;

  @Override
  public void init() throws DBException {
    final Properties props = getProperties();
    // 所有线程数
    int threadCount = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY));
    // 当前线程编号
    int threadNum = THREAD_NUM.getAndIncrement();

    // influxdb 连接信息
    final String url = props.getProperty("url", "http://localhost:8086");
    this.database = props.getProperty("database", "ycsb");
    this.rpName = props.getProperty("rp_name", "autogen");
    int replicationFactor = Integer.parseInt(props.getProperty("replication_factor", "1"));
    final String username = props.getProperty("username", "username");
    final String password = props.getProperty("password", "password");
    // 记录数
    this.recordCount = Integer.parseInt(props.getProperty(Client.RECORD_COUNT_PROPERTY));
    // 控制批量写入参数
    int batchSize = Integer.parseInt(props.getProperty("batch_size", "5000"));
    int batchInterval = Integer.parseInt(props.getProperty("batch_interval_ms", "5000"));
    // 查询类型是否为 scan，
    // 为了尽可能把单点查询和范围查询效率提到最大，在数据写入时，需要有不同策略
    // tag value count 只有在 select type 为 scan 时，才有用
    this.tagValueCount = Integer.parseInt(props.getProperty("tagValueCount", "-1"));
    this.debug = getProperties().getProperty("debug", "false").compareTo("true") == 0;
    // 计算记录之间的时间间隔
    long dataIntervalMs = Long.parseLong(props.getProperty("data_interval_ms", "1"));
    if (dataIntervalMs <= 0L) {
      dataIntervalMs = 1L;
    }
    this.totalDataIntervalMs = dataIntervalMs * threadCount;
    this.startTimestampMs = Long.parseLong(props.getProperty("start_timestamp_ms", System.currentTimeMillis() + ""));
    // 获取下一个时间戳
    this.nextTimestampMs = startTimestampMs + (threadNum * dataIntervalMs);

    if (this.influxdbHelper == null) {
      this.influxdbHelper = InfluxdbHelper.build(url, username, password);
      this.influxdbHelper.setDebug(this.debug);
      if (!this.influxdbHelper.databaseExists(this.database)) {
        this.influxdbHelper.createDatabase(this.database);
      }
      this.influxdbHelper.setDefaultDatabase(this.database);
      if (replicationFactor > 1) {
        this.influxdbHelper.alterReplicationFactor(this.database, this.rpName, replicationFactor);
      }

      if (batchSize > 1) {
        this.influxdbHelper.enableBatch(batchSize, batchInterval, TimeUnit.MILLISECONDS);
      }
    }
  }

  private long getNextTimestampMsAndIncrement() {
    long result = nextTimestampMs;
    this.nextTimestampMs += this.totalDataIntervalMs;
    return result;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      if (this.tagValueCount > 0) {
        LOG.info("当前测试数据是专门测试 scan 用的，终止 read");
        return Status.NOT_IMPLEMENTED;
      }
      if (debug) {
        LOG.info("Reading {}.{} fields: {},", table, key, fields);
      }
      Map<String, String> where = getTags(key);
      List<Map<String, Object>> selectResult = influxdbHelper.select(database, rpName, table, fields, where);
      if (selectResult.isEmpty()) {
        return Status.NOT_FOUND;
      }
      if (selectResult.size() != 1) {
        LOG.error("InfluxDB query returned {} results", selectResult.size());
        return Status.ERROR;
      }
      Map<String, Object> row = selectResult.get(0);
      if (debug) {
        LOG.info("select result: {}", row);
      }
      objectToByteIterator(row, result);
      return Status.OK;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
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
      if (this.tagValueCount <= 0) {
        LOG.info("当前测试数据是专门测试 read 用的，终止 scan");
        return Status.NOT_IMPLEMENTED;
      }

      Map<String, String> tags = getTags(startkey);
      long startTime = this.startTimestampMs + new Random().nextInt(recordCount) * totalDataIntervalMs;
      long endTime = startTime + recordcount * totalDataIntervalMs;

      List<Map<String, Object>> scanResult = influxdbHelper.scan(database, rpName, table, fields,
              tags, startTime, endTime);

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
      LOG.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // 对于时序数据库来说，update 其实也就是一条 insert，此处直接使用 insert
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Map<String, String> tags = getTags(key);

      Map<String, Object> fields = new HashMap<>();
      if (!key.equals(tags.get(TAG_NAME))) {
        fields.put("key0", key);
      }
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fields.put(entry.getKey(), entry.getValue().toString());
      }

      influxdbHelper.insert(database, rpName, table,
              getNextTimestampMsAndIncrement(), TimeUnit.MILLISECONDS,
              tags, fields);
      return Status.OK;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private Map<String, String> getTags(String key) {
    Map<String, String> tags = new HashMap<>();
    if (this.tagValueCount > 0) {
      tags.put(TAG_NAME, getTag(key));
    } else {
      tags.put(TAG_NAME, key);
    }
    return tags;
  }

  private String getTag(String key) {
    return "tag" + (key.hashCode() % this.tagValueCount);
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public void cleanup() throws DBException {
    if (influxdbHelper != null) {
      influxdbHelper.close();
      influxdbHelper = null;
    }
  }
}
