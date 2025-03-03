package site.ycsb.db.influxdb1_8_ts;

import org.junit.Test;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.TimeSeriesWorkload;

import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

public class InfluxDB18TimeClientTest {

  private InfluxDB18TimeClient client;

  private String tableName;

  private TimeSeriesWorkload workload;

  private Object threadState;

  /**
   * Re-create the table for each test. Using default properties.
   */
  public void setUp() throws Exception {
    setUp(new Properties());
  }

  /**
   * Re-create the table for each test. Using custom properties.
   */
  public void setUp(Properties p) throws Exception {
    client = new InfluxDB18TimeClient();

    p.setProperty("debug", "true");

    Measurements.setProperties(p);
    workload = new TimeSeriesWorkload();
    workload.init(p);
    threadState = workload.initThread(p, 0, 2);

    tableName = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    client.setProperties(p);
    client.init();
  }

  @Test
  public void testInsert() throws Exception {
    setUp();

    workload.doInsert(client, threadState);

    // Verify result
  }

  @Test
  public void testDelete() throws Exception {
    setUp();
    final String key = "key";
    final Status status = client.delete(tableName, key);
    assertEquals(Status.OK, status);

    // Verify result
  }

}
