package site.ycsb.db.influxdb1_8;

import org.junit.Test;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

public class InfluxDB18ClientTest {

  private InfluxDB18Client client;

  private String tableName;

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
    client = new InfluxDB18Client();

    p.setProperty("influxdb.user", "admin");
    p.setProperty("influxdb.password", "admin");

    Measurements.setProperties(p);
    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    tableName = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    client.setProperties(p);
    client.init();
  }

  @Test
  public void testInsert() throws Exception {
    setUp();
    final String key = "key";
    final HashMap<String, String> input = new HashMap<String, String>();
    input.put("column1", "value1");
    input.put("column2", "value2");
    final Status status = client.insert(tableName, key, StringByteIterator.getByteIteratorMap(input));
    assertEquals(Status.OK, status);

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
