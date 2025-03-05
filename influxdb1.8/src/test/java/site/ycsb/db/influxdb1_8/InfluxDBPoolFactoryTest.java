package site.ycsb.db.influxdb1_8;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.influxdb.InfluxDB;
import org.junit.Test;

public class InfluxDBPoolFactoryTest {

  @Test
  public void test() throws Exception {
    InfluxDBPoolFactory poolFactory = new InfluxDBPoolFactory("http://localhost:8086", "admin", "admin");
    GenericObjectPool<InfluxDB> pool = poolFactory.newPool();
    InfluxDB influxDB = pool.borrowObject();
    System.out.println(influxDB.ping().isGood());
  }
}
