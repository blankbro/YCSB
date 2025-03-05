package site.ycsb.db.influxdb1_8;

import okhttp3.*;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.ssl.SSLContexts;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.*;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class InfluxDBPoolFactory extends BasePooledObjectFactory<InfluxDB> {

  private String url;
  private String username;
  private String password;
  private int actions;
  private int flushDuration;
  private TimeUnit flushDurationTimeUnit;

  public InfluxDBPoolFactory(String url, String username, String password) {
    this.url = url;
    this.username = username;
    this.password = password;
  }

  public InfluxDBPoolFactory enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit) {
    this.actions = actions;
    this.flushDuration = flushDuration;
    this.flushDurationTimeUnit = flushDurationTimeUnit;
    return this;
  }

  @Override
  public InfluxDB create() throws Exception {
    OkHttpClient.Builder client = new OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .readTimeout(10, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .addNetworkInterceptor(new Interceptor() {
          @NotNull
          @Override
          public Response intercept(@NotNull Chain chain) throws IOException {
            Request newRequest = chain.request().newBuilder().header("Connection", "close").build();
            return chain.proceed(newRequest);
          }
        })
        .sslSocketFactory(defaultSslSocketFactory(), defaultTrustManager())
        .hostnameVerifier(noopHostnameVerifier())
        // 超过阈值的idle连接会由连接池关闭，关闭后sockets进入TIME_WAIT状态等待x系统回收，该参数需根据实际连接数适当调整
        .connectionPool(new ConnectionPool(5, 30, TimeUnit.SECONDS));

    InfluxDB influxDB = InfluxDBFactory.connect(url, username, password, client);
    if (this.actions > 0 && this.flushDuration > 0) {
      influxDB.enableBatch(actions, flushDuration, flushDurationTimeUnit);
    }
    return influxDB;
  }

  @Override
  public PooledObject<InfluxDB> wrap(InfluxDB influxDB) {
    return new DefaultPooledObject<>(influxDB);
  }

  @Override
  public void destroyObject(PooledObject<InfluxDB> p) throws Exception {
    p.getObject().close();
  }

  @Override
  public boolean validateObject(PooledObject<InfluxDB> p) {
    return p.getObject().ping().isGood();
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

  public GenericObjectPool<InfluxDB> newPool() {
    GenericObjectPoolConfig<InfluxDB> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(100);
    config.setMinIdle(20);
    config.setMaxWait(Duration.ofMillis(30000));
    config.setTestOnBorrow(true);
    config.setJmxNamePrefix("influx-pool");
    config.setJmxEnabled(false);
    return new GenericObjectPool<>(this, config);
  }

}
