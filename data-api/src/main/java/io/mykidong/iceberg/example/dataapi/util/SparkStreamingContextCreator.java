package io.mykidong.iceberg.example.dataapi.util;

import io.mykidong.iceberg.example.dataapi.domain.SparkStreamingInstance;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SparkStreamingContextCreator {

    private static Logger LOG = LoggerFactory.getLogger(SparkStreamingContextCreator.class);

    private static SparkSession spark;

    public static ConcurrentMap<String, SparkStreamingInstance> streamingContextMap;
    private static final Object lock = new Object();

    public static SparkStreamingInstance singleton(String user, String metastoreUrl, String bucket, String accessKey, String secretKey, String endpoint) {
        if(streamingContextMap == null) {
            synchronized(lock) {
                if(streamingContextMap == null) {
                    streamingContextMap = new ConcurrentHashMap<>();
                    SparkStreamingInstance streamingContext = streamingContext(metastoreUrl, bucket, accessKey, secretKey, endpoint);
                    LOG.info("streaming context created for the user [{}]", user);
                    streamingContextMap.put(user, streamingContext);
                }
            }
        }
        else
        {
            synchronized(lock) {
                if (!streamingContextMap.containsKey(user)) {
                    SparkStreamingInstance streamingContext = streamingContext(metastoreUrl, bucket, accessKey, secretKey, endpoint);
                    LOG.info("streaming context created for the user [{}]", user);
                    streamingContextMap.put(user, streamingContext);
                }
            }
        }

        return streamingContextMap.get(user);
    }

    private SparkStreamingContextCreator() {
    }



    private static SparkStreamingInstance streamingContext(String metastoreUrl, String bucket, String accessKey, String secretKey, String endpoint) {
        SparkConf sparkConf = new SparkConf().setAppName("disruptor streaming context");
        sparkConf.setMaster("local[2]");

        SparkSession sparkSession = (spark == null) ? SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate() : spark;

        spark = sparkSession;

        // iceberg catalog from hive metastore.
        sparkConf = sparkSession.sparkContext().conf();
        sparkConf.set("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.hive_prod.type", "hive");
        sparkConf.set("spark.sql.catalog.hive_prod.uri", "thrift://" + metastoreUrl);
        sparkConf.set("spark.sql.catalog.hive_prod.warehouse", "s3a://" + bucket + "/warehouse");


        JavaStreamingContext scc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(1000));

        org.apache.hadoop.conf.Configuration hadoopConfiguration = scc.sparkContext().hadoopConfiguration();
        hadoopConfiguration.set("fs.defaultFS", "s3a://" + bucket);
        hadoopConfiguration.set("fs.s3a.endpoint", endpoint);
        hadoopConfiguration.set("fs.s3a.access.key", accessKey);
        hadoopConfiguration.set("fs.s3a.secret.key", secretKey);
        hadoopConfiguration.set("fs.s3a.path.style.access", "true");
        hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopConfiguration.set("hive.metastore.uris", "thrift://" + metastoreUrl);
        hadoopConfiguration.set("hive.server2.enable.doAs", "false");
        hadoopConfiguration.set("hive.metastore.client.socket.timeout", "1800");
        hadoopConfiguration.set("hive.execution.engine", "spark");

        return new SparkStreamingInstance(scc, sparkSession);
    }
}
