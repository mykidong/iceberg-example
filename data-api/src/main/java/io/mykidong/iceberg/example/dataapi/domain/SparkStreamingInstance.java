package io.mykidong.iceberg.example.dataapi.domain;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingInstance {
    private JavaStreamingContext javaStreamingContext;
    private SparkSession spark;

    public SparkStreamingInstance(JavaStreamingContext javaStreamingContext,
                                  SparkSession spark) {
        this.javaStreamingContext = javaStreamingContext;
        this.spark = spark;
    }

    public JavaStreamingContext getJavaStreamingContext() {
        return javaStreamingContext;
    }

    public SparkSession getSpark() {
        return spark;
    }
}
