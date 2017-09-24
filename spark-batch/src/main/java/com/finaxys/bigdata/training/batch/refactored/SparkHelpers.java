package com.finaxys.bigdata.training.batch.refactored;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Properties;

/**
 * Hepler classes to get the contexts from
 */
public class SparkHelpers {
    transient static JavaSparkContext jsc;

    public static synchronized JavaSparkContext getJavaSparkContext(SparkConf conf) {
        if (jsc == null) {
            jsc = new JavaSparkContext(conf);
            return jsc;
        }
        return jsc;
    }

    public static void configureSparkContext(Properties properties) {

    }
}
