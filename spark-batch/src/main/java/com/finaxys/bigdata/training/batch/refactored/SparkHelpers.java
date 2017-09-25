package com.finaxys.bigdata.training.batch.refactored;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Properties;

/**
 * Hepler classes to get the contexts from
 */
public class SparkHelpers {
    private transient static JavaSparkContext jsc;
    private transient static SQLContext sqlContext;
    private transient static HiveContext hiveContext;

    public static synchronized JavaSparkContext getJavaSparkContext(SparkConf conf) {
        if (jsc == null) {
            jsc = new JavaSparkContext(conf);
            return jsc;
        }
        return jsc;
    }

    public static synchronized SQLContext getSqlContext(SparkConf conf) {
        if (sqlContext == null)
            sqlContext = new SQLContext(getJavaSparkContext(conf));
        return sqlContext;
    }

    public static SQLContext getSqlContext(JavaSparkContext jsc) {
        if (sqlContext == null)
            sqlContext = new SQLContext(jsc);
        return sqlContext;
    }

    /**
     * If we need to get the context form the already created spark context
     *
     * @return
     */
    public static SQLContext getSqlContext() {
        if (jsc == null)
            throw new IllegalArgumentException("Spark context is not initialized yet, you have to initialize it before using this method");
        if (sqlContext == null)
            sqlContext = new SQLContext(jsc);
        return sqlContext;
    }

    // In case we want to pass the spark configuration from another file than the defaul spark.defaults,
    // we can spicify some system properties or properties file and loaded into Spark conf object
    public static void configureSparkContext(Properties properties) {

    }

    public static HiveContext getHiveContext() {
        if (jsc == null)
            throw new IllegalArgumentException("Spark context is not initialized yet, you have to initialize it before using this method");
        if (hiveContext == null)
            hiveContext = new HiveContext(jsc.sc());
        return hiveContext;
    }
}
