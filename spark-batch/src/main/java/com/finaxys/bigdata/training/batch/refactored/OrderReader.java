package com.finaxys.bigdata.training.batch.refactored;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import java.io.Serializable;

public class OrderReader implements Serializable {

    /**
     *
     * @param jsc
     * @param filePath the file path when ever it is presented, if local or HDFS Spark will find out depending on the running context
     * @return
     */
    public JavaRDD<String> readInputFile(JavaSparkContext jsc, String filePath) {
        JavaRDD<String> rdd = jsc.textFile(filePath);
        return null;
    }

    public DataFrame toDataFrame(JavaRDD<String> rdd) {
        return null;

    }


}
