package com.finaxys.bigdata.training.batch.refactored;

import com.finaxys.bigdata.training.batch.Order;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class OrderReader implements Serializable {

    /**
     * @param jsc
     * @param filePath the file path when ever it is presented, if local or HDFS Spark will find out depending on the running context
     * @return
     */
    public JavaRDD<String> readInputFile(JavaSparkContext jsc, String filePath) {
        JavaRDD<String> rdd = jsc.textFile(filePath);
        return rdd;
    }

    /**
     * Normalize data into RDD of orders
     *
     * @param ordersRDD
     * @return
     */
    public JavaRDD<Order> normalizeOrders(JavaRDD<String> ordersRDD) {
        JavaRDD<Order> normalizedOrderRDD = ordersRDD.map(new Function<String, Order>() {
            public Order call(String s) throws Exception {
                Order order = new Order();
                return order.toOrder(s);
            }
        });
        return normalizedOrderRDD;
    }

}
