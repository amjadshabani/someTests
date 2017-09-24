package com.finaxys.bigdata.training.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.List;

public class OrderAnalyzerRunner implements Serializable {

    //Spark context should not be serializable.
    //It should always be defined in the driver after that the driver will create the executors with this context\
    //It does not make anysense to copy the context to the executors,
    // but if we want to copy variables on all executors we can use broadcasting
    transient static JavaSparkContext ctxt;

    public static void main(String[] args) {
        // wrong configuration since we hard coded the master. Master should be left to the phase when we run the application
        SparkConf conf = new SparkConf().setAppName("SparkWithDataFrame")
                .setMaster("local[*]");

        ctxt = new JavaSparkContext(conf);

        //Creating an RDD from a text file. This RDD will contains String data
        JavaRDD<String> orders = ctxt.textFile("spark-batch/src/main/resources/order_data.csv");

        SQLContext sqlContext = new SQLContext(ctxt);

        JavaRDD<Order> normalizedOrder = orders.map(new Function<String, Order>() {
            public Order call(String s) throws Exception {
                Order order = new Order();
                return order.toOrder(s);
            }
        });


        //Creating Dataframe from the normalizedOrder RDD, and with the schema acquired form the Java Bean Order
        DataFrame ordersDF = sqlContext.createDataFrame(normalizedOrder, Order.class);

        //Register this schema as a table, so we can do some SQL on this table.
        ordersDF.registerTempTable("orders");

        //Doing some analyzing is as easy as calling Sql on the recently added table
        // The results of Sql queries applied to a DataFrame is saved as DataFrame.
        DataFrame googOrders = sqlContext.sql("SELECT * FROM orders WHERE orderbook=\"GOOG\" ");
        List<Object> results = googOrders.javaRDD().map(new Function<Row, Object>() {
            public Object call(Row row) throws Exception {
                return row;
            }
        }).collect();
        // In the above piece of code we're collecting all the results of RDD. in a small data set it is ok,
        // but when working with big data we should consider that this operation will collect a lot of data from all the partitions
        results.forEach(System.out::println);

    }
}
