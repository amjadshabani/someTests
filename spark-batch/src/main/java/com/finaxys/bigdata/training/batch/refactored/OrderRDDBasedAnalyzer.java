package com.finaxys.bigdata.training.batch.refactored;

import com.finaxys.bigdata.training.batch.Order;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * This implementation do the very same thing done in com.finaxys.bigdata.training.batch.refactored.OrderAnalyzerRunner<br/>
 * The main difference that it uses the basic RDD API instead of benefiting of the new easy-to-use DataFrame API
 *
 */
public class OrderRDDBasedAnalyzer implements Serializable {

    ProjectConfiguration projectConfiguration = ProjectConfiguration.getInstance();

    SparkConf conf = new SparkConf().setAppName(projectConfiguration.getAppName());
    private JavaSparkContext jsc = SparkHelpers.getJavaSparkContext(conf);

    OrderReader reader = new OrderReader();

    public static void main(String[] args) {

    }

    public void analyze(){
        //Read the raw file into RDD
        JavaRDD<String> ordersRawRDD =  reader.readInputFile(jsc, projectConfiguration.getFileLocation());

        //Getting the RDD we should analyze the RDD into Order objects
        JavaRDD<Order> normalizedOrderRDD = ordersRawRDD.map(line -> new Order().toOrder(line));

        //Let' get the key of grouping by statement, in our case it is orderbook
        //The results of the following transformation is a tuple of <ORDERBOOK, 1>
        JavaPairRDD<String, Integer> keyedOrder = normalizedOrderRDD.mapToPair(new PairFunction<Order, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Order order) throws Exception {
                return new Tuple2<String, Integer>(order.getOrderbook(), Integer.valueOf(1));
            }
        });
        //We could use Lambda expression in the previous transformation.
        // But for the sake of demonstration, we chose to go with the Function2 interface
        // We can do that in Lambda expression as the following:
//        JavaPairRDD<String, Integer> test =
//                normalizedOrderRDD.mapToPair(order ->
//                {return new Tuple2<String, Integer>(order.getOrderbook(),Integer.valueOf(1));});


        //The following transformation is to reduceByKey, that means that all the same key (orderbook),
        //it will sum the occurrunce in the file of this key

        JavaPairRDD<String, Integer> countPerGroup = keyedOrder.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });


    }

}
