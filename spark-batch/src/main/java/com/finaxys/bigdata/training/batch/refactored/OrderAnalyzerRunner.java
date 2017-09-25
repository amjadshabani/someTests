package com.finaxys.bigdata.training.batch.refactored;

import com.finaxys.bigdata.training.batch.Order;
import com.finaxys.bigdata.training.batch.refactored.writer.AbstractWriter;
import com.finaxys.bigdata.training.batch.refactored.writer.OrcWriter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class OrderAnalyzerRunner {

    public static final Logger logger = Logger.getLogger(OrderAnalyzerRunner.class);

    ProjectConfiguration projectConfiguration = ProjectConfiguration.getInstance();

    //Here when creating the config oobject , we do not set the spark master, so it will be provided in the spark-submit
    // to run the application in the IDE we edit the Run Configuration and set the VN option -Dspark.master=local[2]
    SparkConf conf = new SparkConf().setAppName(projectConfiguration.getAppName());
    JavaSparkContext jsc = SparkHelpers.getJavaSparkContext(conf);
    SQLContext sqlContext = SparkHelpers.getSqlContext();

    // In order to use ORC format we need to use HiveContext instead of SQLContext
    // HiveContext is a superset of SQLContext, so it contains all the functionality of SQLContext and adds the Hive support to it
    // When Initializing HiveContext, there is no need to connect to hive server
    HiveContext hiveContext = SparkHelpers.getHiveContext();


    OrderReader reader = new OrderReader();
    AbstractWriter writer = new OrcWriter();

    public static void main(String[] args) {
        OrderAnalyzerRunner runner = new OrderAnalyzerRunner();
        logger.info("Application Started with the following configurtion " + runner.projectConfiguration.toString());

        runner.runAnalyzer();
    }

    public void runAnalyzer() {
        //for the sake of obviousity we keept the reader class as a helper class,
        // so the reader will see what is happening
        //In real coding, the logic of reading should be done in the reader class, from a raw file to Data frame

        logger.info("Start reading raw file");
        JavaRDD<String> orders = reader.readInputFile(jsc, projectConfiguration.getFileLocation());


        //The rdd orders will still be in use; so it is usefull for optimization to cache it
        orders.cache();


        logger.info("normalize the lines of the file into Order objects");
        JavaRDD<Order> normalizedOrders = reader.normalizeOrders(orders);

        logger.info("creating datafrrame and register it to a table");
        DataFrame ordersDF = hiveContext.createDataFrame(normalizedOrders,Order.class);


        //the table name is the same is the output table name of the writer (could be different but just to keep track of naming)
        String tableName = projectConfiguration.getOutputTable();
        ordersDF.registerTempTable(tableName);

        //Do some sql
        logger.info("Analyze the data, count per each order book");
        DataFrame results = hiveContext.sql("SELECT orderbook,COUNT(*) FROM " + tableName + " group by orderbook");

        //write data frame to a table;
        logger.info("Write the result data to ORC files");
        writer.write(results);
    }

}
