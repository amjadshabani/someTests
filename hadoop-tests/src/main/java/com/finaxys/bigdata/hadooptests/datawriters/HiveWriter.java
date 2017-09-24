package com.finaxys.bigdata.hadooptests.datawriters;

import com.finaxys.bigdata.hadooptests.helpers.MiniCluster;

import java.sql.*;

public class HiveWriter {

    public static MiniCluster minicluster = new MiniCluster.Builder().withHiveMetastore().withHiveServer().build();

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void insertIntoHive() throws SQLException {
        minicluster.start();
        // To make sure the the mini cluster is up and all the tcp channels are up
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String connectionURL = "jdbc:hive2://localhost:" + MiniCluster.HS2_PORT + "/default";

        Connection con = DriverManager.getConnection(connectionURL, "", "");
        Statement stmt = con.createStatement();
        String tableName = "dev_appl_ch1_fdr.orders_ext";
        stmt.executeQuery("drop table " + tableName);

        String createTableStatement = "CREATE TABLE dev_appl_ch1_fdr.orders_ext (M_TYPE STRING,ORDERBOOK STRING,SENDER STRING,EXTID STRING,O_TYPE CHAR(1),DIR CHAR(1),QUANTITY INT )ROW FORMAT DELIMITED FIELDS TERMINATED BY \';\'\\\\n";

        ResultSet res = stmt.executeQuery(createTableStatement);

        //let's load data into the table
        String filepath = "order_data.csv";
        String sql = "load data local inpath '" + filepath + "' into table " + tableName;

        ResultSet resultSet = stmt.executeQuery(sql);

    }

    public static void main(String[] args) {
        try {
            insertIntoHive();
        } catch (SQLException e) {
//            minicluster.stop();
            e.printStackTrace();
        }
    }

}
