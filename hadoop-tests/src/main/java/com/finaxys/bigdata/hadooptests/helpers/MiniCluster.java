package com.finaxys.bigdata.hadooptests.helpers;

import com.github.sakserv.minicluster.impl.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;


public class MiniCluster {

    //HDFS Global values
    public static final Integer NAMENODE_PORT = 12345;
    public static final Integer NAMENODE_HTTP_PORT = 12341;
    //Yarn global values
    public static final String RM_ADDRESS = "localhost:37001";
    public static final String RM_HOSTNAME = "localhost";
    public static final String RM_SCHEDULER_ADDRESS = "localhost:37002";
    public static final String RM_RESSOURCE_TRACKER_ADDRESS = "localhost:37003";
    public static final String RM_WEBAPP_ADDRESS = "localhost:37004";
    //HiveServer global values
    public static final Integer HS2_PORT = 12348;
    public static final Integer HIVE_METASTORE_PORT = 12347;
    //Zookeeper global values
    public static final String ZK_CONNECTION_STRING = "localhost:54321";
    public static final Integer ZK_PORT = 54321;
    public static Logger logger = Logger.getLogger(MiniCluster.class);
    // just used internally in the class:
    // to register the configured services
    private static SortedMap<Integer, com.github.sakserv.minicluster.MiniCluster> registeredServices =
            new TreeMap<Integer, com.github.sakserv.minicluster.MiniCluster>();
    // let's define all the clusters that could be used in our tests
    private HdfsLocalCluster hdfsLocalCluster;
    private HiveLocalServer2 hiveLocalServer2;
    private HiveLocalMetaStore hiveLocalMetaStore;
    private HbaseLocalCluster hbaseLocalCluster;
    private YarnLocalCluster yarnLocalCluster;
    private KafkaLocalBroker kafkaLocalBroker;

    public MiniCluster(Builder builder) {
        this.hdfsLocalCluster = builder.hdfsLocalCluster;
        this.hiveLocalServer2 = builder.hiveLocalServer2;
        this.hiveLocalMetaStore = builder.hiveLocalMetaStore;
        this.hbaseLocalCluster = builder.hbaseLocalCluster;
        this.yarnLocalCluster = builder.yarnLocalCluster;
        this.kafkaLocalBroker = builder.kafkaLocalBroker;
    }

    public HdfsLocalCluster getHdfsLocalCluster() {
        return hdfsLocalCluster;
    }

    public HiveLocalServer2 getHiveLocalServer2() {
        return hiveLocalServer2;
    }

    public HiveLocalMetaStore getHiveLocalMetaStore() {
        return hiveLocalMetaStore;
    }

    public HbaseLocalCluster getHbaseLocalCluster() {
        return hbaseLocalCluster;
    }

    public YarnLocalCluster getYarnLocalCluster() {
        return yarnLocalCluster;
    }

    public KafkaLocalBroker getKafkaLocalBroker() {
        return kafkaLocalBroker;
    }

    /**
     * Starting the defined instances, if the instance is not instantiated it will not be started
     */
    public void start() {
        logger.info("Starting the services in minicluster");
        //Make sure that there is registered services
        if (registeredServices.entrySet().size() == 0)
            throw new IllegalArgumentException("There is no registered services. There should be at least one registered serrvice for minicluster to start");

        //The order of starting services:
        // 1. HDFS
        // 2. Zookeeper
        // 3. Yarn
        // 4. HiveMetastore
        // 5. HiveServer
        // 6. Kafka
        // 7. HBase
        for (SortedMap.Entry<Integer, com.github.sakserv.minicluster.MiniCluster> entry : registeredServices.entrySet()) {
            try {
                entry.getValue().start();
            } catch (Exception e) {
                logger.error("Some services won't start, check the stack trace");

            }
        }
    }

    public void stop() {
        for (SortedMap.Entry<Integer, com.github.sakserv.minicluster.MiniCluster> entry : registeredServices.entrySet()) {
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                logger.error("Some services couldn' be stopped gracefully, check the stack trace. Please try to stop them");

            }
        }

    }

    public static class Builder {

        private HdfsLocalCluster hdfsLocalCluster;
        private HiveLocalServer2 hiveLocalServer2;
        private HiveLocalMetaStore hiveLocalMetaStore;
        private HbaseLocalCluster hbaseLocalCluster;
        private YarnLocalCluster yarnLocalCluster;
        private KafkaLocalBroker kafkaLocalBroker;

        private ZookeeperLocalCluster zookeeperLocalCluster;

        private synchronized ZookeeperLocalCluster getZookeeperLocalClusterInstance() {
            if (this.zookeeperLocalCluster == null) {
                zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                        .setPort(ZK_PORT)
                        .setTempDir("embedded_zookeeper")
                        .setZookeeperConnectionString(ZK_CONNECTION_STRING)
                        .setMaxClientCnxns(60)
                        .setElectionPort(20001)
                        .setQuorumPort(20002)
                        .setDeleteDataDirectoryOnClose(false)
                        .setServerId(1)
                        .setTickTime(2000)
                        .build();
                registerService(zookeeperLocalCluster, 2);
                return zookeeperLocalCluster;
            }
            return this.zookeeperLocalCluster;
        }


        //add hdfsLocalCluster . This is required.
        public Builder withHDFS() {
            //First get ZK instance. So it will be run before runnning this mini cluster
            getZookeeperLocalClusterInstance();
            //set the hdfsLocalCluster instance
            hdfsLocalCluster = new HdfsLocalCluster.Builder()
                    .setHdfsNamenodePort(NAMENODE_PORT)
                    .setHdfsNamenodeHttpPort(NAMENODE_HTTP_PORT)
                    .setHdfsNumDatanodes(1)
                    .setHdfsTempDir("embedded_hdfs")
                    .setHdfsConfig(new Configuration())
                    .setHdfsEnablePermissions(false)
                    .setHdfsFormat(true)
                    .setHdfsEnableRunningUserAsProxyUser(true)
                    .build();
            registerService(hdfsLocalCluster, 1);

            return this;
        }

        public Builder withYarn() {
            //set the yarnLocalCluster
            yarnLocalCluster = new YarnLocalCluster.Builder().setNumNodeManagers(1)
                    .setNumLocalDirs(1)
                    .setNumLogDirs(1)
                    .setResourceManagerAddress(RM_ADDRESS)
                    .setResourceManagerHostname(RM_HOSTNAME)
                    .setResourceManagerSchedulerAddress(RM_SCHEDULER_ADDRESS)
                    .setResourceManagerResourceTrackerAddress(RM_RESSOURCE_TRACKER_ADDRESS)
                    .setResourceManagerWebappAddress(RM_WEBAPP_ADDRESS)
                    .setUseInJvmContainerExecutor(false)
                    .setConfig(new Configuration())
                    .build();
            registerService(yarnLocalCluster, 3);
            return this;

        }

        public Builder withHiveMetastore() {
            hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                    .setHiveMetastoreHostname("localhost")
                    .setHiveMetastorePort(12347)
                    .setHiveMetastoreDerbyDbDir("metastore_db")
                    .setHiveScratchDir("hive_scratch_dir")
                    .setHiveWarehouseDir("warehouse_dir")
                    .setHiveConf(new HiveConf())
                    .build();
            registerService(hiveLocalMetaStore, 4);
            return this;
        }

        public Builder withHiveServer() {
            // set hiveserverLocalcluster instance
            // This instance require HiveMetastore service to be set. So we should check that it is not null value
            if (this.hiveLocalMetaStore == null)
                throw new IllegalArgumentException("HiveServer require Metastore service to be started first." +
                        " Instantiate one by using .withMetastore method");
            // register ZK service if not already been registered
            getZookeeperLocalClusterInstance();

            hiveLocalServer2 = new HiveLocalServer2.Builder()
                    .setHiveServer2Hostname("localhost")
                    .setHiveServer2Port(HS2_PORT)
                    .setHiveMetastoreHostname("localhost")
                    .setHiveMetastorePort(HIVE_METASTORE_PORT)
                    .setHiveMetastoreDerbyDbDir("metastore_db")
                    .setHiveScratchDir("hive_scratch_dir")
                    .setHiveWarehouseDir("warehouse_dir")
                    .setHiveConf(new HiveConf())
                    .setZookeeperConnectionString(ZK_CONNECTION_STRING)
                    .build();
            registerService(hiveLocalServer2, 5);
            return this;
        }

        public Builder withKafka() {
            getZookeeperLocalClusterInstance();
            kafkaLocalBroker = new KafkaLocalBroker.Builder()
                    .setKafkaHostname("localhost")
                    .setKafkaPort(11111)
                    .setKafkaBrokerId(0)
                    .setKafkaProperties(new Properties())
                    .setKafkaTempDir("embedded_kafka")
                    .setZookeeperConnectionString(ZK_CONNECTION_STRING)
                    .build();
            registerService(kafkaLocalBroker, 6);

            return this;
        }

        public Builder withHbase() {
            getZookeeperLocalClusterInstance();
            hbaseLocalCluster = new HbaseLocalCluster.Builder()
                    .setHbaseMasterPort(25111)
                    .setHbaseMasterInfoPort(-1)
                    .setNumRegionServers(1)
                    .setHbaseRootDir("embedded_hbase")
                    .setZookeeperPort(12345)
                    .setZookeeperConnectionString(ZK_CONNECTION_STRING)
                    .setZookeeperZnodeParent("/hbase-unsecure")
                    .setHbaseWalReplicationEnabled(false)
                    .setHbaseConfiguration(new Configuration())
                    .activeRestGateway()
                    .setHbaseRestHost("localhost")
                    .setHbaseRestPort(28000)
                    .setHbaseRestReadOnly(false)
                    .setHbaseRestThreadMax(100)
                    .setHbaseRestThreadMin(2)
                    .build()
                    .build();
            registerService(hbaseLocalCluster, 7);
            return this;

        }

        public MiniCluster build() {
            MiniCluster miniCluster = new MiniCluster(this);
            return miniCluster;

        }

        private void registerService(com.github.sakserv.minicluster.MiniCluster service, Integer priority) {
            if (service == null)
                throw new IllegalArgumentException("Cannot register service. It has not been instantiated");
            registeredServices.put(priority, service);
        }

    }

}
