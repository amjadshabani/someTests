package com.finaxys.bigdata.hadooptests.helpers;

import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class MiniClusterTest extends TestCase {
    @org.junit.Test
    public void start() throws Exception {
    }

    @Test
    public void testHBase () throws Exception{
        HbaseLocalCluster hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(25111)
                .setHbaseMasterInfoPort(-1)
                .setNumRegionServers(1)
                .setHbaseRootDir("embedded_hbase")
                .setZookeeperPort(12345)
                .setZookeeperConnectionString("localhost:54321")
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
        hbaseLocalCluster.start();
        hbaseLocalCluster.stop(true);
    }

}