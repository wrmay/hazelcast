package com.hazelcast.bulktransport;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.htable.HTable;


public class BulkTransportMain {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        HTable table = node1.getProxy(HTable.class , "table");

        BulkTransport bulkTransport = null;
        //table.newBulkTransport(node2.getCluster().getLocalMember().getAddress(), 10);
        bulkTransport.copyFile(null);
        bulkTransport.close();
    }
}
