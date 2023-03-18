package com.hazelcast.bulktransport.impl;

import com.hazelcast.internal.tpc.member.Op;
import com.hazelcast.internal.tpc.member.OpCodes;
import com.hazelcast.internal.tpc.offheapmap.OffheapMap;
import com.hazelcast.htable.impl.HTableManager;

public class BulkTransportOp extends Op {

    public BulkTransportOp() {
        super(OpCodes.BULK_TRANSPORT);
    }

    @Override
    public int run() throws Exception {
        HTableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);
        return COMPLETED;
    }
}
