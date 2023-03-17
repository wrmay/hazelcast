package com.hazelcast.bulktransport.impl;

import com.hazelcast.internal.tpc.offheapmap.OffheapMap;
import com.hazelcast.internal.tpc.Op;
import com.hazelcast.internal.tpc.OpCodes;
import com.hazelcast.htable.impl.HTableManager;

public class InitBulkTransportOp extends Op {

    public InitBulkTransportOp() {
        super(OpCodes.INIT_BULK_TRANSPORT);
    }

    @Override
    public int run() throws Exception {
        HTableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        return COMPLETED;
    }
}
