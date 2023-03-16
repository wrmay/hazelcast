package com.hazelcast.bulktransport.impl;

import com.hazelcast.internal.tpc.Op;
import com.hazelcast.internal.tpc.OpCodes;
import com.hazelcast.internal.tpc.offheapmap.OffheapMap;
import com.hazelcast.table.impl.TableManager;

public class BulkTransportOp extends Op {

    public BulkTransportOp() {
        super(OpCodes.BULK_TRANSPORT);
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);
        return COMPLETED;
    }
}
