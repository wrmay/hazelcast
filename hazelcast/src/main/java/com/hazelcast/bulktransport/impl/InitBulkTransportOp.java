package com.hazelcast.bulktransport.impl;

import com.hazelcast.internal.tpc.offheapmap.OffheapMap;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.Op;
import com.hazelcast.internal.tpc.OpCodes;
import com.hazelcast.table.impl.TableManager;

import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_REQ_CALL_ID;

public class InitBulkTransportOp extends Op {

    public InitBulkTransportOp() {
        super(OpCodes.INIT_BULK_TRANSPORT);
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        return COMPLETED;
    }
}
