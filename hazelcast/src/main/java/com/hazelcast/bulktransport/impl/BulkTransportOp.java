package com.hazelcast.bulktransport.impl;

import com.hazelcast.internal.alto.offheapmap.OffheapMap;
import com.hazelcast.internal.alto.runtime.FrameCodec;
import com.hazelcast.internal.alto.runtime.Op;
import com.hazelcast.internal.alto.runtime.OpCodes;
import com.hazelcast.table.impl.TableManager;

import static com.hazelcast.internal.alto.runtime.FrameCodec.OFFSET_REQ_CALL_ID;

public class BulkTransportOp extends Op {

    public BulkTransportOp() {
        super(OpCodes.BULK_TRANSPORT);
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        FrameCodec.writeResponseHeader(response, partitionId, request.getLong(OFFSET_REQ_CALL_ID));
        FrameCodec.constructComplete(response);

        return COMPLETED;
    }
}
