package com.hazelcast.internal.tpc.client;

import com.hazelcast.internal.tpc.RequestFuture;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

public class ClientTpcRuntime implements TpcRuntime {

    @Override
    public int getRequestTimeoutMs() {
        return 0;
    }

    @Override
    public int getPartitionCount() {
        return 0;
    }

    @Override
    public RequestFuture<IOBuffer> invoke(IOBuffer request, int partitionId) {
        return null;
    }
}
