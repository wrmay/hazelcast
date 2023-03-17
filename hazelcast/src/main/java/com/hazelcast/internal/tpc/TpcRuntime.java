package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpcengine.AsyncSocket;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

public interface TpcRuntime {

    int getRequestTimeoutMs();

    int getPartitionCount();

    PartitionActorRef[] partitionActorRefs();

    RequestFuture invoke(IOBuffer request, AsyncSocket socket);
}
