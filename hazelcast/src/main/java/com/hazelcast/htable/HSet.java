package com.hazelcast.htable;

import com.hazelcast.core.Command;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.PartitionActorRef;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpc.OpCodes.SET;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HSet implements Command {

    private final TpcRuntime tpcRuntime;
    private final String name;
    private final int partitionCount;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;
    private final PartitionActorRef[] partitionActorRefs;

    public HSet(TpcRuntime tpcRuntime, String name) {
        this.name = name;
        this.tpcRuntime = tpcRuntime;
        this.partitionCount = tpcRuntime.getPartitionCount();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
        this.partitionActorRefs = tpcRuntime.partitionActorRefs();
    }

    public void sync(byte[] key, byte[] value){
        int partitionId = hashToIndex(Arrays.hashCode(key), partitionCount);

        IOBuffer request = requestAllocator.allocate(60);
        FrameCodec.writeRequestHeader(request, partitionId, SET);
        request.writeSizedBytes(key);
        request.writeSizedBytes(value);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> f = partitionActorRefs[partitionId].submit(request);
        try {
            IOBuffer response = f.get(requestTimeoutMs, MILLISECONDS);
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void pipeline(Pipeline pipeline, byte key, byte[] value){

    }
}
