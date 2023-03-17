package com.hazelcast.noop;

import com.hazelcast.htable.Pipeline;
import com.hazelcast.htable.impl.PipelineImpl;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.OpCodes;
import com.hazelcast.internal.tpc.PartitionActorRef;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpc.OpCodes.NOOP;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Nop {
    private final TpcRuntime tpcRuntime;
    private final int partitionCount;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;
    private final PartitionActorRef[] partitionActorRefs;

    public Nop(NodeEngineImpl nodeEngine) {
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.tpcRuntime = nodeEngine.getNode().getTpcRuntime();
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
        this.partitionActorRefs = tpcRuntime.partitionActorRefs();
    }

    public void execute(int partitionId) {
        CompletableFuture<IOBuffer> f = executeAsync(partitionId);
        try {
            IOBuffer response = f.get(requestTimeoutMs, MILLISECONDS);
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void pipeline(Pipeline p, int partitionId){
        PipelineImpl pipeline = (PipelineImpl)p;

        pipeline.init(partitionId);

        int sizePos = pipeline.request.position();
        // size placeholder
        pipeline.request.writeInt(0);
        // opcode
        pipeline.request.writeInt(OpCodes.NOOP);
        // set the size.
        pipeline.request.putInt(sizePos, pipeline.request.position() - sizePos);

        pipeline.count++;
    }

    public CompletableFuture<IOBuffer> executeAsync(int partitionId) {
        //  ConcurrentIOBufferAllocator allocator = new ConcurrentIOBufferAllocator(1,true);
        IOBuffer request = requestAllocator.allocate(32);
        //   request.trackRelease=true;
        FrameCodec.writeRequestHeader(request, partitionId, NOOP);
        FrameCodec.setSize(request);
        return partitionActorRefs[partitionId].submit(request);
    }
}
