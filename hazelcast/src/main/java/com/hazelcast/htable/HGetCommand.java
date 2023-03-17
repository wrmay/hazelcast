package com.hazelcast.htable;

import com.hazelcast.htable.impl.PipelineImpl;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.OpCodes;
import com.hazelcast.internal.tpc.PartitionActorRef;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpc.OpCodes.GET;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HGetCommand {
    private final TpcRuntime tpcRuntime;
    private final String name;
    private final int partitionCount;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;
    private final PartitionActorRef[] partitionActorRefs;

    public HGetCommand(NodeEngineImpl nodeEngine, String name) {
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.tpcRuntime = nodeEngine.getNode().getTpcRuntime();
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
        this.partitionActorRefs = tpcRuntime.partitionActorRefs();
    }

    public byte[] execute(byte[] key) {
        int partitionId = hashToIndex(Arrays.hashCode(key), partitionCount);
        IOBuffer request = requestAllocator.allocate(60);
        FrameCodec.writeRequestHeader(request, partitionId, GET);
        request.writeSizedBytes(key);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> f = partitionActorRefs[partitionId].submit(request);
        IOBuffer response = null;
        try {
            response = f.get(requestTimeoutMs, MILLISECONDS);
            int length = response.readInt();

            if (length == -1) {
                return null;
            } else {
                byte[] bytes = new byte[length];
                response.readBytes(bytes, length);
                return bytes;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (response != null) {
                response.release();
            }
        }
    }

    public void pipeline(Pipeline p, byte[] key) {
        PipelineImpl pipeline = (PipelineImpl) p;
        pipeline.init(hashToIndex(Arrays.hashCode(key), partitionCount));

        int sizePos = pipeline.request.position();
        // size placeholder
        pipeline.request.writeInt(0);
        // opcode
        pipeline.request.writeInt(OpCodes.GET);
        // writing the key
        pipeline.request.writeSizedBytes(key);
        // fixing the size
        pipeline.request.putInt(sizePos, pipeline.request.position() - sizePos);

        pipeline.count++;
    }
}
