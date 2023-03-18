package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.pubsub.Publisher;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpc.member.OpCodes.TOPIC_PUBLISH;

public class PublisherProxy extends AbstractDistributedObject implements Publisher  {

    private final ConcurrentIOBufferAllocator requestAllocator;
    private final byte[] topicIdBytes;
    private final TpcRuntime tpcRuntime;
    private final int requestTimeoutMs;
    private final String name;

    public PublisherProxy(NodeEngineImpl nodeEngine, PublisherService publisherService, String name) {
        super(nodeEngine, publisherService);
        this.name = name;
        this.tpcRuntime = nodeEngine.getNode().getTpcRuntime();
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.topicIdBytes = name.getBytes();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return PublisherService.SERVICE_NAME;
    }

    @Override
    public long publish(int partitionId, byte[] message, byte syncOption) {
        IOBuffer request = requestAllocator.allocate();
        FrameCodec.writeRequestHeader(request, partitionId, TOPIC_PUBLISH);
        request.writeByte(syncOption);
        //request.writeSizedBytes(topicIdBytes);
        request.writeSizedBytes(message);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> future = tpcRuntime.invoke(request, partitionId);
        try {
            IOBuffer response = future.get();
            long sequence = response.readLong();
            response.release();
            return sequence;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
