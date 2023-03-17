package com.hazelcast.pubsub;

import com.hazelcast.core.TpcProxy;

public interface Publisher extends TpcProxy {

    byte SYNC_NONE = 0;
    byte SYNC_FSYNC = 1;
    byte SYNC_FDATASYNC = 2;

    // todo: this API is very inefficient because we impose a byte-array.
    // Perhaps expose a version with a byte buffer.java
    long publish(int partitionId, byte[] message, byte syncOption);
}
