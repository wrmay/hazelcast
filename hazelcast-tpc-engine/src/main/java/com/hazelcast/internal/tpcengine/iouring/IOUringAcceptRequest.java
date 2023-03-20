package com.hazelcast.internal.tpcengine.iouring;

import com.hazelcast.internal.tpcengine.AcceptRequest;

public class IOUringAcceptRequest implements AcceptRequest {

    final NativeSocket nativeSocket;

    public IOUringAcceptRequest(NativeSocket nativeSocket) {
        this.nativeSocket = nativeSocket;
    }

    @Override
    public void close() throws Exception {
        nativeSocket.close();
    }
}
