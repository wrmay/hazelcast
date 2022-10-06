package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.internal.engine.iouring.IOUringAsyncSocket;
import com.hazelcast.internal.engine.iouring.IOUringEventloop;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.engine.AsyncSocket;
import com.hazelcast.tpc.engine.AsyncSocketTest;
import com.hazelcast.internal.engine.Eventloop;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class IOUringAsyncSocketTest extends AsyncSocketTest {

    @Override
    public Eventloop createEventloop() {
        return new IOUringEventloop();
    }

    @Override
    public AsyncSocket createAsyncSocket() {
        AsyncSocket socket = IOUringAsyncSocket.open();
        closeables.add(socket);
        return socket;
    }
}
