package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.Request;
import com.hazelcast.table.impl.SelectByKeyOperation;
import com.hazelcast.table.impl.UpsertOperation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_DONE;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_FOO;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_SELECT_BY_KEY;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_UPSERT;
import static java.nio.channels.SelectionKey.OP_READ;

public class NioReactor extends Thread {
    private final NioReactorFrontEnd frontend;
    private final Selector selector;
    private final ILogger logger;
    private final int port;
    private ServerSocketChannel serverSocketChannel;
    public final ConcurrentLinkedQueue taskQueue = new ConcurrentLinkedQueue();
    private final PacketIOHelper packetIOHelper = new PacketIOHelper();
    private BitSet allowedCpus;

    public NioReactor(NioReactorFrontEnd frontend, Address thisAddress, int port) {
        super("Reactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port);
        this.frontend = frontend;
        this.logger = frontend.logger;
        this.selector = SelectorOptimizer.newSelector(frontend.logger);
        this.port = port;
    }

    public void setThreadAffinity(ThreadAffinity threadAffinity) {
        this.allowedCpus = threadAffinity.nextAllowedCpus();
    }

    public void wakeup() {
        if (Thread.currentThread() != this) {
            System.out.println("wakeup");
            selector.wakeup();
        }
    }

    public void enqueue(Request request) {
        taskQueue.add(request);
        wakeup();
    }

    public Future<Channel> enqueue(SocketAddress address, Connection connection) {
        logger.info("Connect to " + address);

        ConnectRequest connectRequest = new ConnectRequest();
        connectRequest.address = address;
        connectRequest.connection = connection;
        connectRequest.future = new CompletableFuture<>();
        taskQueue.add(connectRequest);

        wakeup();

        return connectRequest.future;
    }

    static class ConnectRequest {
        Connection connection;
        SocketAddress address;
        CompletableFuture<Channel> future;
    }

    public void run() {
        setThreadAffinity();

        try {
            if (bind()) {
                loop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setThreadAffinity() {
        if (allowedCpus == null) {
            return;
        }

        ThreadAffinityHelper.setAffinity(allowedCpus);
        BitSet actualCpus = ThreadAffinityHelper.getAffinity();
        ILogger logger = Logger.getLogger(HazelcastManagedThread.class);
        if (!actualCpus.equals(allowedCpus)) {
            logger.warning(getName() + " affinity was not applied successfully. "
                    + "Expected CPUs:" + allowedCpus + ". Actual CPUs:" + actualCpus);
        } else {
            logger.info(getName() + " has affinity for CPUs:" + allowedCpus);
        }
    }

    private boolean bind() {
        InetSocketAddress address = null;
        try {
            InetAddress host = InetAddress.getLocalHost();
            address = new InetSocketAddress(host, port);
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(address);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("ServerSocket listening at " + address);
            return true;
        } catch (IOException e) {
            logger.severe("Could not bind to " + address);
        }
        return false;
    }

    private void loop() throws Exception {
        while (!frontend.shuttingdown) {

            int keyCount = selector.select();

            //Thread.sleep(1000);

            //System.out.println(this + " selectionCount:" + keyCount);

            if (keyCount > 0) {
                processSelectionKeys();
            }

            processTasks();
        }
    }

    private void processSelectionKeys() throws IOException {
        Set<SelectionKey> selectionKeys = selector.selectedKeys();
        Iterator<SelectionKey> it = selectionKeys.iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            if (key.isValid() && key.isAcceptable()) {
                SocketChannel socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(false);
                SelectionKey selectionKey = socketChannel.register(selector, OP_READ);
                selectionKey.attach(newChannel(socketChannel, null));

                logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
            }

            if (key.isValid() && key.isReadable()) {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                Channel channel = (Channel) key.attachment();
                ByteBuffer readBuf = channel.readBuffer;
                int bytesRead = socketChannel.read(readBuf);
                System.out.println(this + " bytes read: " + bytesRead);
                if (bytesRead <= 0) {
                    socketChannel.close();
                    key.cancel();
                    break;
                }
                readBuf.flip();
                process(readBuf, channel);
                compactOrClear(readBuf);
            }

            if (!key.isValid()) {
                System.out.println("sk not valid");
                key.cancel();
            }
        }
    }

    private Channel newChannel(SocketChannel socketChannel, Connection connection) {
        System.out.println(this + " newChannel: " + socketChannel);

        Channel channel = new Channel();
        channel.reactor = this;
        channel.readBuffer = ByteBuffer.allocate(256 * 1024);
        channel.socketChannel = socketChannel;
        channel.connection = connection;
        return channel;
    }

    private void processTasks() {
        for (; ; ) {
            Object item = taskQueue.poll();
            if (item == null) {
                return;
            }

            if (item instanceof Channel) {
                process((Channel) item);
            } else if (item instanceof ConnectRequest) {
                process((ConnectRequest) item);
            } else if (item instanceof Op) {
                proces((Op) item);
            } else if (item instanceof Request) {
                proces((Request) item);
            } else {
                throw new RuntimeException("Unregonized type:" + item.getClass());
            }
        }
    }

    private void process(Channel channel) {
        System.out.println("Processing channel");
        try {
            // the buffers could be written in batch.
            for (; ; ) {
                ByteBuffer buffer = channel.next();
                if (buffer == null) {
                    break;
                }

                int written = channel.socketChannel.write(buffer);
                System.out.println("written:" + written);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void process(ConnectRequest connectRequest) {
        try {
            SocketAddress address = connectRequest.address;
            System.out.println("makeConnections address:" + address);

            SocketChannel socketChannel = SocketChannel.open();
            // todo: call is blocking
            socketChannel.connect(address);
            socketChannel.configureBlocking(false);
            SelectionKey key = socketChannel.register(selector, OP_READ);

            Channel channel = newChannel(socketChannel, connectRequest.connection);
            key.attach(channel);

            logger.info("Socket listening at " + address);
            connectRequest.future.complete(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void process(ByteBuffer buffer, Channel channel) {
        for (; ; ) {
            Packet packet = packetIOHelper.readFrom(buffer);
            System.out.println(this + " read packet: " + packet);
            if (packet == null) {
                return;
            }

            packet.setConn((ServerConnection) channel.connection);
            packet.channel = channel;
            process(packet);
        }
    }

    private void process(Packet packet) {
        System.out.println(this + " process packet: " + packet);
        try {
            if(packet.isFlagRaised(Packet.FLAG_OP_RESPONSE)){
                frontend.handleResponse(packet);
            }else {
                byte[] bytes = packet.toByteArray();
                byte opcode = bytes[Packet.DATA_OFFSET];
                Op op = allocateOp(opcode);
                op.in.init(packet.toByteArray(), Packet.DATA_OFFSET + 1);
                proces(op);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void proces(Request request) {
        System.out.println("request: " + request);
        try {
            byte[] data = request.out.toByteArray();
            byte opcode = data[0];
            Op op = allocateOp(opcode);
            op.in.init(data, 1);
            proces(op);
            request.invocation.completableFuture.complete(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void proces(Op op) {
        try {
            long callId = op.in.readLong();
            System.out.println("callId: "+callId);
            int runCode = op.run();
            switch (runCode) {
                case RUN_CODE_DONE:
                    free(op);
                    return;
                case RUN_CODE_FOO:
                    throw new RuntimeException();
                default:
                    throw new RuntimeException();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // use pool
    private Op allocateOp(int opcode) {
        Op op;
        switch (opcode) {
            case TABLE_UPSERT:
                op = new UpsertOperation();
                break;
            case TABLE_SELECT_BY_KEY:
                op = new SelectByKeyOperation();
                break;
            default://hack
                op = new UpsertOperation();
                //throw new RuntimeException("Unrecognized opcode:" + opcode);
        }
        op.in = new ByteArrayObjectDataInput(null, (InternalSerializationService) frontend.ss, ByteOrder.BIG_ENDIAN);
        op.managers = frontend.managers;
        return op;
    }

    private void free(Op op) {
        op.cleanup();

        //we should return it to the pool.
    }
}