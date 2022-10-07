package com.hazelcast.internal.alto.bootstrap;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.alto.runtime.RequestService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.tpc.TpcEngine;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.nio.NioAsyncReadHandler;
import com.hazelcast.internal.tpc.nio.NioAsyncServerSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.internal.alto.runtime.SocketConfig;
import com.hazelcast.internal.alto.runtime.TPCEventloopThread;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.lang.System.*;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TpcBootstrap {

    public final NodeEngineImpl nodeEngine;
    public final InternalSerializationService ss;
    public final ILogger logger;
    private final Address thisAddress;
    private final int socketCount;
    private final SocketConfig socketConfig;
    private final boolean writeThrough;
    private final boolean regularSchedule;
    public volatile boolean shuttingdown = false;
    private TpcEngine engine;
    private final Map<Eventloop, Supplier<? extends ReadHandler>> readHandlerSuppliers = new HashMap<>();
    private List<AsyncServerSocket> serverSockets = new ArrayList<>();
    private final boolean enabled;

    public TpcBootstrap(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(RequestService.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.enabled = Boolean.parseBoolean(getProperty("reactor.enabled", "false"));
        logger.info("TPC: " + (enabled ? "enabled" : "disabled"));
        this.writeThrough = Boolean.parseBoolean(getProperty("reactor.write-through", "false"));
        this.regularSchedule = Boolean.parseBoolean(getProperty("reactor.regular-schedule", "true"));
        this.socketCount = Integer.parseInt(getProperty("reactor.channels", "" + Runtime.getRuntime().availableProcessors()));
        this.thisAddress = nodeEngine.getThisAddress();
        this.engine = newEngine();
        this.socketConfig = new SocketConfig();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TpcEngine getEngine() {
        return engine;
    }

    private TpcEngine newEngine() {
        if (!enabled) {
            return null;
        }

        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setThreadFactory(TPCEventloopThread::new);
        configuration.setEventloopType(Eventloop.Type.NIO);

        TpcEngine engine = new TpcEngine(configuration);

        if (socketCount % engine.eventloopCount() != 0) {
            throw new IllegalStateException("socket count is not multiple of eventloop count");
        }

        return engine;
    }

    public void start() {
        if (!enabled) {
            return;
        }

        logger.info("Starting TpcBootstrap");
        engine.start();

        Eventloop.Type eventloopType = engine.eventloopType();
        switch (eventloopType) {
            case NIO:
                startNio();
                break;
            default:
                throw new IllegalStateException("Unknown eventloopType:" + eventloopType);
        }
    }

    private void startNio() {
        for (int k = 0; k < engine.eventloopCount(); k++) {
            NioEventloop eventloop = (NioEventloop) engine.eventloop(k);

            Supplier<NioAsyncReadHandler> readHandlerSupplier = () -> {
                out.println("TPC Server: Making ClientNioAsyncReadHandler");
                //todo: we need to figure out the connection
                return new ClientNioAsyncReadHandler(nodeEngine.getNode().clientEngine);
            };
            readHandlerSuppliers.put(eventloop, readHandlerSupplier);

            try {
                NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(eventloop);
                serverSockets.add(serverSocket);
                serverSocket.receiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.reuseAddress(true);
                int port = toPort(nodeEngine.getThisAddress(), k);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.accept(socket -> {
                    socket.readHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.setWriteThrough(writeThrough);
                    socket.setRegularSchedule(regularSchedule);
                    socket.sendBufferSize(socketConfig.sendBufferSize);
                    socket.receiveBufferSize(socketConfig.receiveBufferSize);
                    socket.tcpNoDelay(socketConfig.tcpNoDelay);
                    socket.keepAlive(true);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private int toPort(Address address, int socketId) {
        return (address.getPort() - 5701) * 100 + 11000 + socketId % engine.eventloopCount();
    }

    public void shutdown() {
        if (!enabled) {
            return;
        }

        logger.info("TcpBootstrap shutdown");

        shuttingdown = true;
        engine.shutdown();

        try {
            engine.awaitTermination(5, SECONDS);
        } catch (InterruptedException e) {
            logger.warning("TpcEngine failed to terminate.");
            Thread.currentThread().interrupt();
        }

        logger.info("TcpBootstrap terminated");
    }

    public String getClientPorts() {
        if (!enabled) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (AsyncServerSocket serverSocket : serverSockets) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(serverSocket.getLocalPort());
        }
        return sb.toString();
    }
}
