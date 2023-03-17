/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.tpcengine.AsyncSocket;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.TpcEngine;
import com.hazelcast.internal.tpcengine.actor.ActorRef;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;

/**
 * An {@link ActorRef} that routes messages to the {@link PartitionActor}.
 * <p>
 * todo:
 * Should also handle redirect messages.
 */
public final class PartitionActorRef extends ActorRef<IOBuffer> {

    private final int partitionId;
    private final InternalPartitionService partitionService;
    private final Address thisAddress;
    private final Requests requests;
    private final MemberTpcRuntime tpcRuntime;
    private final Reactor reactor;

    public PartitionActorRef(int partitionId,
                             InternalPartitionService partitionService,
                             TpcEngine engine,
                             MemberTpcRuntime tpcRuntime,
                             Address thisAddress,
                             Requests requests) {
        this.partitionId = partitionId;
        this.partitionService = partitionService;
        this.thisAddress = thisAddress;
        this.requests = requests;
        this.tpcRuntime = tpcRuntime;
        this.reactor = engine.reactor(hashToIndex(partitionId, engine.reactorCount()));
    }

    public RequestFuture<IOBuffer> submit(IOBuffer request) {
        RequestFuture future = new RequestFuture(request);

        requests.slots.put(future);

        Address address = partitionService.getPartitionOwner(partitionId);
        if (address == null) {
            throw new RuntimeException("Address is still null (we need to deal with this situation better)");
        }

        if (address.equals(thisAddress)) {
            request.socket = null;
            // System.out.println("local request");
            //TODO: deal with return value
            reactor.offer(request);
        } else {
            //  System.out.println("remote request");
            // todo: this should in theory not be needed. We could use the last
            // address and only in case of a redirect, we update.
            TcpServerConnection connection = tpcRuntime.getConnection(address);
            AsyncSocket[] sockets = connection.getSockets();
            AsyncSocket socket = sockets[hashToIndex(partitionId, sockets.length)];

            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();

            //todo: deal with return value.
            socket.writeAndFlush(request);
        }
        return future;
    }

    @Override
    public void send(IOBuffer request) {
        submit(request);
    }
}
