/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.frame.Frame;
import org.jctools.util.PaddedAtomicLong;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;

class RequestRegistry {

    private final int concurrentRequestLimit;
    private final long nextCallIdTimeoutMs = TimeUnit.SECONDS.toMillis(10);
    private final ConcurrentMap<SocketAddress, Requests> requestsPerSocket = new ConcurrentHashMap<>();

    public RequestRegistry(int concurrentRequestLimit) {
        this.concurrentRequestLimit = concurrentRequestLimit;
    }

    void shutdown() {
        for (Requests requests : requestsPerSocket.values()) {
            requests.shutdown();
        }
    }

    Requests get(SocketAddress address) {
        return requestsPerSocket.get(address);
    }

    Requests getRequestsOrCreate(SocketAddress address) {
        Requests requests = requestsPerSocket.get(address);
        if (requests == null) {
            Requests newRequests = new Requests();
            Requests foundRequests = requestsPerSocket.putIfAbsent(address, newRequests);
            return foundRequests == null ? newRequests : foundRequests;
        } else {
            return requests;
        }
    }

    class Requests {
        final ConcurrentMap<Long, Frame> map = new ConcurrentHashMap<>();
        final PaddedAtomicLong started = new PaddedAtomicLong();
        final PaddedAtomicLong completed = new PaddedAtomicLong();

        void complete() {
            if (concurrentRequestLimit > -1) {
                completed.incrementAndGet();
            }
        }

        long nextCallId() {
            if (concurrentRequestLimit == -1) {
                return started.incrementAndGet();
            } else {
                long endTime = currentTimeMillis() + nextCallIdTimeoutMs;
                do {
                    if (completed.get() + concurrentRequestLimit > started.get()) {
                        return started.incrementAndGet();
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    }
                } while (currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }

        long nextCallId(int count) {
            if (concurrentRequestLimit == -1) {
                return started.addAndGet(count);
            } else {
                long endTime = currentTimeMillis() + nextCallIdTimeoutMs;
                do {
                    if (completed.get() + concurrentRequestLimit > started.get() + count) {
                        return started.addAndGet(count);
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    }
                } while (currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }

        void shutdown() {
            for (Frame request : map.values()) {
                request.future.completeExceptionally(new RuntimeException("Shutting down"));
            }
        }
    }
}