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

package com.hazelcast.internal.tpc.member;

import com.hazelcast.bulktransport.impl.BulkTransportOp;
import com.hazelcast.bulktransport.impl.InitBulkTransportOp;
import com.hazelcast.pubsub.impl.TopicPublishOp;
import com.hazelcast.htable.impl.GetOp;
import com.hazelcast.noop.impl.NoOp;
import com.hazelcast.htable.impl.QueryOp;
import com.hazelcast.htable.impl.SetOp;

import java.util.function.Supplier;

import static com.hazelcast.internal.tpc.member.OpCodes.BULK_TRANSPORT;
import static com.hazelcast.internal.tpc.member.OpCodes.GET;
import static com.hazelcast.internal.tpc.member.OpCodes.INIT_BULK_TRANSPORT;
import static com.hazelcast.internal.tpc.member.OpCodes.MAX_OPCODE;
import static com.hazelcast.internal.tpc.member.OpCodes.NOOP;
import static com.hazelcast.internal.tpc.member.OpCodes.PIPELINE;
import static com.hazelcast.internal.tpc.member.OpCodes.QUERY;
import static com.hazelcast.internal.tpc.member.OpCodes.SET;
import static com.hazelcast.internal.tpc.member.OpCodes.TOPIC_PUBLISH;

public final class OpAllocator {

    private final Pool[] pools;
    private final OpScheduler scheduler;
    private final Managers manager;

    public OpAllocator(OpScheduler scheduler, Managers managers) {
        this.scheduler = scheduler;
        this.manager = managers;
        this.pools = new Pool[MAX_OPCODE + 1];
        pools[NOOP] = new Pool(NoOp::new);
        pools[GET] = new Pool(GetOp::new);
        pools[SET] = new Pool(SetOp::new);
        pools[QUERY] = new Pool(QueryOp::new);
        pools[INIT_BULK_TRANSPORT] = new Pool(InitBulkTransportOp::new);
        pools[BULK_TRANSPORT] = new Pool(BulkTransportOp::new);
        pools[TOPIC_PUBLISH] = new Pool(TopicPublishOp::new);
        pools[PIPELINE] = new Pool(PipelineOp::new);
    }

    public Op allocate(int opcode) {
        Pool pool = pools[opcode];
        pool.allocated++;
        Op op;
        if (pool.index == -1) {
            op = pool.supplier.get();
            op.allocator = this;
            op.managers = manager;
            op.scheduler = scheduler;
            op.eventloop = scheduler.getEventloop();
        } else {
            op = pool.array[pool.index];
            pool.array[pool.index] = null;//not needed
            pool.index--;
            pool.allocatedFromPool++;
        }
//
//        if (pool.allocated % 1000000 == 0) {
//            System.out.println("allocate pooled percentage: " +
//                    ((pool.allocatedFromPool * 100f) / pool.allocated) + " %, dropped:"+ pool.dropped);
//        }

        return op;
    }

    public void free(Op op) {
        Pool pool = pools[op.opcode];
        if (pool.index == pool.array.length - 1) {
            pool.dropped++;
            return;
        }

        op.clear();
        op.request = null;
        op.response = null;
        pool.index++;
        pool.array[pool.index] = op;
    }

    private static class Pool {
        public long dropped;
        // index points to first item that can be removed.
        private int index = -1;
        private Op[] array = new Op[16384];
        private Supplier<Op> supplier;
        private long allocatedFromPool = 0;
        private long allocated = 0;

        private Pool(Supplier supplier) {
            this.supplier = supplier;
        }
    }
}
