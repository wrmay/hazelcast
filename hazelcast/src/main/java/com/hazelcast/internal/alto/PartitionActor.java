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

package com.hazelcast.internal.alto;

import com.hazelcast.internal.tpcengine.actor.Actor;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;


/**
 * Processes messages within a partition. Issues like replication already have
 * been taken care of by the infrastructure.
 */
public class PartitionActor extends Actor {

    @Override
    public void process(Object m) {
        IOBuffer request = (IOBuffer) m;
    }
}
