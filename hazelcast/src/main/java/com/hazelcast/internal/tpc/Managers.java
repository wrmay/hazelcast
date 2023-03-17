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

import com.hazelcast.bulktransport.impl.BulkTransportService;
import com.hazelcast.htable.impl.HTableManager;
import com.hazelcast.pubsub.impl.TopicManager;

// Very ugly way to get the dependencies.
public class Managers {

    public HTableManager tableManager;

    public TopicManager topicManager;

    public BulkTransportService bulkTransportService;
}
