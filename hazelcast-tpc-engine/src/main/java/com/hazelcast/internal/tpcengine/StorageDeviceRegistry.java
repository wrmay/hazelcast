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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.Preconditions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

public class StorageDeviceRegistry {
    private final List<StorageDevice> devs = new ArrayList<>();

    public StorageDeviceRegistry() {
    }

    public StorageDevice findStorageDevice(String path) {
        Preconditions.checkNotNull(path, "path");

        for (StorageDevice dev : devs) {
            if (path.startsWith(dev.getPath())) {
                return dev;
            }
        }
        return null;
    }

    /**
     * This method should be called before the StorageScheduler is being used.
     * <p>
     * This method is not thread-safe.
     *
     * @param path          the path to the storage device.
     * @param maxConcurrent the maximum number of concurrent requests for the device.
     * @param maxPending    the maximum number of request that can be buffered
     */
    public void register(String path, int maxConcurrent, int maxPending) {
        File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("A storage device [" + path + "] doesn't exit");
        }

        if (!file.isDirectory()) {
            throw new RuntimeException("A storage device [" + path + "] is not a directory");
        }

        if (findStorageDevice(path) != null) {
            throw new RuntimeException("A storage device with path [" + path + "] already exists");
        }

        checkPositive(maxConcurrent, "maxConcurrent");

        StorageDevice dev = new StorageDevice(path, maxConcurrent, maxPending);
        devs.add(dev);
    }
}
