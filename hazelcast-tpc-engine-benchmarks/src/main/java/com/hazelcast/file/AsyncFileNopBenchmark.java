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

package com.hazelcast.file;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.StorageDeviceRegistry;
import com.hazelcast.internal.tpc.iouring.IOUringReactor;
import com.hazelcast.internal.tpc.iouring.IOUringReactorBuilder;
import com.hazelcast.internal.util.ThreadAffinity;

import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpc.util.OS.pageSize;

public class AsyncFileNopBenchmark {
    private static String path = System.getProperty("user.home");

    public static long operations = 10000l * 1000 * 1000;
    public static final int concurrency = 100;
    public static final int openFlags = AsyncFile.O_CREAT | AsyncFile.O_DIRECT | AsyncFile.O_WRONLY;
    public static final String affinity = "1";
    public static final boolean spin = false;

    public static void main(String[] args) throws Exception {
        StorageDeviceRegistry storageDeviceRegistry = new StorageDeviceRegistry();
        //storageDeviceRegistry.registerStorageDevice(path, 100, 16384);

        IOUringReactorBuilder configuration = new IOUringReactorBuilder();
        if (affinity != null) {
            configuration.setThreadAffinity(new ThreadAffinity(affinity));
        }
        configuration.setSpin(spin);
        configuration.setStorageDeviceRegistry(storageDeviceRegistry);
        Reactor reactor = configuration.build();
        reactor.start();

        List<AsyncFile> files = new ArrayList<>();
        //files.add(initFile(reactor, "/home/pveentjer"));
        files.add(initFile(reactor, path));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operations / concurrency;

        CountDownLatch latch = new CountDownLatch(concurrency);
        reactor.offer(() -> {
            for (int k = 0; k < concurrency; k++) {
                NopLoop loop = new NopLoop();
                loop.latch = latch;
                loop.sequentialOperations = sequentialOperations;
                loop.file = files.get(k % files.size());
                reactor.offer(loop);
            }
        });

        latch.await();
        long duration = System.currentTimeMillis() - startMs;
        System.out.println((operations * 1000 / duration) + " IOPS");
        System.exit(0);
    }

    private static AsyncFile initFile(Reactor eventloop, String dir) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.offer(() -> {
            AsyncFile file = eventloop.eventloop().newAsyncFile(randomTmpFile(dir));
            file.open(openFlags, PERMISSIONS_ALL)
                    .then((o, o2) -> file.fallocate(0, 0, pageSize())
                            .then((o1, o21) -> {
                                initFuture.complete(file);
                            }));
        });

        return initFuture.get();
    }

    private static class NopLoop implements Runnable, BiConsumer<Object, Throwable> {
        private long operation;
        private AsyncFile file;
        private long sequentialOperations;
        private CountDownLatch latch;

        @Override
        public void run() {
            if (operation < sequentialOperations) {
                operation++;
                file.nop().then(this).releaseOnComplete();
            } else {
                latch.countDown();
            }
        }

        @Override
        public void accept(Object o, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            run();
        }
    }

    private static String randomTmpFile(String dir) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        return dir + separator + uuid;
    }
}
