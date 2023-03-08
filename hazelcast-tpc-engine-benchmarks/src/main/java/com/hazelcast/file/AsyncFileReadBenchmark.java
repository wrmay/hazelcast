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
import com.hazelcast.internal.tpc.iouring.IOUringReactorBuilder;
import com.hazelcast.internal.util.ThreadAffinity;

import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpc.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpc.AsyncFile.O_NOATIME;
import static com.hazelcast.internal.tpc.AsyncFile.O_RDONLY;
import static com.hazelcast.internal.tpc.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.BufferUtil.toPageAlignedAddress;
import static com.hazelcast.internal.tpc.util.OS.pageSize;

// todo: should create a file with real content to prevent being optimized due to fallocate
public class AsyncFileReadBenchmark {
    private static String path = System.getProperty("user.home");

    public static boolean sequential = true;
    public static long operations = 100 * 1000 * 1000;
    public static int concurrency = 1;
    public static int openFlags = O_CREAT | O_DIRECT | O_RDONLY|O_NOATIME;
    public static final int blockSize = pageSize();
    public static final long fileSize = 1024l * blockSize;
    public static final String affinity = "1";
    public static boolean spin = false;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IOUringReactorBuilder configuration = new IOUringReactorBuilder();
        if(affinity!=null) {
            configuration.setThreadAffinity(new ThreadAffinity(affinity));
        }
        configuration.setSpin(spin);
        Reactor reactor = configuration.build();
        reactor.start();

        List<AsyncFile> files = new ArrayList<>();
        files.add(initFile(reactor, path));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operations / concurrency;

        CountDownLatch latch = new CountDownLatch(concurrency);
        reactor.offer(() -> {
            for (int k = 0; k < concurrency; k++) {
                ReadLoop loop = new ReadLoop();
                loop.latch = latch;
                loop.file = files.get(k % files.size());
                loop.operations = sequentialOperations;
                reactor.offer(loop);
            }
        });

        latch.await();
        long duration = System.currentTimeMillis() - startMs;
        System.out.println((operations * 1000f / duration) + " IOPS");
        long dataSize = blockSize * operations;
        System.out.println("Bandwidth: " + (dataSize * 1000 / (duration * 1024 * 1024)) + " MB/s");
        System.exit(0);
    }

    private static AsyncFile initFile(Reactor eventloop, String path) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.offer(() -> {
            AsyncFile file = eventloop.eventloop().newAsyncFile(path);
            file.open(openFlags, PERMISSIONS_ALL).then((BiConsumer) (o, o2) -> initFuture.complete(file));
        });

        return initFuture.get();
    }

    private static class ReadLoop implements Runnable, BiConsumer<Integer, Throwable> {
        private long operation;
        private AsyncFile file;
        private long operations;
        private CountDownLatch latch;
        private final ThreadLocalRandom random = ThreadLocalRandom.current();
        private final int blockCount = (int) (fileSize / blockSize);
        private final ByteBuffer buffer = ByteBuffer.allocateDirect(blockSize + pageSize());
        private final long rawAddress = addressOf(buffer);
        private final long bufferAddress = toPageAlignedAddress(rawAddress);
        private long block;

        @Override
        public void run() {
            if (operation < operations) {
                operation++;

                long offset;
                if (sequential) {
                    offset = block * blockSize;
                    block++;
                    if (block >= blockCount) {
                        block = 0;
                    }
                } else {
                    offset = ((long) random.nextInt(blockCount)) * blockSize;
                }

                file.pread(offset, blockSize, bufferAddress).then(this).releaseOnComplete();
            } else {
                latch.countDown();
            }
        }

        @Override
        public void accept(Integer o, Throwable throwable) {
            System.out.println("Storage response received");

            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            run();
        }
    }


    private static String randomTmpFile() {
        String dir = "/home/pveentjer";
        //System.getProperty("java.io.tmpdir");
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();

        return dir + separator + uuid;
    }

}
