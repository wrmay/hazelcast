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
import com.hazelcast.internal.tpc.iouring.IOUringReactorBuilder;
import com.hazelcast.internal.util.ThreadAffinity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import static com.hazelcast.file.FileBenchmarkUtils.initFile;
import static com.hazelcast.file.FileBenchmarkUtils.randomTmpFile;
import static com.hazelcast.internal.tpc.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpc.AsyncFile.O_NOATIME;
import static com.hazelcast.internal.tpc.AsyncFile.O_RDWR;
import static com.hazelcast.internal.tpc.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.BufferUtil.toPageAlignedAddress;
import static com.hazelcast.internal.tpc.util.OS.pageSize;

/**
 * For a FIO based benchmark so you can compare
 * fio --name=write_throughput --numjobs=1 --size=4M --time_based --runtime=60s --ramp_time=2s --ioengine=io_uring  --direct=1 --verify=0 --bs=4k --iodepth=64 --rw=write --group_reporting=1
 */
public class AsyncFileWriteBenchmark {

    public static final String dir = "/run/media/pveentjer/b72258e9-b9c9-4f7c-8b76-cef961eeec55";
    public static final boolean sequential = true;
    public static final long operationsPerThread = 4 * 1000 * 1000;
    public static final int concurrencyPerThread = 64;
    public static final int openFlags = O_DIRECT | O_RDWR | O_NOATIME;
    public static final int blockSize = 1 * pageSize();
    public static final int fileSize = 1024 * blockSize;
    public static final int threadCount = 1;
    public static final String affinity = "1";
    public static final boolean spin = false;

    public static void main(String[] args) throws Exception {
        ThreadAffinity threadAffinity = new ThreadAffinity(affinity);

        List<Reactor> reactors = new ArrayList<>();
        List<String> fileList = new ArrayList<>();
        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            IOUringReactorBuilder configuration = new IOUringReactorBuilder();
            //configuration.setFlags(IORING_SETUP_IOPOLL);
            configuration.setThreadAffinity(threadAffinity);
            configuration.setSpin(spin);

            StorageDeviceRegistry storageDeviceRegistry = new StorageDeviceRegistry();
            storageDeviceRegistry.register(dir, 512, 512);
            configuration.setStorageDeviceRegistry(storageDeviceRegistry);

            Reactor reactor = configuration.build();
            reactors.add(reactor);
            reactor.start();

            String path = randomTmpFile(dir);
            initFile(path, fileSize);
            fileList.add(path);
        }

        System.out.println("Starting benchmark");

        long startMs = System.currentTimeMillis();
        List<CountDownLatch> latches = new ArrayList<>();
        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            final int _threadIndex = threadIndex;
            ByteBuffer buffer = ByteBuffer.allocateDirect(blockSize + pageSize());
            long rawAddress = addressOf(buffer);
            long bufferAddress = toPageAlignedAddress(rawAddress);

            for (int c = 0; c < buffer.capacity() / 2; c++) {
                buffer.putChar('a');
            }
            Reactor reactor = reactors.get(threadIndex);
            CountDownLatch completionLatch = new CountDownLatch(concurrencyPerThread);
            reactor.offer(() -> {
                for (int loopIndex = 0; loopIndex < concurrencyPerThread; loopIndex++) {
                    WriteLoop loop = new WriteLoop();
                    loop.latch = completionLatch;
                    loop.bufferAddress = bufferAddress;
                    loop.operations = operationsPerThread / concurrencyPerThread;
                    AsyncFile file = reactor.eventloop().newAsyncFile(fileList.get(_threadIndex));
                    file.open(openFlags, PERMISSIONS_ALL).then((integer, throwable) -> {
                        if (throwable != null) {
                            throwable.printStackTrace();
                            System.exit(1);
                        }

                        loop.file = file;
                        reactor.offer(loop);
                    });
                }
            });

            latches.add(completionLatch);
        }

        for (CountDownLatch latch : latches) {
            latch.await();
        }

        long duration = System.currentTimeMillis() - startMs;
        long operations = threadCount * operationsPerThread;

        System.out.println((operations * 1000f / duration) + " IOPS");
        long dataSize = blockSize * operations;
        System.out.println("Bandwidth: " + (dataSize * 1000 / (duration * 1024 * 1024)) + " MB/s");
        System.exit(0);
    }

    private static class WriteLoop implements Runnable, BiConsumer<Integer, Throwable> {
        private long operation;
        private AsyncFile file;
        private long bufferAddress;
        private long operations;
        private CountDownLatch latch;
        private final int blockCount = fileSize / blockSize;
        private final ThreadLocalRandom random = ThreadLocalRandom.current();
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

                file.pwrite(offset, blockSize, bufferAddress).then(this).releaseOnComplete();
            } else {
                latch.countDown();
            }
        }

        @Override
        public void accept(Integer result, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            run();
        }
    }

}
