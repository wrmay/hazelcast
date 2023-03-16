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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.util.UUID;

import static com.hazelcast.internal.tpcengine.util.OS.pageSize;

public class FileBenchmarkUtils {

    public static void initFile(String path, int fileSize) throws IOException {
        long startMs = System.currentTimeMillis();
        File file = new File(path);
        //file.deleteOnExit();
        long blockCount = fileSize / pageSize();
        FileOutputStream fos = new FileOutputStream(file);
        byte[] zeroBlock = new byte[pageSize()];
        OutputStream out = new BufferedOutputStream(fos, 128 * pageSize());
        for (int k = 0; k < blockCount; k++) {
            out.write(zeroBlock);
        }
        out.flush();
        fos.getFD().sync();
        out.close();
        long duration = System.currentTimeMillis() - startMs;
        System.out.println("Creating file " + path + " took " + duration + " ms");
    }

    public static String randomTmpFile(String dir) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        return dir + separator + uuid;
    }
}
