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

package com.hazelcast.tpc.engine.iobuffer;

import com.hazelcast.tpc.engine.AsyncSocket;
import com.hazelcast.tpc.requestservice.FrameCodec;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.nio.Bits.BYTES_CHAR;
import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;
import static com.hazelcast.internal.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;


/**
 * The IOBuffer is used to read/write bytes from I/O devices like a socket or a file.
 * <p>
 * Currently, the IOBuffer has one ByteBuffer underneath. The problem is that if you have very large
 * payloads, a single chunk of memory is needed for those bytes. This can lead to allocation problems
 * due to fragmentation (perhaps it can't be allocated because of fragmentation), but it can also
 * lead to fragmentation because buffers can have different sizes.
 * <p>
 * So instead of having a single ByteBuffer underneath, allow for a list of ByteBuffer all with some
 * fixed size, e.g. up to 16 KB. So if a 1MB chunk of data is received, just 64 byte-arrays of 16KB.
 * This will prevent the above fragmentation problem although it could lead to some increased
 * internal fragmentation because more memory is allocated than used. I believe this isn't such a
 * big problem because IOBuffer are short lived.
 * <p>
 * So if an IOBuffer should contain a list of ByteBuffers, then regular reading/writing to the IOBuffer
 * should be agnostic of the composition.
 */
public class IOBuffer {

    public IOBuffer next;
    public AsyncSocket socket;

    public boolean trackRelease;
    private ByteBuffer buff;
    public IOBufferAllocator allocator;
    public boolean concurrent = false;
    // make field?
    protected AtomicInteger refCount = new AtomicInteger();

    public IOBuffer(int size) {
        this(size, false);
    }

    public IOBuffer(int size, boolean direct) {
        //todo: allocate power of 2.
        this.buff = direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    public IOBuffer(ByteBuffer buffer) {
        this.buff = buffer;
    }

    public ByteBuffer byteBuffer() {
        return buff;
    }

    public void clear() {
        buff.clear();
    }

    public void writeByte(byte value) {
        ensureRemaining(BYTE_SIZE_IN_BYTES);
        buff.put(value);
    }

    public void writeChar(char value) {
        ensureRemaining(CHAR_SIZE_IN_BYTES);
        buff.putChar(value);
    }

    public void setInt(int pos, int value) {
        buff.putInt(pos, value);
    }

    public void writeInt(int value) {
        ensureRemaining(BYTES_INT);
        buff.putInt(value);
    }

    public void writeSizedBytes(byte[] src) {
        ensureRemaining(src.length + BYTES_INT);
        buff.putInt(src.length);
        buff.put(src);
    }

    public void writeBytes(byte[] src) {
        ensureRemaining(src.length);
        buff.put(src);
    }

    public int position() {
        return buff.position();
    }

    // very inefficient
    public void writeString(String s) {
        int length = s.length();

        ensureRemaining(BYTES_INT + length * BYTES_CHAR);

        buff.putInt(length);
        for (int k = 0; k < length; k++) {
            buff.putChar(s.charAt(k));
        }
    }

    // very inefficient
    public void readString(StringBuffer sb) {
        int size = buff.getInt();
        for (int k = 0; k < size; k++) {
            sb.append(buff.getChar());
        }
    }

    public String readString() {
        StringBuffer sb = new StringBuffer();
        int size = buff.getInt();
        for (int k = 0; k < size; k++) {
            sb.append(buff.getChar());
        }
        return sb.toString();
    }

    public void writeLong(long value) {
        ensureRemaining(BYTES_LONG);
        buff.putLong(value);
    }

    public void putLong(int index, long value) {
        buff.putLong(index, value);
    }

    public long getLong(int index) {
        return buff.getLong(index);
    }

    public void putInt(int index, int value) {
        buff.putInt(index, value);
    }

    public int readInt() {
        return buff.getInt();
    }

    public long readLong() {
        return buff.getLong();
    }

    public char readChar() {
        return buff.getChar();
    }

    public IOBuffer reconstructComplete() {
        buff.flip();
        return this;
    }

    public int getInt(int index) {
        return buff.getInt(index);
    }

    public byte getByte(int index) {
        return buff.get(index);
    }

    public void write(ByteBuffer src, int count) {
        ensureRemaining(count);

        if (src.remaining() <= count) {
            buff.put(src);
        } else {
            int limit = src.limit();
            src.limit(src.position() + count);
            buff.put(src);
            src.limit(limit);
        }
    }

    public void position(int position) {
        buff.position(position);
    }

    public void incPosition(int delta) {
        buff.position(buff.position() + delta);
    }

    /**
     * Returns the number of bytes remaining in this buffer for reading or writing
     *
     * @return
     */
    public int remaining() {
        return buff.remaining();
    }

    public void ensureRemaining(int remaining) {
        if (buff.remaining() < remaining) {
            int newCapacity = nextPowerOfTwo(buff.capacity() + remaining);

            ByteBuffer newBuffer = buff.hasArray()
                    ? ByteBuffer.allocate(newCapacity)
                    : ByteBuffer.allocateDirect(newCapacity);
            buff.flip();
            newBuffer.put(buff);
            buff = newBuffer;
        }
    }

    public void readBytes(byte[] dst, int len) {
        buff.get(dst, 0, len);
    }

    public void acquire() {
        if (allocator == null) {
            return;
        }

        if (concurrent) {
            for (; ; ) {
                int current = refCount.get();
                if (current == 0) {
                    throw new IllegalStateException("Can't acquire a freed IOBuffer");
                }

                if (refCount.compareAndSet(current, current + 1)) {
                    break;
                }
            }
        } else {
            refCount.lazySet(refCount.get() + 1);
        }
    }

    public int refCount() {
        return refCount.get();
    }

    public void release() {
        if (allocator == null) {
            return;
        }

        if (concurrent) {
            for (; ; ) {
                int current = refCount.get();
                if (current > 0) {
                    if (refCount.compareAndSet(current, current - 1)) {
                        if (current == 1) {
                            allocator.free(this);
                        }
                        break;
                    }
                } else {
                    throw new IllegalStateException("Too many releases. Ref counter must be larger than 0, current:" + current);
                }
            }
        } else {
            int current = refCount.get();
            if (current == 1) {
                refCount.lazySet(0);
                allocator.free(this);
            } else if (current > 1) {
                refCount.lazySet(current - 1);
            } else {
                throw new IllegalStateException("Too many releases. Ref counter must be larger than 0, current:" + current);
            }
        }
    }
}
