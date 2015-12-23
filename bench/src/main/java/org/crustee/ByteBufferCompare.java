/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.crustee;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.ToIntFunction;
import org.crustee.raft.utils.ByteBufferUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import com.google.common.primitives.Longs;
import sun.misc.Unsafe;
import uk.co.real_logic.agrona.UnsafeAccess;

@State(Scope.Benchmark)
public class ByteBufferCompare {

    public static final Comparator<ByteBuffer> BYTE_BUFFER_COMPARATOR = ByteBufferUtils.lengthFirstComparator();

    ByteBuffer bb1;
    ByteBuffer bb2;

    @Setup
    public void setup() {
        bb1 = ByteBuffer.allocateDirect(128);
        bb2 = ByteBuffer.allocateDirect(128);
    }

    @Benchmark
    public void compareTo(Blackhole blackhole) {
        blackhole.consume(bb1.compareTo(bb2));
    }

    @Benchmark
    public void lengthFirst_then_compareTo(Blackhole blackhole) {
        blackhole.consume(lengthFirstCompare(bb1, bb2));
    }

    private int lengthFirstCompare(ByteBuffer o1, ByteBuffer o2) {
        int o1Remaining = o1.remaining();
        int o2Remaining = o2.remaining();
        if (o1Remaining < o2Remaining) {
            return -1;
        }
        if (o1Remaining > o2Remaining) {
            return 1;
        }
        // buffers have the same length, so this is safe
        int longComparisons = o1.limit() & ~7;
        int i = 0;
        for (; i < longComparisons ; i += Longs.BYTES) {
            int cmp = Long.compare(o1.getLong(i), o2.getLong(i));
            if (cmp != 0) {
                return cmp;
            }
        }

        for (; i < o1.limit(); i++) {
            int cmp = Byte.compare(o1.get(i), o2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Benchmark
    public void wordComparisons(Blackhole blackhole) {
        blackhole.consume(BYTE_BUFFER_COMPARATOR.compare(bb1, bb2));
    }

    static final long addressOffset;

    public static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;

    static {
        try {
            Class<?> aClass = Class.forName("java.nio.Buffer");
            Field address = aClass.getDeclaredField("address");
            addressOffset = UNSAFE.objectFieldOffset(address);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void unsafe(Blackhole blackhole) {
        // interestingly, in benches using unsafe is faster but in almost-real workloads, it is slower
        blackhole.consume(unsafeCompare(bb1, bb2));
    }

    private static int unsafeCompare(ByteBuffer o1, ByteBuffer o2) {
        int o1Remaining = o1.remaining();
        int o2Remaining = o2.remaining();
        if (o1Remaining < o2Remaining) {
            return -1;
        }
        if (o1Remaining > o2Remaining) {
            return 1;
        }
        // buffers have the same length, so this is safe
        int longComparisons = o1.limit() & ~7;
        int i = 0;
        long address1 = UNSAFE.getLong(o1, addressOffset);
        long address2 = UNSAFE.getLong(o2, addressOffset);
        for (; i < longComparisons ; i += Longs.BYTES) {
            long l1 = UNSAFE.getLong(address1 + i);
            long l2 = UNSAFE.getLong(address2 + i);
            int cmp = Long.compare(l1, l2);
            if (cmp != 0) {
                return cmp;
            }
        }

        for (; i < o1.limit(); i++) {
            byte b1 = UNSAFE.getByte(address1 + i);
            byte b2 = UNSAFE.getByte(address2 + i);
            int cmp = Byte.compare(b1, b2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Benchmark
    public void comparatorCombinations(Blackhole blackhole) {
        blackhole.consume(Comparator
                .comparingInt((ToIntFunction<ByteBuffer>) ByteBuffer::limit)
                .thenComparing(ByteBuffer::compareTo)
                .compare(bb1, bb2)
        );
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + ByteBufferCompare.class.getSimpleName() + ".*")
                .warmupIterations(3)
                .measurementIterations(3)
                .threads(1)
                .forks(0)
//                .addProfiler(LinuxPerfAsmProfiler.class)
                .build();

        new Runner(opt).run();
    }
}
