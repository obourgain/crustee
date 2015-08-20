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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class BuffersSizeLoopVersusStream {

    ByteBuffer[] buffersFull = new ByteBuffer[1024];
    ByteBuffer[] buffersHalf = new ByteBuffer[1024];

    public BuffersSizeLoopVersusStream() {
        for (int i = 0; i < buffersFull.length; i++) {
            buffersFull[i] = (ByteBuffer) ByteBuffer.allocate(8).putLong(i).flip();
        }
        int half = buffersHalf.length / 2;
        for (int i = 0; i < half; i++) {
            buffersFull[i] = (ByteBuffer) ByteBuffer.allocate(8).putLong(i).flip();
        }
    }

    @Benchmark
    public long loop_full() {
        long size = 0;
        for (int i = 0; i < buffersFull.length; i++) {
            size += buffersFull[i].limit();
        }
        return size;
    }

    @Benchmark
    public long stream_full() {
        return Arrays.stream(buffersFull).filter(b -> b != null).mapToInt(Buffer::limit).sum();
    }

    @Benchmark
    public long loop_half_full() {
        long size = 0;
        for (int i = 0; i < buffersHalf.length; i++) {
            if(buffersFull[i] == null) {
                return size;
            }
            size += buffersFull[i].limit();
        }
        return size;
    }

    @Benchmark
    public long stream_half_full() {
        return Arrays.stream(buffersFull).filter(b -> b != null).mapToInt(Buffer::limit).sum();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + BuffersSizeLoopVersusStream.class.getSimpleName() + ".*")
                .warmupIterations(10)
                .measurementIterations(10)
                .threads(1)
                .forks(0)
//                .addProfiler(LinuxPerfAsmProfiler.class)
                .build();

        new Runner(opt).run();
    }
}
