package org.crustee.raft.utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class UncheckedIOUtils {

    public static long write(FileChannel channel, ByteBuffer[] buffers) {
        try {
            return channel.write(buffers, 0, buffers.length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static int write(FileChannel channel, ByteBuffer buffer, long position) {
        try {
            return channel.write(buffer, position);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static int write(FileChannel channel, ByteBuffer buffer) {
        try {
            return channel.write(buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static long read(FileChannel channel, ByteBuffer[] buffers) {
        try {
            return channel.read(buffers);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static int read(FileChannel channel, ByteBuffer buffer) {
        try {
            return channel.read(buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static int read(FileChannel channel, ByteBuffer buffer, long position) {
        try {
            return channel.read(buffer, position);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void close(FileChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void fsync(FileChannel channel, boolean metadata) {
        try {
            channel.force(metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void fsyncDir(Path path) {
        try(FileChannel channel = openChannel(path, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static long size(FileChannel channel) {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static long position(FileChannel channel) {
        try {
            return channel.position();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static FileChannel setPosition(FileChannel channel, long position) {
        try {
            return channel.position(position);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path tempFile() {
        try {
            return Files.createTempFile(null, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static RandomAccessFile openRandomAccessFile(Path path, String mode) {
        try {
            return new RandomAccessFile(path.toFile(), mode);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static FileChannel openChannel(Path path, OpenOption... openOption) {
        try {
            return FileChannel.open(path, openOption);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ByteBuffer readAllToBuffer(Path path) {
        try {
            return ByteBuffer.wrap(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
