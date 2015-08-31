package org.crustee.raft.utils;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;

public class UncheckedIOUtils {

    public static long write(GatheringByteChannel channel, ByteBuffer[] buffers) {
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

    public static int write(WritableByteChannel channel, ByteBuffer buffer) {
        try {
            return channel.write(buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void flush(Flushable flushable) {
        try {
            flushable.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static long read(ScatteringByteChannel channel, ByteBuffer[] buffers) {
        try {
            return channel.read(buffers);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static int read(ReadableByteChannel channel, ByteBuffer buffer) {
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

    public static void close(Closeable closeable) {
        try {
            closeable.close();
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
        try (FileChannel channel = openChannel(path, READ)) {
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
            return Files.createTempFile(Paths.get("/tmp/crustee/"), null, null);
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

    public static MappedByteBuffer map(Path path) {
        try(FileChannel channel = FileChannel.open(path, READ)) {
            return channel.map(READ_ONLY, 0, channel.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
