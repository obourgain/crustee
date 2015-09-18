package org.crustee.raft.utils;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;

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
            return Files.createTempFile(Paths.get("/tmp/"), null, null);
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

    /**
     * Return a buffer ready to read.
     */
    public static ByteBuffer readAllToBuffer(Path path) {
        try {
            return ByteBuffer.wrap(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static MappedByteBuffer mapReadOnly(Path path) {
        try(FileChannel channel = FileChannel.open(path, READ)) {
            return channel.map(READ_ONLY, 0, channel.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static MappedByteBuffer mapReadWrite(Path path) {
        try(FileChannel channel = FileChannel.open(path, READ, WRITE)) {
            return channel.map(READ_WRITE, 0, channel.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void createFile(Path path, FileAttribute ... fileAttributes) {
        try {
            Files.createFile(path, fileAttributes);
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

    /**
     * The byte buffer must be of a size sufficient to fully write the content of the channel.
     * e.g. this method can be used to copy the content of a file to a buffer allocated with a size equal to the file length.
     *
     * It is not meant to be fast, only to be convenient.
     *
     * This method flips the buffer, so what was read from the channel is ready to be read.
     */
    public static void copy(ReadableByteChannel channel, ByteBuffer byteBuffer) {
        byte[] buffer = new byte[8 * 1024];
        int read;
        try (InputStream inputStream = Channels.newInputStream(channel)) {
            while ((read = inputStream.read(buffer)) != -1) {
                byteBuffer.put(buffer, 0, read);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        byteBuffer.flip();
    }
}
