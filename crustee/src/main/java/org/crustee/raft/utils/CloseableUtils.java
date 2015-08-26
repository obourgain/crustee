package org.crustee.raft.utils;

public class CloseableUtils {

    public static void close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
