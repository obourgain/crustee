package org.crustee.raft.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class UncheckedFutureUtils {

    public static <T> T get(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
