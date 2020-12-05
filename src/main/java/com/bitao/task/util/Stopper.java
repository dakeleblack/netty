package com.bitao.task.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class Stopper {

    private static volatile AtomicBoolean signal = new AtomicBoolean(false);

    public static boolean isStopped() {
        return signal.get();
    }

    public static boolean isRunning() {
        return !signal.get();
    }

    public static void stop() {
        signal.getAndSet(true);
    }
}
