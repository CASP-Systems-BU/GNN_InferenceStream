
package org.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class SyntheticNodeSource implements SourceFunction<String> {
    private volatile boolean running = true;
    private final AtomicLong baseTimestamp;
    private final Random random;
    private static final int MAX_NODE_ID = 232964; // max node ID
    private final int numPerBurst;

    public SyntheticNodeSource(int numPerBurst) {
        this.baseTimestamp = new AtomicLong(System.currentTimeMillis());
        this.random = new Random(0L);
        this.numPerBurst = numPerBurst; //set via constructor
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running) {
            long targetTime = System.nanoTime() + 100_000_000L; // 100ms in nanoseconds

            for (int i = 0; i < numPerBurst; i++) {
                int nodeId = random.nextInt(MAX_NODE_ID + 1); // generating random IDs in range [0, MAX_NODE_ID]
                long eventTime = baseTimestamp.getAndAdd(200);
                String message = nodeId + "," + eventTime;

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(message);  // ensuring messages are actually emitted
                }
            }

            while (System.nanoTime() < targetTime) {
                Thread.onSpinWait(); // remove unnecessary sleeping
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
