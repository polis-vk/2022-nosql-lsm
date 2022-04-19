package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class FlushManager {
    private final ExecutorService flushService;
    private final List<Future<?>> taskResults;
    private final AtomicInteger pairNum;
    private final PairsWrapper pairsWrapper;
    private final FileOperations fileOperations;
    private NavigableMap<Byte[], BaseEntry<Byte[]>> pairs;
    private final Runnable flushTask = new Runnable() {
        @Override
        public void run() {
            try {
                fileOperations.flush(pairs);
                pairsWrapper.clearPair(pairNum);
            } catch (IOException e) {
                throw new RuntimeException("Unable to flush: " + e);
            }
        }
    };

    public FlushManager(PairsWrapper pairsWrapper, FileOperations fileOperations) {
        flushService = Executors.newSingleThreadExecutor();
        taskResults = new ArrayList<>();
        pairNum = new AtomicInteger(0);
        this.fileOperations = fileOperations;
        this.pairsWrapper = pairsWrapper;
    }

    public void performBackgroundFlush(NavigableMap<Byte[], BaseEntry<Byte[]>> pairs, AtomicInteger pairNum) {
        this.pairs = pairs;
        this.pairNum.set(pairNum.get());
        taskResults.add(flushService.submit(flushTask));
    }

    public NavigableMap<Byte[], BaseEntry<Byte[]>> peekFlushQueue() {
        return pairs;
    }

    public void closeFlushThreadService() {
        flushService.shutdown();
        for (Future<?> taskResult : taskResults) {
            if (taskResult != null && !taskResult.isDone()) {
                try {
                    taskResult.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Unable to finish flush tasks: " + e);
                }
            }
        }
        taskResults.clear();
        if (pairs != null) {
            pairs.clear();
        }
    }
}
