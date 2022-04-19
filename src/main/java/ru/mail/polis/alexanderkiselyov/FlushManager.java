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
    private NavigableMap<byte[], BaseEntry<byte[]>> pairs;

    public FlushManager(PairsWrapper pairsWrapper, FileOperations fileOperations) {
        flushService = Executors.newSingleThreadExecutor();
        taskResults = new ArrayList<>();
        pairNum = new AtomicInteger(0);
        this.fileOperations = fileOperations;
        this.pairsWrapper = pairsWrapper;
    }

    public void performBackgroundFlush(NavigableMap<byte[], BaseEntry<byte[]>> pairs, AtomicInteger pairNum) {
        this.pairs = pairs;
        this.pairNum.set(pairNum.get());
        taskResults.add(flushService.submit(() -> {
            try {
                fileOperations.flush(pairs);
            } catch (IOException e) {
                e.printStackTrace();
            }
            pairsWrapper.clearPair(pairNum);
        }));
    }

    public NavigableMap<byte[], BaseEntry<byte[]>> getFlushPairs() {
        return pairs;
    }

    public void closeFlushThreadService() {
        flushService.shutdown();
        for (Future<?> taskResult : taskResults) {
            if (taskResult != null && !taskResult.isDone()) {
                try {
                    taskResult.get();
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        taskResults.clear();
        if (pairs != null) {
            pairs.clear();
        }
    }
}
