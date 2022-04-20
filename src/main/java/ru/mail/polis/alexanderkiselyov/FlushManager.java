package ru.mail.polis.alexanderkiselyov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final Logger logger;

    public FlushManager(PairsWrapper pairsWrapper, FileOperations fileOperations) {
        flushService = Executors.newSingleThreadExecutor();
        taskResults = new ArrayList<>();
        pairNum = new AtomicInteger(0);
        this.fileOperations = fileOperations;
        this.pairsWrapper = pairsWrapper;
        logger = LoggerFactory.getLogger(FlushManager.class);
    }

    public void performBackgroundFlush(NavigableMap<byte[], BaseEntry<byte[]>> pairs, AtomicInteger pairNum) {
        this.pairs = pairs;
        this.pairNum.set(pairNum.get());
        taskResults.add(flushService.submit(() -> {
            try {
                fileOperations.flush(pairs);
            } catch (IOException e) {
                logger.error("Flush operation was interrupted.", e);
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
                    logger.error("Flush termination was interrupted.", e);
                }
            }
        }
        taskResults.clear();
        if (pairs != null) {
            pairs.clear();
        }
    }
}
