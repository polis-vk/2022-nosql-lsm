package ru.mail.polis.alexanderkiselyov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CompactManager {
    private final BlockingQueue<Boolean> compactQueue;
    private final FileOperations fileOperations;
    private final ScheduledExecutorService compactService;
    private final ScheduledFuture<?> compactResult;
    private final Logger logger;

    public CompactManager(FileOperations fileOperations) {
        compactQueue = new LinkedBlockingQueue<>();
        compactService = Executors.newSingleThreadScheduledExecutor();
        this.fileOperations = fileOperations;
        logger = LoggerFactory.getLogger(CompactManager.class);
        compactResult = compactService.scheduleAtFixedRate(() -> {
            if (compactQueue.size() != 0) {
                try {
                    compactOperation(compactQueue.poll());
                } catch (IOException e) {
                    logger.error("Compact operation was interrupted.", e);
                }
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    public void performCompact(boolean hasPairs) {
        compactQueue.add(hasPairs);
    }

    public void closeCompactThreadService() {
        compactService.shutdown();
        if (compactResult != null && !compactResult.isDone()) {
            try {
                if (!compactService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
                    throw new RuntimeException("Compact operation was terminated.");
                }
            } catch (InterruptedException e) {
                logger.error("Compact termination was interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void compactOperation(boolean hasPairs) throws IOException {
        Iterator<BaseEntry<byte[]>> iterator;
        iterator = new SkipNullValuesIterator(
                new IndexedPeekIterator(0, fileOperations.diskIterator(null, null))
        );
        if (!iterator.hasNext()) {
            return;
        }
        fileOperations.compact(iterator, hasPairs);
    }
}
