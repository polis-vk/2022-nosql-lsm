package ru.mail.polis.alexanderkiselyov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CompactManager {
    private final FileOperations fileOperations;
    private final ExecutorService compactService;
    private final List<Future<?>> taskResults;
    private final Logger logger;

    public CompactManager(FileOperations fileOperations) {
        compactService = Executors.newSingleThreadExecutor(r -> new Thread(r, "Compact"));
        this.fileOperations = fileOperations;
        logger = LoggerFactory.getLogger(CompactManager.class);
        taskResults = new ArrayList<>();
    }

    public synchronized void performCompact(boolean hasPairs) {
        taskResults.add(compactService.submit(() -> {
            Iterator<BaseEntry<byte[]>> iterator;
            try {
                iterator = new TombstoneFilteringIterator(
                        MergeIterator.of(fileOperations.diskIterators(null, null), EntryKeyComparator.INSTANCE)
                );
                if (!iterator.hasNext()) {
                    return;
                }
                fileOperations.compact(iterator, hasPairs);
            } catch (IOException e) {
                logger.error("Compact operation was interrupted.", e);
            }
        }));
    }

    public void closeCompactThreadService() {
        compactService.shutdown();
        for (Future<?> taskResult : taskResults) {
            if (taskResult != null && !taskResult.isDone()) {
                try {
                    taskResult.get();
                } catch (ExecutionException | InterruptedException e) {
                    logger.error("Compact termination was interrupted.", e);
                }
            }
        }
        taskResults.clear();
    }
}
