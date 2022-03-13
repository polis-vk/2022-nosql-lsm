package ru.mail.polis.stepanponomarev.log;

import ru.mail.polis.stepanponomarev.TimestampEntry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class AheadLogger implements Closeable {
    private final CommitLog commitLog;
    private final BlockingQueue<TimestampEntry> log;

    private static final TimestampEntry FINAL_ENTRY = new TimestampEntry(null, -1);
    private final ExecutorService executorService;

    public AheadLogger(Path path, long size) throws IOException {
        this.commitLog = new CommitLog(path, size);
        this.log = new LinkedBlockingQueue<>();

        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
                    while (true) {
                        try {
                            final TimestampEntry timedLog = log.take();
                            final boolean finalEntry = timedLog.equals(FINAL_ENTRY);
                            if (finalEntry && log.isEmpty()) {
                                break;
                            }

                            if (finalEntry) {
                                log.put(FINAL_ENTRY);
                            }

                            if (!finalEntry) {
                                commitLog.log(timedLog);
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
        );
        executorService.shutdown();
    }

    public void log(TimestampEntry entry) {
        log.add(entry);
    }

    public void clear(long timestamp) throws IOException {
        log.removeIf(e -> e.getTimestamp() < timestamp);
        commitLog.clean();
    }

    @Override
    public void close() throws IOException {
        try {
            log.put(FINAL_ENTRY);

            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IOException("We are waiting too long.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            if (Thread.interrupted()) {
                throw new IOException("Very strange unexpected exception", e);
            }
        }
    }

    public Iterator<TimestampEntry> load() {
        return commitLog.load();
    }
}
