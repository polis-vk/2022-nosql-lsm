package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static ru.mail.polis.nikitadergunov.Storage.FILE_EXT;
import static ru.mail.polis.nikitadergunov.Storage.FILE_NAME;
import static ru.mail.polis.nikitadergunov.Storage.LOW_PRIORITY_FILE;
import static ru.mail.polis.nikitadergunov.Storage.maxPriorityFile;

public class ConcurrencyWrapperStorage implements Closeable {
    public static volatile Thread flushingThread;
    public static volatile Thread compactThread;
    public static volatile boolean isCompacted;
    private static final ReentrantLock mutex = new ReentrantLock();

    private final Storage storage;

    public ConcurrencyWrapperStorage(Storage storage) {
        this.storage = storage;
    }

    public Storage getStorage() {
        return storage;
    }

    static void flush(Config config,
                      ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory) {
        Runnable flushRun = () -> {
            mutex.lock();
            try {
                Storage.save(config, memory.values());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                mutex.unlock();
            }
        };
        flushingThread = new Thread(flushRun);
        flushingThread.start();
        isCompacted = false;
        flushingThread = null;
    }

    static void compact(Config config,
                        Storage previousState) {
        if (previousState.sstables.size() < 2 || isCompacted) {
            return;
        }
        Runnable compactRun = () -> {
            mutex.lock();
            try {
                compactTask(config, previousState);
            } finally {
                mutex.unlock();
            }
        };

        compactThread = new Thread(compactRun);
        compactThread.start();
        isCompacted = true;
        compactThread = null;
    }

    private static void compactTask(Config config,
                                    Storage previousState) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(previousState.iterate(null, null));
        Iterator<Entry<MemorySegment>> mergeIterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);
        Iterator<Entry<MemorySegment>> entriesIterator = new MemorySegmentDao.TombstoneFilteringIterator(mergeIterator);
        List<Entry<MemorySegment>> entries = new ArrayList<>();
        while (entriesIterator.hasNext()) {
            entries.add(entriesIterator.next());
        }
        if (entries.isEmpty()) {
            return;
        }
        try (Stream<Path> listFiles = Files.list(config.basePath())) {
            Storage.save(config, entries);
            Path sstablePathOld = config.basePath().resolve(FILE_NAME + maxPriorityFile + FILE_EXT);
            Path sstablePathNew = config.basePath().resolve(FILE_NAME + LOW_PRIORITY_FILE + FILE_EXT);
            listFiles.filter(path -> !path.equals(sstablePathOld))
                    .forEach(ConcurrencyWrapperStorage::deleteSstables);
            Files.move(sstablePathOld, sstablePathNew, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteSstables(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (flushingThread != null) {
                flushingThread.join();
            }
            if (compactThread != null) {
                compactThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        storage.close();
    }

}
