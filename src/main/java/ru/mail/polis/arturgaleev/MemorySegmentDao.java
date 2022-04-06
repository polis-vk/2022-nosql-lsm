package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> dataBase
            = new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
    private final Config config;
    private final DBReader reader;

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        reader = new DBReader(config.basePath());
    }

    // При канкарент если сделать с начала get а затем compose, то есть шанс, что все упадет,
    // тк у файлы по которым entryIterator ходит - исчезнут
    private static Iterator<Entry<MemorySegment>> dataBaseIterator;
    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (from == null && to == null) {
                dataBaseIterator = dataBase.values().iterator();
            } else if (from != null && to == null) {
                dataBaseIterator = dataBase.tailMap(from).values().iterator();
            } else if (from == null) {
                dataBaseIterator = dataBase.headMap(to).values().iterator();
            } else {
                dataBaseIterator = dataBase.subMap(from, to).values().iterator();
            }

            if (reader.getReadersCount() == 0) {
                return dataBaseIterator;
            } else {
                return new MergeIterator<>(
                        new PriorityPeekingIterator<>(0, reader.get(from, to)),
                        new PriorityPeekingIterator<>(1, dataBaseIterator),
                        MemorySegmentComparator.INSTANCE
                );
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        lock.readLock().lock();
        try {
            Entry<MemorySegment> entry = dataBase.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
            return reader.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            dataBase.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @SuppressWarnings("LockNotBeforeTry")
    @Override
    public void compact() throws IOException {
        lock.writeLock().lock();
        if (!dataBase.isEmpty() || reader.getReadersCount() > 1) {
            Path compactionPath = config.basePath().resolve((reader.getBiggestFileId() + 1) + ".txt");
            try (FileDBWriter writer =
                         new FileDBWriter(compactionPath)) {
                writer.writeIterable(() -> get(null, null));
                dataBase.clear();
                try (Stream<Path> files = Files.list(config.basePath())) {
                    for (Path path : files.toList()) {
                        if (!path.equals(compactionPath)) {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    }
                }
            } finally {
                reader.updateReadersList();
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        if (!dataBase.isEmpty()) {
            try (FileDBWriter writer =
                         new FileDBWriter(config.basePath().resolve((reader.getBiggestFileId() + 1) + ".txt"))) {
                writer.writeIterable(dataBase.values());
            } finally {
                reader.updateReadersList();
                lock.writeLock().unlock();
            }
        }
    }
}
