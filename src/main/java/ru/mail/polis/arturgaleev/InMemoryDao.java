package ru.mail.polis.arturgaleev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.test.arturgaleev.DBReader;
import ru.mail.polis.test.arturgaleev.FileDBWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> dataBase = new ConcurrentSkipListMap<>();
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    //private static final AtomicInteger newFileNumber = new AtomicInteger(1);
    private int newFileNumber;
    private final DBReader reader;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        if (!Files.isDirectory(config.basePath())) {
            Files.createDirectories(config.basePath());
        }
        reader = new DBReader(config.basePath());
        String[] temp = new File(String.valueOf(config.basePath())).list();
        if (temp != null) {
            newFileNumber = temp.length;
        } else {
            newFileNumber = 0;
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        lock.readLock().lock();
        try {
            if (from == null && to == null) {
                return new PeakingIterator(
                        new PriorityIterator<>(1, dataBase.values().iterator()),
                        new PriorityIterator<>(0, reader.get(null, null))
                );
            }
            if (from != null && to == null) {
                return new PeakingIterator(
                        new PriorityIterator<>(1, dataBase.tailMap(from).values().iterator()),
                        new PriorityIterator<>(0, reader.get(from, to))
                );
            }
            if (from == null) {
                return new PeakingIterator(
                        new PriorityIterator<>(1, dataBase.headMap(to).values().iterator()),
                        new PriorityIterator<>(0, reader.get(from, to))
                );
            }
            return new PeakingIterator(
                    new PriorityIterator<>(1, dataBase.subMap(from, to).values().iterator()),
                    new PriorityIterator<>(0, reader.get(from, to))
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> entry = dataBase.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }

            return reader.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        lock.readLock().lock();
        try {
            dataBase.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            try (FileDBWriter writer = new FileDBWriter(config.basePath().resolve(newFileNumber + ".txt"))) {
                writer.writeMap(dataBase);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            newFileNumber++;
            lock.writeLock().unlock();
        }
    }

    public class PriorityIterator<E> implements Iterator<E> {
        private final int priority;
        private final Iterator<E> delegate;
        private E current = null;

        private PriorityIterator(int priority, Iterator<E> delegate) {
            this.priority = priority;
            this.delegate = delegate;
        }

        public int getPriority() {
            return priority;
        }

        public E peek() {
            if (current == null) {
                current = delegate.next();
            }
            return current;
        }

        @Override
        public boolean hasNext() {
            return current != null || delegate.hasNext();
        }

        @Override
        public E next() {
            E peek = peek();
            current = null;
            return peek;
        }
    }

    private class PeakingIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final PriorityBlockingQueue<PriorityIterator<BaseEntry<ByteBuffer>>> iteratorsQueue;
        private BaseEntry<ByteBuffer> current = null;

        public PeakingIterator(PriorityIterator<BaseEntry<ByteBuffer>> inFilesIterator,
                               PriorityIterator<BaseEntry<ByteBuffer>> inMemoryIterator
        ) {

            iteratorsQueue = new PriorityBlockingQueue<>(2,
                    (PriorityIterator<BaseEntry<ByteBuffer>> it1, PriorityIterator<BaseEntry<ByteBuffer>> it2) -> {
                        if (it1.peek().key().compareTo(it2.peek().key()) < 0) {
                            return -1;
                        } else if (it1.peek().key().compareTo(it2.peek().key()) == 0) {
                            return Integer.compare(it1.getPriority(), it2.getPriority());
                        } else {
                            return 1;
                        }
                    }
            );
            if (inMemoryIterator.hasNext()) {
                iteratorsQueue.put(new PriorityIterator<>(1, inMemoryIterator));
            }
            if (inFilesIterator.hasNext()) {
                iteratorsQueue.put(new PriorityIterator<>(0, inFilesIterator));
            }
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                try {
                    current = peek();
                } catch (IndexOutOfBoundsException e) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (current != null) {
                BaseEntry<ByteBuffer> prev = current;
                current = null;
                return prev;
            }
            if (iteratorsQueue.isEmpty()) {
                throw new IndexOutOfBoundsException();
            }

            BaseEntry<ByteBuffer> entry = getNotRemovedDeletedElement();

            if (entry.value() == null) {
                throw new IndexOutOfBoundsException();
            }
            return entry;
        }

        public String toString(ByteBuffer in) {
            ByteBuffer data = in.asReadOnlyBuffer();
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private BaseEntry<ByteBuffer> getNotRemovedDeletedElement() {
            PriorityIterator<BaseEntry<ByteBuffer>> iterator = iteratorsQueue.poll();
            BaseEntry<ByteBuffer> entry = iterator.next();
            if (iterator.hasNext()) {
                iteratorsQueue.put(iterator);
            }
            removeElementsWithKey(entry.key());

            while (!iteratorsQueue.isEmpty() && entry.value() == null) {
                iterator = iteratorsQueue.poll();
                entry = iterator.next();
                if (iterator.hasNext()) {
                    iteratorsQueue.put(iterator);
                }
                removeElementsWithKey(entry.key());
            }
            return entry;
        }

        private void removeElementsWithKey(ByteBuffer lastKey) {
            while (!iteratorsQueue.isEmpty() && lastKey.equals(iteratorsQueue.peek().peek().key())) {
                PriorityIterator<BaseEntry<ByteBuffer>> poll = iteratorsQueue.poll();
                if (poll.hasNext()) {
                    BaseEntry<ByteBuffer> entry = poll.next();
                    if (poll.hasNext()) {
                        iteratorsQueue.put(poll);
                    }
                }
            }
        }

        public BaseEntry<ByteBuffer> peek() {
            if (current == null) {
                current = next();
            }
            return current;
        }
    }
}
