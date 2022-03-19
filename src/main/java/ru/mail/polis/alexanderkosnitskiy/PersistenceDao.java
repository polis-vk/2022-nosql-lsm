package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String FILE = "data";
    private static final String INDEX = "index";
    private static final String EXTENSION = ".anime";
    private final Config config;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> memory = new ConcurrentSkipListMap<>();
    private long amountOfFiles;

    public PersistenceDao(Config config) throws IOException {
        long numberOfFiles;
        this.config = config;
        try (Stream<Path> files = Files.list(config.basePath())) {
            if (files == null) {
                numberOfFiles = 0;
            } else {
                numberOfFiles = files.count() / 2;
            }
        } catch (NoSuchFileException e) {
            numberOfFiles = 0;
        }
        this.amountOfFiles = numberOfFiles;
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = memory.get(key);
        if (result != null) {
            if (result.value() == null) {
                return null;
            }
            return result;
        }
        return findInFiles(key);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if(amountOfFiles == 0) {
            return getMemory(from, to);
        }
        return new MergeIterator(from, to);
    }

    private Iterator<BaseEntry<ByteBuffer>> getMemory(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return memory.values().iterator();
        }
        if (from == null) {
            return memory.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return memory.tailMap(from, true).values().iterator();
        }
        return memory.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        memory.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        store();
        memory.clear();
    }

    private void store() throws IOException {
        try (DaoWriter out = new DaoWriter(config.basePath().resolve(FILE + amountOfFiles + EXTENSION),
                config.basePath().resolve(INDEX + amountOfFiles + EXTENSION))) {
            out.writeMap(memory);
            amountOfFiles++;
        }
    }

    private BaseEntry<ByteBuffer> findInFiles(ByteBuffer key) {
        BaseEntry<ByteBuffer> result;
        for (long i = amountOfFiles - 1; i >= 0; i--) {
            result = findInFile(key, i);
            if (result != null) {
                if (result.value() == null) {
                    return null;
                }
                return result;
            }
        }
        return null;
    }

    private BaseEntry<ByteBuffer> findInFile(ByteBuffer key, long num) {
        try {
            DaoReader finder = new DaoReader(config.basePath().resolve(FILE + num + EXTENSION),
                    config.basePath().resolve(INDEX + num + EXTENSION));
            return finder.binarySearch(key);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    private class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final PriorityQueue<PriorityConstruction> queue;
        private final Iterator<BaseEntry<ByteBuffer>> memIter;
        private final List<FileIterator> list;
        private BaseEntry<ByteBuffer> nextElement;

        public MergeIterator(ByteBuffer from, ByteBuffer to) {
            list = new ArrayList<>();
            memIter = getMemory(from, to);
            for (long i = amountOfFiles - 1; i >= 0; i--) {
                list.add(new FileIterator(
                        config.basePath().resolve(FILE + i + EXTENSION),
                        config.basePath().resolve(INDEX + i + EXTENSION),
                        from, to));
            }
            queue = new PriorityQueue<>((l, r) -> {
                if (l.entry.key().compareTo(r.entry.key()) > 0) {
                    return 1;
                }
                if (l.entry.key().compareTo(r.entry.key()) < 0) {
                    return -1;
                }
                return Integer.compare(l.index, r.index);
            });
            if (memIter.hasNext()) {
                queue.add(new PriorityConstruction(0, memIter.next()));
            }
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).peek() != null) {
                    queue.add(new PriorityConstruction(i + 1, list.get(i).next()));
                }
            }
            nextElement = getNextElement();
        }

        @Override
        public boolean hasNext() {
            return nextElement != null;
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (!hasNext()) {
                throw new UnsupportedOperationException();
            }
            BaseEntry<ByteBuffer> temp = nextElement;
            nextElement = getNextElement();
            return temp;
        }

        private BaseEntry<ByteBuffer> getNextElement() {
            if (queue.isEmpty()) {
                return null;
            }
            PriorityConstruction curr = queue.remove();
            if (curr.index == 0) {
                if (memIter.hasNext()) {
                    queue.add(new PriorityConstruction(0, memIter.next()));
                }
            } else if (list.get(curr.index - 1).hasNext()) {
                queue.add(new PriorityConstruction(curr.index, list.get(curr.index - 1).next()));
            }
            PriorityConstruction similar;
            while (!queue.isEmpty() && queue.peek().entry.key().equals(curr.entry.key())) {
                similar = queue.remove();
                if (similar.index == 0) {
                    if (memIter.hasNext()) {
                        queue.add(new PriorityConstruction(0, memIter.next()));
                    }
                } else if (list.get(similar.index - 1).hasNext()) {
                    queue.add(new PriorityConstruction(similar.index, list.get(similar.index - 1).next()));
                }
            }
            if (curr.entry.value() == null) {
                return getNextElement();
            }
            return curr.entry;
        }

        private static class PriorityConstruction {
            int index;
            BaseEntry<ByteBuffer> entry;

            PriorityConstruction(int index, BaseEntry<ByteBuffer> entry) {
                this.index = index;
                this.entry = entry;
            }
        }
    }
}
