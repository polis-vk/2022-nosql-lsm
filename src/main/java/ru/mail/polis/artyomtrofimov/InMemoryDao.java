package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, Entry<String>> {
    private static final String ALL_FILES = "files.fl";
    private static final String DATA_EXT = ".dat";
    private static final String INDEX_EXT = ".ind";
    private static final Random rnd = new Random();
    private final ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private final Path basePath;
    private volatile boolean commit;
    private final Deque<String> filesList = new ArrayDeque<>();

    public InMemoryDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("Config shouldn't be null");
        }
        basePath = config.basePath();
        loadFilesList();
    }

    private static String generateString() {
        char[] chars = new char[rnd.nextInt(8, 9)];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) (rnd.nextInt('z' - '0') + '0');
        }
        return new String(chars);
    }

    private static Entry<String> findInFileByKey(String key, Path basePath, String name) throws IOException {
        if (name == null) {
            return null;
        }
        Path dataPath = basePath.resolve(name + DATA_EXT);
        Path indexPath = basePath.resolve(name + INDEX_EXT);
        try (RandomAccessFile input = new RandomAccessFile(dataPath.toString(), "r");
             RandomAccessFile indexInput = new RandomAccessFile(indexPath.toString(), "r")) {
            input.seek(0);
            int size = input.readInt();
            long left = 0;
            long right = size;
            long mid;
            while (left < right) {
                mid = left + (right - left) / 2;
                indexInput.seek(mid * Long.BYTES);
                input.seek(indexInput.readLong());
                String currentKey = input.readUTF();
                int keyComparing = key.compareTo(currentKey);
                if (keyComparing == 0) {
                    return new BaseEntry<>(currentKey, input.readUTF());
                } else if (keyComparing < 0) {
                    right = mid;
                } else {
                    left = mid;
                }
            }
            return null;
        } catch (FileNotFoundException | EOFException e) {
            return null;
        }
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) throws IOException {
        boolean isFromNull = from == null;
        boolean isToNull = to == null;
        Iterator<Entry<String>> dataIterator;
        if (isFromNull && isToNull) {
            dataIterator = data.values().iterator();
        } else if (isFromNull) {
            dataIterator = data.headMap(to).values().iterator();
        } else if (isToNull) {
            dataIterator = data.tailMap(from).values().iterator();
        } else {
            dataIterator = data.subMap(from, to).values().iterator();
        }

        List<ExtendedIterator> iterators = new ArrayList<>();
        iterators.add(new ExtendedIterator(dataIterator));
        for (String file : filesList) {
            iterators.add(new ExtendedIterator(new FileIterator(basePath, file, from, to)));
        }

        return new MergeIterator(iterators);

    }

    @Override
    public Entry<String> get(String key) throws IOException {
        Entry<String> entry = data.get(key);
        if (entry == null) {
            for (String file : filesList) {
                entry = findInFileByKey(key, basePath, file);
                if (entry != null) {
                    break;
                }
            }
        }
        return entry;
    }

    @Override
    public void upsert(Entry<String> entry) {
        data.put(entry.key(), entry);
        commit = false;
    }

    private void loadFilesList() throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(basePath.resolve(ALL_FILES).toString(), "r")) {
            while (reader.getFilePointer() < reader.length()) {
                String file = reader.readUTF();
                filesList.addFirst(file);
            }
        } catch (NoSuchFileException | EOFException | FileNotFoundException ignored) {
            //it is ok
        }
    }

    @Override
    public void flush() throws IOException {
        if (commit) {
            return;
        }
        String name = generateString();
        Path file = basePath.resolve(name + DATA_EXT);
        Path index = basePath.resolve(name + INDEX_EXT);
        filesList.addFirst(name);

        try (RandomAccessFile output = new RandomAccessFile(file.toString(), "rw");
             RandomAccessFile indexOut = new RandomAccessFile(index.toString(), "rw");
             RandomAccessFile allFilesOut = new RandomAccessFile(basePath.resolve(ALL_FILES).toString(), "rw")
        ) {
            output.seek(0);
            output.writeInt(data.size());
            for (Entry<String> value : data.values()) {
                indexOut.writeLong(output.getFilePointer());
                output.writeUTF(value.key());
                output.writeUTF(value.value());
            }

            allFilesOut.setLength(0);
            while (!filesList.isEmpty()) {
                allFilesOut.writeUTF(filesList.pollLast());
            }

        }
        commit = true;
    }

    private record MergeIterator(List<ExtendedIterator> iterators) implements Iterator<Entry<String>> {

        @Override
        public boolean hasNext() {
            iterators.removeIf(item -> !item.hasNext());
            return !iterators.isEmpty();
        }

        @Override
        public Entry<String> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<String> nextEntry = null;
            for (ExtendedIterator iterator : iterators) {
                if (nextEntry == null || iterator.peek().key().compareTo(nextEntry.key()) < 0) {
                    nextEntry = iterator.peek();
                }
            }
            if (nextEntry != null) {
                for (ExtendedIterator iterator : iterators) {
                    if (iterator.peek().key().compareTo(nextEntry.key()) == 0) {
                        iterator.next();
                    }
                }
            }
            return nextEntry;
        }
    }

    private static class ExtendedIterator implements Iterator<Entry<String>> {

        private final Iterator<Entry<String>> iterator;
        private Entry<String> currentEntry;

        public ExtendedIterator(Iterator<Entry<String>> iterator) {
            this.iterator = iterator;
        }

        public Entry<String> peek() {
            if (currentEntry == null && iterator.hasNext()) {
                currentEntry = iterator.next();
            }
            return currentEntry;
        }

        @Override
        public boolean hasNext() {
            if (currentEntry == null) {
                return iterator.hasNext();
            }
            return true;
        }

        @Override
        public Entry<String> next() {
            try {
                return currentEntry;
            } finally {
                if (iterator.hasNext()) {
                    currentEntry = iterator.next();
                } else {
                    currentEntry = null;
                }
            }
        }
    }

    private static class FileIterator implements Iterator<Entry<String>> {
        private final Path basePath;
        String from;
        String to;
        private RandomAccessFile raf;
        private Entry<String> nextEntry = null;

        public FileIterator(Path basePath, String name, String from, String to) throws IOException {
            this.from = from;
            this.to = to;
            this.basePath = basePath;
            try {
                raf = new RandomAccessFile(basePath.resolve(name + DATA_EXT).toString(), "r");
                if (this.from == null) {
                    this.from = "";
                }
                findFloorEntry(name);
            } catch (FileNotFoundException | EOFException e) {
                nextEntry = null;
            }
        }

        private void findFloorEntry(String name) throws IOException {
            try (RandomAccessFile index = new RandomAccessFile(basePath.resolve(name + INDEX_EXT).toString(), "r")) {
                raf.seek(0);
                int size = raf.readInt();
                long left = 0;
                long right = size;
                long mid;
                while (left < right) {
                    mid = left + (right - left) / 2;
                    index.seek(mid * Long.BYTES);
                    long entryPos = index.readLong();
                    raf.seek(entryPos);
                    String currentKey = raf.readUTF();
                    int keyComparing = currentKey.compareTo(from);
                    if (keyComparing == 0) {
                        this.nextEntry = new BaseEntry<>(currentKey, raf.readUTF());
                        break;
                    } else if (keyComparing > 0) {
                        this.nextEntry = new BaseEntry<>(currentKey, raf.readUTF());
                        right = mid;
                    } else {
                        left = mid;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null && !nextEntry.key().equals(to);
        }

        @Override
        public Entry<String> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<String> retval = nextEntry;
            try {
                String currentKey = raf.readUTF();
                nextEntry = new BaseEntry<>(currentKey, raf.readUTF());
            } catch (EOFException e) {
                nextEntry = null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return retval;
        }
    }
}
