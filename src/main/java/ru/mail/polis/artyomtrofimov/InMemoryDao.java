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
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, Entry<String>> {
    private static final String FILENAME = "db.dat";
    private static final String ALL_FILES = "files.fl";
    private static final String INDEX_FILE_NAME = "index.ind";
    private static final String DATA_EXT = ".dat";
    private static final String INDEX_EXT = ".ind";
    private static final Random rnd = new Random();
    private final ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private final Path basePath;
    private volatile boolean commit;
    private final Deque<String> filesList = new LinkedList<>();

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
            String line;
            int size = input.readInt();
            long left = 0;
            long right = size;
            long mid;
            while (left < right) {
                mid = left + (right - left) / 2;
                indexInput.seek(mid * Long.BYTES);
                input.seek(indexInput.readLong());
                line = input.readUTF();
                int delimiterIndex = line.indexOf(' ');
                if (delimiterIndex == -1) {
                    continue;
                }
                int keyLength = Integer.parseInt(line, 0, delimiterIndex, 10);
                int entryDelimiter = delimiterIndex + keyLength + 1;
                String currentKey = line.substring(delimiterIndex + 1, entryDelimiter);
                int keyComparing = key.compareTo(currentKey);
                if (keyComparing == 0) {
                    return new BaseEntry<>(currentKey, line.substring(entryDelimiter));
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

        List<ExtendedIterator> iterators = new LinkedList<>();
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
             RandomAccessFile allFilesOut = new RandomAccessFile(basePath.resolve(ALL_FILES).toString(), "rw");
        ) {
            output.seek(0);
            StringBuilder result = new StringBuilder();
            output.writeInt(data.size());
            for (Entry<String> value : data.values()) {
                result.append(value.key().length()).append(' ').append(value.key()).append(value.value());
                indexOut.writeLong(output.getFilePointer());
                output.writeUTF(result.toString());
                result.setLength(0);
            }

            allFilesOut.setLength(0);
            while (!filesList.isEmpty()) {
                allFilesOut.writeUTF(filesList.pollLast());
            }

        }
        commit = true;
    }

    private static class MergeIterator implements Iterator<Entry<String>> {

        private final List<ExtendedIterator> iterators;
        private Entry<String> nextEntry;

        public MergeIterator(List<ExtendedIterator> iterators) {
            this.iterators = iterators;
        }

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
            nextEntry = null;
            for (ExtendedIterator iterator : iterators) {
                if (nextEntry == null || iterator.peek().key().compareTo(nextEntry.key()) < 0) {
                    nextEntry = iterator.peek();
                }
            }
            for (ExtendedIterator iterator : iterators) {
                if (iterator.peek().key().compareTo(nextEntry.key()) == 0) {
                    iterator.next();
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
            if (currentEntry == null) {
                if (iterator.hasNext()) {
                    currentEntry = iterator.next();
                }
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
        String from, to;
        private RandomAccessFile raf;
        private long pos = 0;
        private boolean open = false;
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
                open = true;
            } catch (FileNotFoundException | EOFException e) {
                open = false;
            }
        }

        private void findFloorEntry(String name) throws IOException {
            try (RandomAccessFile index = new RandomAccessFile(basePath.resolve(name + INDEX_EXT).toString(), "r")) {
                raf.seek(0);
                String line;
                int size = raf.readInt();
                long left = 0;
                long right = size;
                long mid;
                while (left < right) {
                    mid = left + (right - left) / 2;
                    index.seek(mid * Long.BYTES);
                    long entryPos = index.readLong();
                    raf.seek(entryPos);
                    line = raf.readUTF();
                    int delimiterIndex = line.indexOf(' ');
                    if (delimiterIndex == -1) {
                        continue;
                    }
                    int keyLength = Integer.parseInt(line, 0, delimiterIndex, 10);
                    int entryDelimiter = delimiterIndex + keyLength + 1;
                    String currentKey = line.substring(delimiterIndex + 1, entryDelimiter);
                    int keyComparing = currentKey.compareTo(from);
                    if (keyComparing == 0) {
                        pos = entryPos;
                        this.nextEntry = new BaseEntry<>(currentKey, line.substring(entryDelimiter));
                        break;
                    } else if (keyComparing > 0) {
                        pos = entryPos;
                        this.nextEntry = new BaseEntry<>(currentKey, line.substring(entryDelimiter));
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
            String line;
            try {
                line = raf.readUTF();
                int delimiterIndex = line.indexOf(' ');
                if (delimiterIndex != -1) {
                    int keyLength = Integer.parseInt(line, 0, delimiterIndex, 10);
                    int entryDelimiter = delimiterIndex + keyLength + 1;
                    String currentKey = line.substring(delimiterIndex + 1, entryDelimiter);
                    nextEntry = new BaseEntry<>(currentKey, line.substring(entryDelimiter));
                }
                pos = raf.getFilePointer();
            } catch (EOFException e) {
                nextEntry = null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return retval;
        }
    }
}
