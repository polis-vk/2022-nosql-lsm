package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, Entry<String>> {
    public static final String DATA_EXT = ".dat";
    public static final String INDEX_EXT = ".ind";
    private static final String ALL_FILES = "files.fl";
    private static final Random rnd = new Random();
    private final ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private final Path basePath;
    private volatile boolean commit;
    private final Deque<String> filesList = new ArrayDeque<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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

    @Override
    public Iterator<Entry<String>> get(String from, String to) throws IOException {
        String start = from;
        if (start == null) {
            start = "";
        }
        Iterator<Entry<String>> dataIterator;
        lock.readLock().lock();
        try {
            if (to == null) {
                dataIterator = data.tailMap(start).values().iterator();
            } else {
                dataIterator = data.subMap(start, to).values().iterator();
            }
        } finally {
            lock.readLock().unlock();
        }
        List<PeekingIterator> iterators = new ArrayList<>();
        int priority = 0;
        iterators.add(new PeekingIterator(dataIterator, priority++));
        for (String file : filesList) {
            iterators.add(new PeekingIterator(new FileIterator(basePath, file, start, to), priority++));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public Entry<String> get(String key) throws IOException {
        lock.readLock().lock();
        try {
            Entry<String> entry = data.get(key);
            if (entry != null) {
                return getRealEntry(entry);
            }
            for (String file : filesList) {
                try (RandomAccessFile raf = new RandomAccessFile(basePath.resolve(file + DATA_EXT).toString(), "r")) {
                    entry = Utils.findCeilEntry(raf, key, basePath.resolve(file + INDEX_EXT));
                }
                if (entry != null && entry.key().equals(key)) {
                    break;
                } else {
                    entry = null;
                }
            }
            return getRealEntry(entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Entry<String> getRealEntry(Entry<String> entry) {
        if (entry != null && entry.value() == null) {
            return null;
        }
        return entry;
    }

    @Override
    public void upsert(Entry<String> entry) {
        lock.readLock().lock();
        try {
            data.put(entry.key(), entry);
            commit = false;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void loadFilesList() throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(basePath.resolve(ALL_FILES).toString(), "r")) {
            while (reader.getFilePointer() < reader.length()) {
                String file = reader.readUTF();
                filesList.addFirst(file);
            }
        } catch (FileNotFoundException ignored) {
            //it is ok because there can be no files
        }
    }

    @Override
    public void flush() throws IOException {
        if (commit) {
            return;
        }
        String name;
        do {
            name = generateString();
        } while (filesList.contains(name));
        Path file = basePath.resolve(name + DATA_EXT);
        Path index = basePath.resolve(name + INDEX_EXT);
        filesList.addFirst(name);

        lock.writeLock().lock();
        try (RandomAccessFile output = new RandomAccessFile(file.toString(), "rw");
             RandomAccessFile indexOut = new RandomAccessFile(index.toString(), "rw");
             RandomAccessFile allFilesOut = new RandomAccessFile(basePath.resolve(ALL_FILES).toString(), "rw")
        ) {
            output.seek(0);
            output.writeInt(data.size());
            for (Entry<String> value : data.values()) {
                indexOut.writeLong(output.getFilePointer());
                Utils.writeEntry(output, value);
            }
            allFilesOut.setLength(0);
            for (Iterator<String> filesIterator = filesList.descendingIterator(); filesIterator.hasNext(); ) {
                allFilesOut.writeUTF(filesIterator.next());
            }
            commit = true;
            data.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }


    @Override
    public void compact() throws IOException {
        Iterator<Entry<String>> iterator = all();
        String name;
        do {
            name = generateString();
        } while (filesList.contains(name));
        Path file = basePath.resolve(name + DATA_EXT);
        Path index = basePath.resolve(name + INDEX_EXT);
        lock.writeLock().lock();
        try (RandomAccessFile output = new RandomAccessFile(file.toString(), "rw");
             RandomAccessFile indexOut = new RandomAccessFile(index.toString(), "rw");
             RandomAccessFile allFilesOut = new RandomAccessFile(basePath.resolve(ALL_FILES).toString(), "rw")
        ) {
            output.seek(Integer.BYTES);
            output.writeInt(data.size());
            int count = 0;
            while (iterator.hasNext()) {
                Entry<String> entry = iterator.next();
                count++;
                if (entry != null) {
                    indexOut.writeLong(output.getFilePointer());
                    Utils.writeEntry(output, entry);
                }
            }
            output.seek(0);
            output.writeInt(count);

            allFilesOut.setLength(0);
            allFilesOut.writeUTF(name);
            data.clear();

            Deque<String> filesListCopy = new ArrayDeque<>(filesList);
            filesList.clear();
            filesList.add(name);

            CompletableFuture.runAsync(() -> {
                IOException exception = null;
                for (String fileToDelete : filesListCopy) {
                    try {
                        Files.deleteIfExists(basePath.resolve(fileToDelete + DATA_EXT));
                        Files.deleteIfExists(basePath.resolve(fileToDelete + INDEX_EXT));
                    } catch (IOException e) {
                        exception = e;
                    }
                }
                if (exception != null) {
                    throw new UncheckedIOException(exception);
                }
            });
            commit = true;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
