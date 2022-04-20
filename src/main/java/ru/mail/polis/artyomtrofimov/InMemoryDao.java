package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, Entry<String>> {
    public static final String DATA_EXT = ".dat";
    public static final String INDEX_EXT = ".ind";
    private static final String ALL_FILES = "files.fl";
    private static final Random rnd = new Random();
    private ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private volatile boolean commit = true;
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final BlockingDeque<ConcurrentNavigableMap<String, Entry<String>>> queueToFlush = new LinkedBlockingDeque<>(2);
    private final Deque<String> filesList = new ArrayDeque<>();
    private long dataSizeBytes = 0;
    private final AtomicInteger flushCount = new AtomicInteger(0);
    private final Lock filesLock = new ReentrantLock();
    private final Lock writeFileListLock = new ReentrantLock();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService compactExecutor = Executors.newSingleThreadExecutor();

    public InMemoryDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("Config shouldn't be null");
        }
        this.config = config;
        loadFilesList();
        initFlush();
    }

    private void initFlush() {
        flushExecutor.submit(() -> {
            boolean isActive = true;
            while (isActive || !queueToFlush.isEmpty()) {
                String name = getUniqueFileName();
                Path file = config.basePath().resolve(name + DATA_EXT);
                Path index = config.basePath().resolve(name + INDEX_EXT);

                ConcurrentNavigableMap<String, Entry<String>> dataCopy = null;
                try {
                    dataCopy = queueToFlush.take();
                    queueToFlush.addFirst(dataCopy);
                } catch (InterruptedException e) {
                    isActive = false;
                    continue;
                }

                try (RandomAccessFile output = new RandomAccessFile(file.toString(), "rw");
                     RandomAccessFile indexOut = new RandomAccessFile(index.toString(), "rw");
                     RandomAccessFile allFilesOut = new RandomAccessFile(config.basePath().resolve(ALL_FILES).toString(), "rw")
                ) {
                    output.seek(0);
                    output.writeInt(dataCopy.size());
                    for (Entry<String> value : dataCopy.values()) {
                        indexOut.writeLong(output.getFilePointer());
                        Utils.writeEntry(output, value);
                    }

                    filesLock.lock();
                    try {
                        filesList.addFirst(name);
                    } finally {
                        filesLock.unlock();
                        queueToFlush.removeFirst();
                    }

                    writeFileListLock.lock();
                    try {
                        writeFileListToDisk(allFilesOut);
                    } finally {
                        writeFileListLock.unlock();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    private void writeFileListToDisk(RandomAccessFile allFilesOut) throws IOException {
        // newer is at the end
        allFilesOut.setLength(0);
        Iterator<String> filesListIterator = filesList.descendingIterator();
        while (filesListIterator.hasNext()) {
            allFilesOut.writeUTF(filesListIterator.next());
        }
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
        ArrayList<PeekingIterator> iterators = new ArrayList<>(filesList.size() + 1);
        int priority = 0;
        iterators.add(getMemoryPeekingIterator(data, start, to, priority));
        ++priority;

        var storageInQueue = queueToFlush.descendingIterator();
        while (storageInQueue.hasNext()) {
            iterators.add(getMemoryPeekingIterator(storageInQueue.next(), start, to, priority));
            priority++;
        }
        iterators.addAll(getFilePeekingIteratorList(start, to, priority).second);

        return new MergeIterator(iterators);
    }

    private Pair<List<String>, List<PeekingIterator>> getFilePeekingIteratorList(String from, String to, int startPriority) throws IOException {
        List<PeekingIterator> fileIterators = new ArrayList<>();
        int priority = startPriority;
        filesLock.lock();
        Deque<String> fileListCopy;
        try {
            fileListCopy = new ArrayDeque<>(filesList);
        } finally {
            filesLock.unlock();
        }

        for (String file : fileListCopy) {
            fileIterators.add(new PeekingIterator(new FileIterator(config.basePath(), file, from, to), priority));
            ++priority;
        }
        return new Pair(fileListCopy, fileIterators);
    }


    private PeekingIterator getMemoryPeekingIterator(ConcurrentNavigableMap<String, Entry<String>> storage,
                                                     String from, String to, int priority) {
        Iterator<Entry<String>> dataIterator;
        if (to == null) {
            dataIterator = storage.tailMap(from).values().iterator();
        } else {
            dataIterator = storage.subMap(from, to).values().iterator();
        }
        return new PeekingIterator(dataIterator, priority);
    }

    @Override
    public Entry<String> get(String key) throws IOException {
        Entry<String> entry = data.get(key);
        if (entry != null) {
            return getRealEntry(entry);
        }
        for (ConcurrentNavigableMap<String, Entry<String>> storage : queueToFlush) {
            entry = storage.get(key);
            if (entry != null) {
                return getRealEntry(entry);
            }
        }
        filesLock.lock();
        Deque<String> filesListCopy;
        try {
            filesListCopy = new ArrayDeque<>(filesList);
        } finally {
            filesLock.unlock();
        }
        for (String file : filesListCopy) {
            try (RandomAccessFile raf = new RandomAccessFile(config.basePath().resolve(file + DATA_EXT).toString(), "r")) {
                entry = Utils.findCeilEntry(raf, key, config.basePath().resolve(file + INDEX_EXT));
            }
            if (entry != null && entry.key().equals(key)) {
                break;
            } else {
                entry = null;
            }
        }
        return getRealEntry(entry);
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
            dataSizeBytes += 2L * entry.key().getBytes().length + (entry.value() != null ? entry.value().getBytes().length : 0);
            commit = false;
        } finally {
            lock.readLock().unlock();
        }
        try {
            if (dataSizeBytes > config.flushThresholdBytes()) {
                flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void loadFilesList() throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(config.basePath().resolve(ALL_FILES).toString(), "r")) {
            while (reader.getFilePointer() < reader.length()) {
                String file = reader.readUTF();
                filesList.addFirst(file);
            }
        } catch (FileNotFoundException ignored) {
            //it is ok because there can be no files
        }
    }

    @Override
    public void close() throws IOException {
        flush();

        flushExecutor.shutdownNow();
        compactExecutor.shutdownNow();

        try {
            flushExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            compactExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void flush() throws IOException {
        if (commit) {
            return;
        }
        if (queueToFlush.size() == 2) {
            throw new IOException("Exceeded limit");
        }
        queueToFlush.add(data);

        lock.writeLock().lock();
        try {
            data = new ConcurrentSkipListMap<>();
            dataSizeBytes = 0;
            commit = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (filesList.size() <= 1) {
            return;
        }
        compactExecutor.submit(() -> {
            String name = getUniqueFileName();
            Path file = config.basePath().resolve(name + DATA_EXT);
            Path index = config.basePath().resolve(name + INDEX_EXT);
            try (RandomAccessFile output = new RandomAccessFile(file.toString(), "rw");
                 RandomAccessFile indexOut = new RandomAccessFile(index.toString(), "rw");
                 RandomAccessFile allFilesOut = new RandomAccessFile(config.basePath().resolve(ALL_FILES).toString(), "rw")
            ) {
                Pair<List<String>, List<PeekingIterator>> files = getFilePeekingIteratorList(null, null, 0);
                Iterator<Entry<String>> iterator = new MergeIterator(files.second);
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
                filesLock.lock();
                try {
                    filesList.removeAll(files.first);
                    filesList.add(name);
                } finally {
                    filesLock.unlock();
                }

                writeFileListLock.lock();
                try {
                    writeFileListToDisk(allFilesOut);
                } finally {
                    writeFileListLock.unlock();
                    Utils.removeOldFiles(config, files.first);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private String getUniqueFileName() {
        String name;
        do {
            name = generateString();
        } while (filesList.contains(name));
        return name;
    }
}
