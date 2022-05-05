package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMap =
            new ConcurrentSkipListMap<>(String::compareTo);
    /*
    Используется для доступа на чтение данных, которые записываются при flush
     */
    private ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMapPointer =
            stringConcurrentSkipListMap;
    private static final String FILE_NAME = "cache";
    private static final String COMPACT_FILE_NAME = "compact";
    private static final int FILLER_FOR_OFFSETS_OFFSET = 0;
    private final Deque<FilePeekIterator> listOfFiles = new ArrayDeque<>();
    private final Path directoryPath;
    private final long flushThresholdBytes;
    private final ExecutorService compactExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean isDoneFlushing = new AtomicBoolean(true);
    private final AtomicLong mapSize = new AtomicLong(0);
    private final Path basePath;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public InMemoryDao(Config config) throws IOException {
        directoryPath = config.basePath();
        flushThresholdBytes = config.flushThresholdBytes();
        basePath = config.basePath();
        getFiles();
    }

    @Override
    public BaseEntry<String> get(String key) {
        BaseEntry<String> resultFromMap = stringConcurrentSkipListMapPointer.get(key);

        if (resultFromMap != null) {
            return resultFromMap.value() == null ? null : resultFromMap;
        }

        for (FilePeekIterator filePeekIterator : listOfFiles) {
            filePeekIterator.setBoundaries(null, null);
            BaseEntry<String> tmp = filePeekIterator.findValueByKey(key);
            if (tmp != null) {
                return tmp.value() == null ? null : tmp;
            }
        }

        return null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        PeekIterator stringMapIterator;
        if (from == null && to == null) {
            stringMapIterator = new PeekIterator(
                    getIterator(stringConcurrentSkipListMapPointer)
            );
        } else if (from == null) {
            stringMapIterator = new PeekIterator(
                    getIterator(stringConcurrentSkipListMapPointer.headMap(to))
            );
        } else if (to == null) {
            stringMapIterator = new PeekIterator(
                    getIterator(stringConcurrentSkipListMapPointer.tailMap(from, true))
            );
        } else {
            stringMapIterator = new PeekIterator(
                    getIterator(stringConcurrentSkipListMapPointer.subMap(from, to))
            );
        }

        Deque<BasePeekIterator> listOfIterators = getFromFiles(from, to);
        if (stringMapIterator.hasNext()) {
            listOfIterators.addFirst(stringMapIterator);
        }

        return listOfIterators.isEmpty() ? Collections.emptyIterator() : new MergeIterator(listOfIterators);
    }

    public Deque<BasePeekIterator> getFromFiles(String from, String to) {
        Deque<BasePeekIterator> listOfIterators = new ArrayDeque<>();
        for (FilePeekIterator filePeekIterator : listOfFiles) {
            filePeekIterator.setBoundaries(from, to);
            if (filePeekIterator.hasNext()) {
                listOfIterators.add(filePeekIterator);
            }
        }
        return listOfIterators;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        if (mapSize.addAndGet(entrySize(entry)) > flushThresholdBytes) {
            if (!isDoneFlushing.get()) {
                throw new IllegalStateException("Previous table is still flushing");
            }
            stringConcurrentSkipListMap = new ConcurrentSkipListMap<>(String::compareTo);
            mapSize.set(0);
            flushExecutor.execute(() -> {
                try {
                    isDoneFlushing.set(false);
                    save(
                            directoryPath.resolve(listOfFiles.size() + FILE_NAME),
                            stringConcurrentSkipListMapPointer,
                            false
                    );
                    getFiles();
                    stringConcurrentSkipListMapPointer = stringConcurrentSkipListMap;
                    isDoneFlushing.set(true);
                } catch (IOException e) {
                    isDoneFlushing.set(true);
                }
            });
        }
        stringConcurrentSkipListMap.put(entry.key(), entry);
    }

    private Iterator<BaseEntry<String>> getIterator(ConcurrentNavigableMap<String, BaseEntry<String>> map) {
        return map.values().iterator();
    }

    @Override
    public void flush() throws IOException {
        save(
                directoryPath.resolve(listOfFiles.size() + FILE_NAME),
                stringConcurrentSkipListMap,
                false
        );
    }

    private void save(Path path, ConcurrentNavigableMap<String, BaseEntry<String>> map, boolean isCompact)
            throws IOException {
        Deque<Integer> offsets = new ArrayDeque<>();
        Deque<Integer> lengths = new ArrayDeque<>();
        long keyValueSize;
        try (DataOutputStream firstWriter
                     = new DataOutputStream(
                new BufferedOutputStream(
                        Files.newOutputStream(path)))) {

            if (map.isEmpty()) {
                Files.delete(path);
                return;
            }

            firstWriter.writeBoolean(isCompact);
            firstWriter.writeInt(map.size());
            firstWriter.writeLong(FILLER_FOR_OFFSETS_OFFSET);

            int lastOffset;
            for (BaseEntry<String> entry : map.values()) {
                offsets.add(firstWriter.size());
                if (entry.value() == null) {
                    firstWriter.writeBoolean(false);
                    lastOffset = firstWriter.size();
                    writeValue(firstWriter, entry.key(), 0);

                } else {
                    firstWriter.writeBoolean(true);
                    lastOffset = firstWriter.size();
                    writeValue(firstWriter, entry.key(), 0);
                    lengths.add(firstWriter.size() - lastOffset - Integer.BYTES);
                    lastOffset = firstWriter.size();
                    writeValue(firstWriter, entry.value(), 0);
                }
                lengths.add(firstWriter.size() - lastOffset - Integer.BYTES);
            }

            keyValueSize = firstWriter.size();
        }

        if (stringConcurrentSkipListMap.values().size() > 10_000) {
            try (DataOutputStream writer
                         = new DataOutputStream(
                    new BufferedOutputStream(
                            Files.newOutputStream(path)))) {
                writeWithBufferedWriter(writer, isCompact, map, keyValueSize, offsets, lengths);
            }
        } else {
            try (RandomAccessFile secondWriter
                         = new RandomAccessFile(path.toString(), "rw")) {
                writeWithRandomAccess(secondWriter, map, keyValueSize, offsets, lengths);
            }
        }
    }

    private void writeWithBufferedWriter(
            DataOutputStream writer,
            boolean isCompact,
            ConcurrentNavigableMap<String, BaseEntry<String>> map,
            long keyValueSize,
            Deque<Integer> offsets,
            Deque<Integer> lengths) throws IOException {
        writer.writeBoolean(isCompact);
        writer.writeInt(map.size());
        writer.writeLong(keyValueSize);
        for (BaseEntry<String> entry : map.values()) {
            if (entry.value() == null) {
                writer.writeBoolean(false);
                writeValue(writer, entry.key(), lengths.pop());
            } else {
                writer.writeBoolean(true);
                writeValue(writer, entry.key(), lengths.pop());
                writeValue(writer, entry.value(), lengths.pop());
            }
        }

        for (long offset : offsets) {
            writer.writeLong(offset);
        }

        writer.writeLong(writer.size() + Long.BYTES);
    }

    private void writeWithRandomAccess(
            RandomAccessFile writer,
            ConcurrentNavigableMap<String, BaseEntry<String>> map,
            long keyValueSize,
            Deque<Integer> offsets,
            Deque<Integer> lengths) throws IOException {
        writer.skipBytes(5);
        writer.writeLong(keyValueSize);
        for (BaseEntry<String> entry : map.values()) {
            writer.skipBytes(1);
            if (entry.value() == null) {
                writeLength(writer, lengths.pop());
            } else {
                writeLength(writer, lengths.pop());
                writeLength(writer, lengths.pop());
            }
        }

        for (long offset : offsets) {
            writer.writeLong(offset);
        }

        writer.writeLong(writer.length() + Long.BYTES);
    }

    @Override
    public void compact() throws IOException {
        compactExecutor.execute(() -> {
            try {
                runCompact();
            } catch (IOException ignored) {
            }
        });
    }

    private void runCompact() throws IOException {
        if (listOfFiles.size() <= 1) {
            return;
        }

        ConcurrentNavigableMap<String, BaseEntry<String>> memoryMap
                = new ConcurrentSkipListMap<>();
        Deque<BasePeekIterator> listMemoryIterators = getFromFiles(null, null);
        Iterator<BaseEntry<String>> iterator =
                listMemoryIterators.isEmpty() ? Collections.emptyIterator() : new MergeIterator(listMemoryIterators);

        while (iterator.hasNext()) {
            BaseEntry<String> entry = iterator.next();
            memoryMap.put(entry.key(), entry);
        }

        for (FilePeekIterator file : listOfFiles) {
            Files.delete(file.getPath());
        }

        save(
                directoryPath.resolve(COMPACT_FILE_NAME),
                memoryMap,
                true
        );
    }

    @Override
    public void close() throws IOException {

        try {
            compactExecutor.shutdown();
            flushExecutor.shutdown();
            compactExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
            flushExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (stringConcurrentSkipListMap.isEmpty()) {
            return;
        } else {
            flush();
        }
    }

    private void writeValue(DataOutputStream writer, String value, int size) throws IOException {
        writer.writeInt(size);
        writer.writeBytes(value);
    }

    private void writeLength(RandomAccessFile writer, int length) throws IOException {
        writer.writeInt(length);
        writer.skipBytes(length);
    }

    private long entrySize(BaseEntry<String> entry) {
        long keySize = entry.key().length() * 2L;
        return entry.value() == null ? keySize : keySize + entry.value().length() * 2L;
    }

    private void getFiles() {
        File[] arrayOfFiles = basePath.toFile().listFiles();
        if (arrayOfFiles != null) {
            Arrays.sort(arrayOfFiles, Comparator.comparingLong(File::lastModified));
            for (int i = arrayOfFiles.length - 1; i >= 0; i--) {
                listOfFiles.add(new FilePeekIterator(arrayOfFiles[i].toPath(), arrayOfFiles.length - i));
            }
        }
    }
}
