package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static ru.mail.polis.lutsenkodmitrii.DaoUtils.preprocess;

/**
 * ----------------------------------------------------------------------------------------------*
 * Описание формата файла.
 * - Минимальный ключ во всем файле
 * - Максимальный ключ во всем файле
 * - 0 - Длина предыдущей entry для первой entry
 * - В цикле для всех entry:
 * - Длина ключа
 * - Ключ
 * - EXISTING_MARK или DELETED_MARK
 * - Значение, если не равно null
 * -'\n'
 * - Длина всего записанного + размер самого числа относительно типа char
 * Пример (пробелы и переносы строк для наглядности):
 * k2 k55
 * 0 2 k2 1 v2 '\n'
 * 10 3 k40 1 v40 '\n'
 * 12 3 k55 1 v5555 '\n'
 * 14 5 ka123 0 '\n'
 * 11
 * ----------------------------------------------------------------------------------------------*
 **/
public class PersistenceRangeDao implements Dao<String, BaseEntry<String>> {

    private static final OpenOption[] writeOptions = new OpenOption[]{
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
    };
    public static final int DELETED_MARK = 0;
    public static final int EXISTING_MARK = 1;
    public static final String DATA_FILE_NAME = "daoData";
    public static final String DATA_FILE_EXTENSION = ".txt";
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean isClosed;
    private final ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();
    private final Config config;
    private long currentFileNumber;

    public PersistenceRangeDao(Config config) throws IOException {
        this.config = config;
        try (Stream<Path> stream = Files.find(config.basePath(), 1,
                (p, a) -> a.isRegularFile() && p.getFileName().toString().endsWith(DATA_FILE_EXTENSION))) {
            currentFileNumber = stream.count();
        } catch (NoSuchFileException e) {
            currentFileNumber = 0;
        }
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        lock.readLock().lock();
        try {
            return new MergeIterator(this, from, to);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        lock.readLock().lock();
        try {
            data.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (data.isEmpty()) {
            return;
        }
        Path dataFilePath = generateNextFilePath();
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(dataFilePath, UTF_8, writeOptions)) {
            DaoUtils.writeUnsignedInt(0, bufferedFileWriter);
            for (BaseEntry<String> baseEntry : data.values()) {
                String key = preprocess(baseEntry.key());
                writeKey(key, bufferedFileWriter);
                int keyWrittenSize = DaoUtils.CHARS_IN_INT + DaoUtils.CHARS_IN_INT + key.length() + 1;
                // +1 из-за DELETED_MARK или EXISTING_MARK
                if (baseEntry.value() == null) {
                    bufferedFileWriter.write(DELETED_MARK + '\n');
                    DaoUtils.writeUnsignedInt(keyWrittenSize, bufferedFileWriter);
                    continue;
                }
                String value = preprocess(baseEntry.value());
                writeValue(value, bufferedFileWriter);
                DaoUtils.writeUnsignedInt(keyWrittenSize + value.length(), bufferedFileWriter);
            }
            bufferedFileWriter.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        lock.writeLock().lock();
        try {
            flush();
            isClosed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (currentFileNumber == 0 || (currentFileNumber == 1 && data.isEmpty())) {
            return;
        }
        Iterator<BaseEntry<String>> allEntriesIterator = get(null, null);
        lock.writeLock().lock();
        Path compactionFilePath = generateNextFilePath();
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(compactionFilePath, UTF_8, writeOptions)) {
            DaoUtils.writeUnsignedInt(0, bufferedFileWriter);
            while (allEntriesIterator.hasNext()) {
                BaseEntry<String> baseEntry = allEntriesIterator.next();
                String key = preprocess(baseEntry.key());
                writeKey(key, bufferedFileWriter);
                String value = preprocess(baseEntry.value());
                writeValue(value, bufferedFileWriter);
                DaoUtils.writeUnsignedInt(DaoUtils.CHARS_IN_INT + DaoUtils.CHARS_IN_INT
                        + key.length() + value.length() + 1, bufferedFileWriter); // +1 из-за EXISTING_MARK
            }
            bufferedFileWriter.flush();
        } finally {
            lock.writeLock().unlock();
        }
        for (int i = 0; i < currentFileNumber; i++) {
            Path oldFile = config.basePath().resolve(DATA_FILE_NAME + i + DATA_FILE_EXTENSION);
            Files.delete(oldFile);
        }
        data.clear();
        currentFileNumber = 0;
        Files.move(compactionFilePath, generateNextFilePath());
    }

    public Iterator<BaseEntry<String>> getInMemoryDataIterator(String from, String to) {
        lock.readLock().lock();
        try {
            if (from == null && to == null) {
                return data.values().iterator();
            }
            if (from == null) {
                return data.headMap(to).values().iterator();
            }
            if (to == null) {
                return data.tailMap(from).values().iterator();
            }
            return data.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeKey(String key, BufferedWriter bufferedFileWriter) throws IOException {
        DaoUtils.writeUnsignedInt(key.length(), bufferedFileWriter);
        bufferedFileWriter.write(key);
    }

    private void writeValue(String value, BufferedWriter bufferedFileWriter) throws IOException {
        bufferedFileWriter.write(EXISTING_MARK);
        bufferedFileWriter.write(value + '\n');
    }

    private Path generateNextFilePath() {
        return config.basePath().resolve(DATA_FILE_NAME + currentFileNumber + DATA_FILE_EXTENSION);
    }

    public Config getConfig() {
        return config;
    }

    public long getCurrentFileNumber() {
        return currentFileNumber;
    }
}
