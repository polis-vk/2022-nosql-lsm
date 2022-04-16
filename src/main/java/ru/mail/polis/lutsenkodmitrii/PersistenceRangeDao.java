package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    public static final String MEMORY_FILE_NAME = "memory";
    public static final String COMPACTION_FILE_NAME = "compaction";
    public static final String DATA_FILE_EXTENSION = ".txt";
    public static final String TEMP_FILE_EXTENSION = ".tmp";
    public static final AtomicInteger tmpCounter = new AtomicInteger(0);
    private final AtomicInteger currentFileNumber = new AtomicInteger(0);
    ;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Config config;
    private final MemStorage memStorage;
    private final ConcurrentSkipListMap<Path, FileInputStream> filesMap = new ConcurrentSkipListMap<>(
            Comparator.comparingInt(this::getFileNumber)
    );
    private final ExecutorService flushExecutor = Executors.newCachedThreadPool();
    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor();

    public PersistenceRangeDao(Config config) throws IOException {
        this.config = config;
        this.memStorage = new MemStorage(config.flushThresholdBytes());
        try (Stream<Path> stream = Files.find(config.basePath(), 1,
                (p, a) -> a.isRegularFile() && p.getFileName().toString().endsWith(DATA_FILE_EXTENSION))) {
            List<Path> paths = stream.toList();
            for (Path path : paths) {
                filesMap.put(path, new FileInputStream(path.toString()));
            }
            currentFileNumber.set(filesMap.isEmpty() ? 0 : getFileNumber(filesMap.lastKey()) + 1);
        } catch (NoSuchFileException e) {
            filesMap.clear();
            currentFileNumber.set(0);
        }
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        lock.readLock().lock();
        try {
            return new MergeIterator(this, from, to, true);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        lock.readLock().lock();
        try {
            if (!memStorage.firstTableFull() && !memStorage.firstTableOnFlush()) {
                memStorage.upsertIfFitsFirstTable(entry);
            }
            if (memStorage.firstTableFull() && memStorage.firstTableNotOnFlushAndSetTrue()) {
                flushFirstMemTable();
            }
            if (memStorage.firstTableOnFlush()) {
                if (memStorage.secondTableOnFlush()) {
                    throw new RuntimeException("Can`t upsert now, try later");
                }
                memStorage.upsertIfFitsSecondTable(entry);
                if (memStorage.secondTableIsFull()) {
                    throw new RuntimeException("Can`t upsert now, try later");
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private void flushFirstMemTable() {
        flushExecutor.execute(() -> {
            try {
                Path tempMemoryFilePath = generateTempPath(MEMORY_FILE_NAME);
                writeMemoryToFile(tempMemoryFilePath, firstTableIterator());
                Path dataFilePath = generateNextFilePath();
                Files.move(tempMemoryFilePath, dataFilePath);
                FileInputStream inputStream = new FileInputStream(dataFilePath.toString());
                lock.writeLock().lock();
                try {
                    filesMap.put(dataFilePath, inputStream);
                    memStorage.clearFirstTable();
                } finally {
                    lock.writeLock().unlock();
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Flush first table failed");
            }
        });
    }

    @Override
    public void flush() throws IOException {
        if (memStorage.isEmpty()) {
            return;
        }
        if (memStorage.isSecondTableEmpty()) {
            if (memStorage.firstTableNotOnFlushAndSetTrue()) {
                flushFirstMemTable();
            }
            return;
        }
        flushExecutor.execute(() -> {
            try {
                Path tempMemoryFilePath = generateTempPath(MEMORY_FILE_NAME);
                boolean notSecondOnFlush = memStorage.secondTableNotOnFlushAndSetTrue();
                boolean isFlushingBothTables = memStorage.firstTableNotOnFlushAndSetTrue() && notSecondOnFlush;
                if (isFlushingBothTables) {
                    writeMemoryToFile(tempMemoryFilePath, inMemoryDataIterator());
                } else if (notSecondOnFlush) {
                    writeMemoryToFile(tempMemoryFilePath, secondTableIterator());
                } else {
                    return; //Обе таблицы уже flush-ся, в это время все новые upsert-ы отклоняются
                }
                Path dataFilePath = generateNextFilePath();
                Files.move(tempMemoryFilePath, dataFilePath);
                FileInputStream inputStream = new FileInputStream(dataFilePath.toString());
                lock.writeLock().lock();
                try {
                    Files.move(tempMemoryFilePath, dataFilePath);
                    filesMap.put(dataFilePath, inputStream);
                    if (isFlushingBothTables) {
                        memStorage.clear();
                    } else {
                        memStorage.clearSecondTable();
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Flush fail");
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (isClosed.getAndSet(true)) {
            return;
        }
        shutdownAndAwaitTermination(flushExecutor);
        shutdownAndAwaitTermination(compactionExecutor);
        if (!memStorage.isEmpty()) {
            writeMemoryToFile(generateNextFilePath(), inMemoryDataIterator());
        }
        for (FileInputStream inputStream : filesMap.values()) {
            inputStream.close();
        }
    }

    @Override
    public void compact() throws IOException {
        if (filesMap.isEmpty() || (filesMap.size() == 1 && memStorage.isEmpty())) {
            return;
        }
        // Comact-им только файлы существующие на момент вызова компакта.
        // Начинаем писать compact во временный файл,
        // после переименовываем его в обычный самый приоритетный файл
        // Состояние валидно - остальные файлы не тронуты, временный не учитывается при чтении/записи
        // Начинаем удалять старые файлы: все кроме только что созданного. После переименовываем compact-файл в первый
        // При ошибке выше чтение будет из корректного compact-файла,
        // который останется самым приоритетным файлом существующие на момент вызова компакта
        // Независимо от успеха/ошибок в действиях выше, файлы записанные после compact-a будут приоритетнее
        compactionExecutor.execute(() -> {
            Path tempCompactionFilePath = generateTempPath(COMPACTION_FILE_NAME);
            Path lastFilePath = generateNextFilePath();
            Set<Map.Entry<Path, FileInputStream>> compactionFilesMapEntries;
            try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(tempCompactionFilePath, UTF_8, writeOptions)) {
                MergeIterator allEntriesIterator = new MergeIterator(this, null, null, false);
                compactionFilesMapEntries = allEntriesIterator.getFilesMapEntries();
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
            } catch (IOException e) {
                throw new RuntimeException("Write " + tempCompactionFilePath + " failed");
            }
            lock.writeLock().lock();
            try {
                Files.move(tempCompactionFilePath, lastFilePath);
                for (Map.Entry<Path, FileInputStream> filesMapEntry : compactionFilesMapEntries) {
                    filesMap.remove(filesMapEntry.getKey());
                    filesMapEntry.getValue().close();
                    Files.delete(filesMapEntry.getKey());
                }
                filesMap.put(lastFilePath, new FileInputStream(lastFilePath.toString()));
            } catch (IOException e) {
                throw new RuntimeException("Renaming compaction file or deleting old files failed");
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    public Iterator<BaseEntry<String>> inMemoryDataIterator(String from, String to) {
        lock.readLock().lock();
        try {
            return memStorage.iterator(from, to);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<BaseEntry<String>> inMemoryDataIterator() {
        lock.readLock().lock();
        try {
            return memStorage.iterator(null, null);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<BaseEntry<String>> firstTableIterator() {
        lock.readLock().lock();
        try {
            return memStorage.firstTableIterator(null, null);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<BaseEntry<String>> secondTableIterator() {
        lock.readLock().lock();
        try {
            return memStorage.secondTableIterator(null, null);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeMemoryToFile(Path dataFilePath, Iterator<BaseEntry<String>> memoryIterator) throws IOException {
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(dataFilePath, UTF_8, writeOptions)) {
            DaoUtils.writeUnsignedInt(0, bufferedFileWriter);
            while (memoryIterator.hasNext()) {
                BaseEntry<String> baseEntry = memoryIterator.next();
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
        return config.basePath().resolve(DATA_FILE_NAME + currentFileNumber.getAndIncrement() + DATA_FILE_EXTENSION);
    }

    private Path generateTempPath(String fileName) {
        return config.basePath().resolve(fileName + tmpCounter.getAndIncrement() + TEMP_FILE_EXTENSION);
    }

    public Config getConfig() {
        return config;
    }

    public Map<Path, FileInputStream> getFilesMap() {
        lock.readLock().lock();
        try {
            return filesMap;
        } finally {
            lock.readLock().unlock();
        }
    }

    private int getFileNumber(Path path) {
        String filename = path.getFileName().toString();
        return Integer.parseInt(filename.substring(
                DATA_FILE_NAME.length(),
                filename.length() - DATA_FILE_EXTENSION.length()));
    }

    private void shutdownAndAwaitTermination(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                throw new RuntimeException("Await termination too long");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("");
        }
    }
}
