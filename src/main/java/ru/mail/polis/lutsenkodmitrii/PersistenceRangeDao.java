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

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * ----------------------------------------------------------------------------------------------*
 * Описание формата файла
 * <p>
 * - Минимальный ключ во всем файле
 * - Максимальный ключ во всем файле
 * - 0 - Длина предыдущей entry для первой entry
 * - В цикле для всех entry:
 * - Длина ключа
 * - Ключ
 * - Значение + '\n'
 * - Длина всего записанного + размер самого числа относительно типа char
 *
 * Пример (пробелы и переносы строк для наглядности):
 * k2 k55
 * 0 2 k2 v2 '\n'
 * 9 3 k40 v40 '\n'
 * 11 3 k55 v5555 '\n'
 * 13
 * ----------------------------------------------------------------------------------------------*
 **/
public class PersistenceRangeDao implements Dao<String, BaseEntry<String>> {

    private static final OpenOption[] writeOptions = new OpenOption[]{
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
    };
    public static final String DATA_FILE_NAME = "daoData";
    public static final String DATA_FILE_EXTENSION = ".txt";
    private final ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();
    private final Config config;
    private long currentFileNumber;

    public PersistenceRangeDao(Config config) throws IOException {
        this.config = config;
        try {
            currentFileNumber = Files.find(config.basePath(), 1,
                    (p, a) -> a.isRegularFile() && p.getFileName().toString().endsWith(DATA_FILE_EXTENSION)).count();
        } catch (NoSuchFileException e) {
            currentFileNumber = 0;
        }
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        return new MergeIterator(this, from, to);
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (data.isEmpty()) {
            return;
        }
        Path dataFilePath = generateNextFilePath();
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(dataFilePath, UTF_8, writeOptions)) {
            String fileMinKey = data.firstKey();
            String fileMaxKey = data.lastKey();
            DaoUtils.writeUnsignedInt(fileMinKey.length(), bufferedFileWriter);
            bufferedFileWriter.write(fileMinKey);
            DaoUtils.writeUnsignedInt(fileMaxKey.length(), bufferedFileWriter);
            bufferedFileWriter.write(fileMaxKey);
            DaoUtils.writeUnsignedInt(0, bufferedFileWriter);
            for (BaseEntry<String> baseEntry : data.values()) {
                DaoUtils.writeUnsignedInt(baseEntry.key().length(), bufferedFileWriter);
                bufferedFileWriter.write(baseEntry.key());
                // Длина value не пишется так как она не нужна
                bufferedFileWriter.write(baseEntry.value() + '\n');
                DaoUtils.writeUnsignedInt(
                        DaoUtils.CHARS_IN_INT + DaoUtils.CHARS_IN_INT
                                + baseEntry.key().length() + baseEntry.value().length() + 1,
                        bufferedFileWriter);
            }
            bufferedFileWriter.flush();
        }
    }

    public Iterator<BaseEntry<String>> getInMemoryDataIterator(String from, String to) {
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