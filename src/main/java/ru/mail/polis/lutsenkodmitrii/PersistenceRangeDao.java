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
    public static final char NEXT_LINE = '\n';
    public static final String NEXT_LINE_REPLACEMENT = "\\n";
    public static final String NEXT_LINE_IN_STRING = String.valueOf(NEXT_LINE);
    public static final int NEXT_LINE_REPLACEMENT_ADDITION =
            NEXT_LINE_REPLACEMENT.length() - NEXT_LINE_IN_STRING.length();
    public static final String DATA_FILE_NAME = "daoData";
    public static final String DATA_FILE_EXTENSION = ".txt";
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
            String fileMinKey = preprocess(data.firstKey());
            String fileMaxKey = preprocess(data.lastKey());
            DaoUtils.writeUnsignedInt(fileMinKey.length(), bufferedFileWriter);
            bufferedFileWriter.write(fileMinKey);
            DaoUtils.writeUnsignedInt(fileMaxKey.length(), bufferedFileWriter);
            bufferedFileWriter.write(fileMaxKey);
            DaoUtils.writeUnsignedInt(0, bufferedFileWriter);
            int keyNextLines;
            int valueNextLines;
            for (BaseEntry<String> baseEntry : data.values()) {
                keyNextLines = (int) baseEntry.key().chars().parallel().filter(ch -> ch == NEXT_LINE).count();
                DaoUtils.writeUnsignedInt(baseEntry.key().length() + keyNextLines * NEXT_LINE_REPLACEMENT_ADDITION,
                        bufferedFileWriter);
                bufferedFileWriter.write(keyNextLines == 0 ? baseEntry.key() : preprocess(baseEntry.key()));
                if (baseEntry.value() == null) {
                    bufferedFileWriter.write(DELETED_MARK + NEXT_LINE);
                    DaoUtils.writeUnsignedInt(DaoUtils.CHARS_IN_INT + DaoUtils.CHARS_IN_INT
                            + baseEntry.key().length() + keyNextLines * NEXT_LINE_REPLACEMENT_ADDITION
                            + NEXT_LINE_IN_STRING.length(), bufferedFileWriter);
                    continue;
                }
                bufferedFileWriter.write(EXISTING_MARK);
                valueNextLines = (int) baseEntry.value().chars().parallel().filter(ch -> ch == NEXT_LINE).count();
                bufferedFileWriter.write((valueNextLines == 0 ? baseEntry.value() : preprocess(baseEntry.value()))
                        + NEXT_LINE); // Длина value не пишется так как она не нужна
                DaoUtils.writeUnsignedInt(DaoUtils.CHARS_IN_INT + DaoUtils.CHARS_IN_INT
                        + baseEntry.key().length() + baseEntry.value().length()
                        + (keyNextLines + valueNextLines) * NEXT_LINE_REPLACEMENT_ADDITION
                        + NEXT_LINE_IN_STRING.length(), bufferedFileWriter);
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
