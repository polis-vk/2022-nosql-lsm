package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";

    private final OpenOption[] writeOptions = {StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE};
    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Path pathToDirectory;
    private int fileToWrite;
    private int lastDataSize;
    private int lastReadFile = -1;
    private int maxValueSize;
    private byte[] valueBuffer;
    private long[] offsetsOfLastFile;

    public InMemoryDao(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        fileToWrite = files == null ? 0 : files.length / 2;
    }

    public InMemoryDao() {
        this.pathToDirectory = null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        Map<String, BaseEntry<String>> subMap;
        if (from == null && to == null) {
            subMap = dataMap;
        } else if (from == null) {
            subMap = dataMap.headMap(to);
        } else if (to == null) {
            subMap = dataMap.tailMap(from);
        } else {
            subMap = dataMap.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = dataMap.get(key);
        if (entry != null) {
            return entry;
        }
        entry = getFromFile(key);
        return entry;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        if (entry.value().length() > maxValueSize) {
            maxValueSize = entry.value().length();
        }
        dataMap.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        savaData();
        dataMap.clear();
    }

    @Override
    public void close() throws IOException {
        savaData();
    }

    private void savaData() throws IOException {
        Path pathToData = pathToDirectory.resolve(DATA_FILE + fileToWrite + FILE_EXTENSION);
        Path pathToOffsets = pathToDirectory.resolve(META_FILE + fileToWrite + FILE_EXTENSION);
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToData, writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToOffsets, writeOptions)
             ))) {
            long currentOffset = 0;
            metaStream.writeInt(dataMap.size());
            metaStream.writeInt(maxValueSize);
            for (BaseEntry<String> entry : dataMap.values()) {
                dataStream.writeUTF(entry.key());
                dataStream.writeBytes(entry.value());
                metaStream.writeLong(currentOffset);
                currentOffset += entry.key().getBytes(StandardCharsets.UTF_8).length
                        + entry.value().getBytes(StandardCharsets.UTF_8).length + 2;
            }
            metaStream.writeLong(currentOffset);
            fileToWrite++;
        }
    }

    private BaseEntry<String> getFromFile(String key) throws IOException {
        BaseEntry<String> res = null;
        for (int fileNumber = fileToWrite - 1; fileNumber >= 0; fileNumber--) {
            Path pathToData = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
            Path pathToMeta = pathToDirectory.resolve(META_FILE + fileNumber + FILE_EXTENSION);
            if (lastReadFile != fileNumber) {
                try (DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(
                        Files.newInputStream(pathToMeta)))) {
                    lastDataSize = dataInputStream.readInt();
                    offsetsOfLastFile = new long[lastDataSize + 1];
                    maxValueSize = dataInputStream.readInt();
                    valueBuffer = new byte[maxValueSize];
                    for (int i = 0; i < lastDataSize + 1; i++) {
                        offsetsOfLastFile[i] = dataInputStream.readLong();
                    }
                    lastReadFile = fileNumber;
                }
            }
            try (RandomAccessFile reader = new RandomAccessFile(pathToData.toFile(), "r")) {
                int left = 0;
                int middle;
                int right = lastDataSize - 1;

                while (left <= right) {
                    middle = (right - left) / 2 + left;
                    long pos = offsetsOfLastFile[middle];
                    reader.seek(pos);
                    String entryKey = reader.readUTF();
                    int comparison = key.compareTo(entryKey);

                    if (comparison == 0) {
                        int valueSize = (int) (offsetsOfLastFile[middle + 1] - pos)
                                - entryKey.getBytes(StandardCharsets.UTF_8).length - 2;
                        reader.read(valueBuffer, 0, valueSize);
                        String entryValue = new String(valueBuffer, 0, valueSize, StandardCharsets.UTF_8);
                        res = new BaseEntry<>(entryKey, entryValue);
                        break;
                    } else if (comparison > 0) {
                        left = middle + 1;
                    } else {
                        right = middle - 1;
                    }
                }
            }
            if (res != null) {
                break;
            }
        }
        return res;
    }

}
