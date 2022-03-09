package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    private static final int MAX_SIZE = 100_000;
    private static final String DATA_FILE = "data";
    private static final String OFFSETS_FILE = "offset";

    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Path pathToDirectory;
    private int fileToWrite;
    private int lastReadFile = -1;
    private List<String> offsetsOfLastFile;

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
        if (dataMap.size() == MAX_SIZE && !dataMap.containsKey(entry.key())) {
            try {
                flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
        Path pathToData = pathToDirectory.resolve(DATA_FILE + fileToWrite + ".txt");
        Path pathToOffsets = pathToDirectory.resolve(OFFSETS_FILE + fileToWrite + ".txt");
        createFileIfNeeded(pathToData);
        createFileIfNeeded(pathToOffsets);
        try (BufferedWriter writer = Files.newBufferedWriter(pathToData);
             BufferedWriter writer2 = Files.newBufferedWriter(pathToOffsets)) {
            long currentOffset = 0;
            for (BaseEntry<String> entry : dataMap.values()) {
                String line = entry.key() + " " + entry.value() + "\n";
                writer.write(line);
                writer2.write(currentOffset + "\n");
                currentOffset += line.getBytes().length;
            }
            fileToWrite++;
        }
    }

    private BaseEntry<String> getFromFile(String key) throws IOException {
        BaseEntry<String> res = null;
        for (int i = fileToWrite - 1; i >= 0; i--) {
            Path pathToData = pathToDirectory.resolve(DATA_FILE + i + ".txt");
            Path pathToOffsets = pathToDirectory.resolve(OFFSETS_FILE + i + ".txt");
            try (RandomAccessFile reader = new RandomAccessFile(pathToData.toFile(), "r")) {
                if (lastReadFile != i) {
                    offsetsOfLastFile = Files.readAllLines(pathToOffsets);
                    lastReadFile = i;
                }
                int left = 0;
                int middle;
                int right = offsetsOfLastFile.size() - 1;
                while (left <= right) {
                    middle = (right - left) / 2 + left;
                    reader.seek(Long.parseLong(offsetsOfLastFile.get(middle)));
                    String[] entry = reader.readLine().split(" ");
                    int comparison = key.compareTo(entry[0]);
                    if (comparison == 0) {
                        res = new BaseEntry<>(entry[0], entry[1].trim());
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

    private void createFileIfNeeded(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }

}
