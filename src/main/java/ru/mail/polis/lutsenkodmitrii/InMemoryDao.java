package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static int fileCounter = 1;
    private static final int BUFFER_FLUSH_LIMIT = 256;
    private static final OpenOption[] writeOptions = new OpenOption[]{
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
    };
    private static final String DATA_FILE_NAME = "daoData";
    private static final String DATA_FILE_EXTENSION = ".txt";
    private final Config config;
    private final ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();

    public InMemoryDao() {
        config = null;
    }

    public InMemoryDao(Config config) {
        this.config = config;
    }

    class MergeIterator implements Iterator<BaseEntry<String>> {
        private final List<FileInfo> filesList = new LinkedList<>();
        private final ConcurrentSkipListMap<String, BaseEntry<String>> tempData = new ConcurrentSkipListMap<>();
        private final HashMap<String, Integer> tempDataPriorities = new HashMap<>();
        private final Iterator<BaseEntry<String>> inMemoryIterator;
        private final String to;
        private String inMemoryLastKey;
        boolean isFromNull;
        boolean isToNull;

        public MergeIterator(Config config, String from, String to) throws IOException {
            this.to = to;
            isFromNull = from == null;
            isToNull = to == null;
            for (int i = 1; i < fileCounter; i++) {
                Path path = config.basePath().resolve(DATA_FILE_NAME + i + DATA_FILE_EXTENSION);
                BufferedReader bufferedReader;
                try {
                    bufferedReader = Files.newBufferedReader(path);
                } catch (NoSuchFileException e) {
                    continue;
                }
                String fileMinKey = DaoUtils.readKey(bufferedReader);
                String fileMaxKey = DaoUtils.readKey(bufferedReader);
                BaseEntry<String> firstEntry = null;
                if (isFromNull || from.compareTo(fileMinKey) <= 0) {
                    firstEntry = DaoUtils.readEntry(bufferedReader);
                } else if (from.compareTo(fileMaxKey) <= 0) {
                    firstEntry = DaoUtils.ceilKey(path, bufferedReader, from, fileMinKey.length() + fileMaxKey.length());
                }
                if (firstEntry != null && (isToNull || firstEntry.key().compareTo(to) < 0)) {
                    tempData.put(firstEntry.key(), firstEntry);
                    tempDataPriorities.put(firstEntry.key(), i);
                    filesList.add(new FileInfo(i, bufferedReader, firstEntry.key()));
                }
            }
            inMemoryIterator = getInMemoryDataIterator(from, to);
            if (inMemoryIterator.hasNext()) {
                BaseEntry<String> entry = inMemoryIterator.next();
                tempData.put(entry.key(), entry);
                tempDataPriorities.put(entry.key(), Integer.MAX_VALUE);
                inMemoryLastKey = entry.key();
            }
        }

        @Override
        public boolean hasNext() {
            return !tempData.isEmpty();
        }

        @Override
        public BaseEntry<String> next() {
            BaseEntry<String> firstEntry = tempData.pollFirstEntry().getValue();
            try {
                BaseEntry<String> newEntry;
                for (FileInfo fileInfo : filesList) {
                    if (!fileInfo.lastReadElement.equals(firstEntry.key())) {
                        continue;
                    }
                    newEntry = DaoUtils.readEntry(fileInfo.bufferedReader);
                    if (newEntry != null) {
                        Integer fileNumber = tempDataPriorities.get(newEntry.key());
                        if (fileNumber == null || fileInfo.fileNumber > fileNumber) {
                            tempData.put(newEntry.key(), newEntry);
                            tempDataPriorities.put(newEntry.key(), fileInfo.fileNumber);
                        }
                        fileInfo.lastReadElement = newEntry.key();
                    }
                }
                if (inMemoryIterator.hasNext() && inMemoryLastKey.equals(firstEntry.key())) {
                    newEntry = inMemoryIterator.next();
                    tempData.put(newEntry.key(), newEntry);
                    tempDataPriorities.put(newEntry.key(), Integer.MAX_VALUE);
                    inMemoryLastKey = newEntry.key();
                }
                if (tempData.isEmpty() || (!isToNull && tempData.firstKey().compareTo(to) >= 0)) {
                    tempData.clear();
                    for (FileInfo fileInfo : filesList) {
                        fileInfo.bufferedReader.close();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Reading next fail", e);
            }
            return firstEntry;
        }
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (from == null && to == null) {
            return new MergeIterator(config, from, to);
        }
        if (from == null) {
            return new MergeIterator(config, from, to);
        }
        if (to == null) {
            return new MergeIterator(config, from, to);
        }
        return new MergeIterator(config, from, to);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (data.containsKey(key)) {
            return data.get(key);
        }
        for (int i = fileCounter - 1; i >= 1; i--) {
            Path path = config.basePath().resolve(DATA_FILE_NAME + i + DATA_FILE_EXTENSION);
            BaseEntry<String> entry = DaoUtils.searchInFile(path, key);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (data.isEmpty() || config == null) {
            return;
        }
        Path dataFilePath = generateNextFilePath(config);
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(dataFilePath, UTF_8, writeOptions)) {
            int bufferedEntriesCounter = 0;
            String fileMinKey = data.firstKey();
            String fileMaxKey = data.lastKey();
            bufferedFileWriter.write(DaoUtils.intToCharArray(fileMinKey.length()));
            bufferedFileWriter.write(fileMinKey);
            bufferedFileWriter.write(DaoUtils.intToCharArray(fileMaxKey.length()));
            bufferedFileWriter.write(fileMaxKey);
            bufferedFileWriter.write(DaoUtils.intToCharArray(0));
            for (BaseEntry<String> baseEntry : data.values()) {
                bufferedFileWriter.write(DaoUtils.intToCharArray(baseEntry.key().length()));
                bufferedFileWriter.write(baseEntry.key());
                bufferedFileWriter.write(baseEntry.value() + '\n');
                bufferedFileWriter.write(DaoUtils.intToCharArray(
                        Integer.BYTES + Integer.BYTES
                                + baseEntry.key().length() + baseEntry.value().length() + 1));
                bufferedEntriesCounter++;
                if (bufferedEntriesCounter % BUFFER_FLUSH_LIMIT == 0) {
                    bufferedFileWriter.flush();
                }
            }
            bufferedFileWriter.flush();
        }
    }

    private Iterator<BaseEntry<String>> getInMemoryDataIterator(String from, String to) {
        if (from == null && to == null) {
            return data.values().iterator();
        }
        if (from == null) {
            return data.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return data.tailMap(from, true).values().iterator();
        }
        return data.subMap(from, true, to, false).values().iterator();
    }

    private static Path generateNextFilePath(Config config) {
        return config.basePath().resolve(DATA_FILE_NAME + fileCounter++ + DATA_FILE_EXTENSION);
    }

    static class FileInfo {
        private final int fileNumber;
        private final BufferedReader bufferedReader;
        private String lastReadElement;

        public FileInfo(int fileNumber, BufferedReader bufferedReader, String lastReadElement) {
            this.fileNumber = fileNumber;
            this.bufferedReader = bufferedReader;
            this.lastReadElement = lastReadElement;
        }
    }
}
