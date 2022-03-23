package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MergeIterator implements Iterator<BaseEntry<String>>, AutoCloseable {

    private final NavigableMap<String, BaseEntry<String>> tempData = new TreeMap<>();
    private final Map<String, Integer> tempDataPriorities = new HashMap<>();
    private final Map<String, List<FileInfo>> lastElementWithFilesMap = new HashMap<>();
    private final Iterator<BaseEntry<String>> inMemoryIterator;
    private final String to;
    private final boolean isFromNull;
    private final boolean isToNull;
    private String inMemoryLastKey;
    private BaseEntry<String> polledEntry;

    public MergeIterator(PersistenceRangeDao dao, String from, String to) throws IOException {
        this.to = to;
        isFromNull = from == null;
        isToNull = to == null;
        for (int i = 0; i < dao.getCurrentFileNumber(); i++) {
            Path path = dao.getConfig().basePath().resolve(
                    PersistenceRangeDao.DATA_FILE_NAME + i + PersistenceRangeDao.DATA_FILE_EXTENSION
            );
            BufferedReader bufferedReader = Files.newBufferedReader(path, UTF_8);
            String fileMinKey = DaoUtils.readKey(bufferedReader);
            String fileMaxKey = DaoUtils.readKey(bufferedReader);
            BaseEntry<String> firstEntry = null;
            if (isFromNull || from.compareTo(fileMinKey) <= 0) {
                firstEntry = DaoUtils.readEntry(bufferedReader);
            } else if (from.compareTo(fileMaxKey) <= 0) {
                firstEntry = DaoUtils.ceilKey(path, bufferedReader, from,
                        DaoUtils.CHARS_IN_INT + DaoUtils.CHARS_IN_INT + fileMinKey.length() + fileMaxKey.length());
            }
            if (firstEntry != null && (isToNull || firstEntry.key().compareTo(to) < 0)) {
                tempData.put(firstEntry.key(), firstEntry);
                tempDataPriorities.put(firstEntry.key(), i);
                lastElementWithFilesMap
                        .computeIfAbsent(firstEntry.key(), files -> new ArrayList<>())
                        .add(new FileInfo(i, bufferedReader));
            }
        }
        inMemoryIterator = dao.getInMemoryDataIterator(from, to);
        if (inMemoryIterator.hasNext()) {
            BaseEntry<String> entry = inMemoryIterator.next();
            tempData.put(entry.key(), entry);
            tempDataPriorities.put(entry.key(), Integer.MAX_VALUE);
            inMemoryLastKey = entry.key();
        }
    }

    @Override
    public boolean hasNext() {
        if (tempData.isEmpty()) {
            return false;
        }
        try {
            do {
                polledEntry = tempData.pollFirstEntry().getValue();
                readNextFromFiles(lastElementWithFilesMap.get(polledEntry.key()));
                readNextFromMemory();
                tempDataPriorities.remove(polledEntry.key());
            } while (!tempData.isEmpty() && polledEntry.value() == null);
        } catch (IOException e) {
            throw new RuntimeException("Fail to read new Entry after" + polledEntry, e);
        }
        return polledEntry.value() != null && (isToNull || polledEntry.key().compareTo(to) < 0);
    }

    private void readNextFromMemory() {
        if (inMemoryIterator.hasNext() && inMemoryLastKey.equals(polledEntry.key())) {
            BaseEntry<String> newEntry = inMemoryIterator.next();
            tempData.put(newEntry.key(), newEntry);
            tempDataPriorities.put(newEntry.key(), Integer.MAX_VALUE);
            inMemoryLastKey = newEntry.key();
        }
    }

    private void readNextFromFiles(List<FileInfo> filesToRead) throws IOException {
        if (filesToRead == null) {
            return;
        }
        for (FileInfo fileInfo : filesToRead) {
            BaseEntry<String> newEntry = DaoUtils.readEntry(fileInfo.bufferedReader);
            if (newEntry == null) {
                continue;
            }
            Integer currentFileNumber = tempDataPriorities.get(newEntry.key());
            if (currentFileNumber == null || fileInfo.fileNumber > currentFileNumber) {
                tempData.put(newEntry.key(), newEntry);
                tempDataPriorities.put(newEntry.key(), fileInfo.fileNumber);
            }
            lastElementWithFilesMap
                    .computeIfAbsent(newEntry.key(), files -> new ArrayList<>())
                    .add(fileInfo);
        }
        lastElementWithFilesMap.remove(polledEntry.key());
    }

    @Override
    public BaseEntry<String> next() {
        return polledEntry;
    }

    @Override
    public void close() throws Exception {
        tempData.clear();
        for (List<FileInfo> value : lastElementWithFilesMap.values()) {
            for (FileInfo fileInfo : value) {
                fileInfo.bufferedReader.close();
            }
        }
    }

    private static final class FileInfo {
        private final int fileNumber;
        private final BufferedReader bufferedReader;

        private FileInfo(int fileNumber, BufferedReader bufferedReader) {
            this.fileNumber = fileNumber;
            this.bufferedReader = bufferedReader;
        }
    }
}
