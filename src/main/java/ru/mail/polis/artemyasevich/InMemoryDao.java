package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";

    private final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};
    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Path pathToDirectory;
    private final List<long[]> offsets;

    private int fileToWrite;

    public InMemoryDao(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        fileToWrite = files == null ? 0 : files.length / 2;
        this.offsets = new ArrayList<>(fileToWrite);
        for (int i = 0; i < fileToWrite; i++) {
            offsets.add(null);
        }
    }

    public InMemoryDao() {
        pathToDirectory = null;
        offsets = null;
    }

    private Iterator<BaseEntry<String>> getDataMapIterator(String from, String to) {
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

    private BaseEntry<String> findFirstValid(String from, String to, RandomAccessFile reader, long[] offsets, int fileNumber, int[] entryToRead) throws IOException {
        int left = 0;
        int middle;
        int right = offsets.length - 2;

        String goodKey = null;
        String goodValue = null;

        while (left <= right) {
            middle = (right - left) / 2 + left;
            long pos = offsets[middle];
            reader.seek(pos);
            String key = reader.readUTF();
            String value = reader.readUTF();
            int comparison = from.compareTo(key);
            if (comparison == 0) {
                entryToRead[fileNumber] = middle + 1;
                return new BaseEntry<>(key, value);
            } else if (comparison > 0) {
                left = middle + 1;
            } else {
                entryToRead[fileNumber] = middle + 1;
                goodKey = key;
                goodValue = value;
                right = middle - 1;
            }
        }
        if (goodKey == null) {
            return null;
        }
        return to != null && goodKey.compareTo(to) >= 0 ? null : new BaseEntry<>(goodKey, goodValue);
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        return new MergeIterator(from, to);
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
            for (BaseEntry<String> entry : dataMap.values()) {
                dataStream.writeUTF(entry.key());
                dataStream.writeUTF(entry.value());
                metaStream.writeLong(currentOffset);
                currentOffset += entry.key().getBytes(StandardCharsets.UTF_8).length
                        + entry.value().getBytes(StandardCharsets.UTF_8).length + Short.BYTES * 2;
            }
            metaStream.writeLong(currentOffset);
            fileToWrite++;
        }
    }

    private BaseEntry<String> getFromFile(String key) throws IOException {
        BaseEntry<String> res = null;
        for (int fileNumber = fileToWrite - 1; fileNumber >= 0; fileNumber--) {
            Path pathToData = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
            if (offsets.get(fileNumber) == null) {
                offsets.set(fileNumber, readOffsets(fileNumber));
            }
            try (RandomAccessFile reader = new RandomAccessFile(pathToData.toFile(), "r")) {
                int left = 0;
                int middle;
                int right = offsets.get(fileNumber).length - 2;

                while (left <= right) {
                    middle = (right - left) / 2 + left;
                    long pos = offsets.get(fileNumber)[middle];
                    reader.seek(pos);
                    String entryKey = reader.readUTF();
                    int comparison = key.compareTo(entryKey);
                    if (comparison == 0) {
                        String entryValue = reader.readUTF();
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

    private long[] readOffsets(int fileNumber) throws IOException {
        long[] fileOffsets;
        try (DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(pathToDirectory.resolve(META_FILE + fileNumber + FILE_EXTENSION))))) {
            int dataSize = dataInputStream.readInt();
            fileOffsets = new long[dataSize + 1];
            for (int i = 0; i < dataSize + 1; i++) {
                fileOffsets[i] = dataInputStream.readLong();
            }
        }
        return fileOffsets;
    }

    private class MergeIterator implements Iterator<BaseEntry<String>> {
        private final List<RandomAccessFile> readers = new ArrayList<>(fileToWrite);
        private final List<BaseEntry<String>> currents = new ArrayList<>(fileToWrite + 1);
        private final int[] entryToRead = new int[fileToWrite];
        private final Iterator<BaseEntry<String>> iterator;
        private final String to;

        public MergeIterator(String from, String to) throws IOException {
            this.to = to;
            this.iterator = getDataMapIterator(from, to);
            proceedMeta();
            for (int fileNumber = 0; fileNumber < fileToWrite; fileNumber++) {
                Path path = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
                readers.add(new RandomAccessFile(path.toFile(), "r"));
                if (from != null) {
                    BaseEntry<String> closest = findFirstValid(from, to, readers.get(fileNumber), offsets.get(fileNumber), fileNumber, entryToRead);
                    currents.add(closest);
                } else {
                    currents.add(readEntry(fileNumber));
                }
            }
            currents.add(iterator.hasNext() ? iterator.next() : null);
        }

        @Override
        public boolean hasNext() {
            return currents.stream().anyMatch(Objects::nonNull);
        }

        @Override
        public BaseEntry<String> next() {
            String minKey = "";
            int minI = 0;
            for (int i = fileToWrite; i >= 0; i--) {
                BaseEntry<String> current = currents.get(i);
                if (current != null) {
                    minKey = current.key();
                    minI = i;
                    break;
                }
            }

            for (int i = minI - 1; i >= 0; i--) {
                BaseEntry<String> current = currents.get(i);
                if (current == null) {
                    continue;
                }
                if (current.key().compareTo(minKey) < 0) {
                    minKey = current.key();
                    minI = i;
                }
            }
            BaseEntry<String> next = currents.get(minI);
            try {
                currents.set(minI, readEntry(minI));
                for (int i = 0; i < fileToWrite + 1; i++) {
                    BaseEntry<String> current = currents.get(i);
                    if (current == null) {
                        continue;
                    }
                    if (current.key().equals(minKey)) {
                        currents.set(i, readEntry(i));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return next;
        }

        private void proceedMeta() throws IOException {
            for (int i = 0; i < fileToWrite; i++) {
                if (offsets.get(i) != null) {
                    continue;
                }
                offsets.set(i, readOffsets(i));
            }
        }

        private BaseEntry<String> readEntry(int fileNumber) throws IOException {
            if (fileNumber == fileToWrite) {
                return iterator.hasNext() ? iterator.next() : null;
            }
            RandomAccessFile reader = readers.get(fileNumber);
            long[] curOffsets = offsets.get(fileNumber);
            reader.seek(curOffsets[entryToRead[fileNumber]]);
            if (reader.getFilePointer() == curOffsets[curOffsets.length - 1]) {
                reader.close();
                return null;
            }
            String key = reader.readUTF();
            if (to != null && key.compareTo(to) >= 0) {
                reader.close();
                return null;
            }
            String value = reader.readUTF();
            entryToRead[fileNumber]++;
            return new BaseEntry<>(key, value);
        }
    }
}