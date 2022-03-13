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
    private int numberOfFiles;

    public InMemoryDao(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        this.numberOfFiles = files == null ? 0 : files.length / 2;
        this.offsets = new ArrayList<>(this.numberOfFiles);
        for (int i = 0; i < this.numberOfFiles; i++) {
            offsets.add(null);
        }
    }

    public InMemoryDao() {
        pathToDirectory = null;
        offsets = null;
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
        for (int fileNumber = numberOfFiles - 1; fileNumber >= 0; fileNumber--) {
            Path pathToData = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
            if (offsets.get(fileNumber) == null) {
                offsets.set(fileNumber, readOffsets(fileNumber));
            }
            try (RandomAccessFile reader = new RandomAccessFile(pathToData.toFile(), "r")) {
                entry = findValidClosest(key, null, reader, fileNumber, null);
                if (entry != null) {
                    break;
                }
            }
        }
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

    private BaseEntry<String> findValidClosest(String from, String to, RandomAccessFile reader, int fileNumber, int[] nextEntryToRead) throws IOException {
        int left = 0;
        int middle;
        int right = offsets.get(fileNumber).length - 2;
        boolean accurately = nextEntryToRead == null;
        String goodKey = null;
        String goodValue = null;

        while (left <= right) {
            middle = (right - left) / 2 + left;
            long pos = offsets.get(fileNumber)[middle];
            reader.seek(pos);
            String key = reader.readUTF();
            int comparison = from.compareTo(key);
            if (comparison <= 0) {
                String value = reader.readUTF();
                if (!accurately) {
                    nextEntryToRead[fileNumber] = middle + 1;
                }
                if (comparison == 0) {
                    return new BaseEntry<>(key, value);
                } else {
                    right = middle - 1;
                    goodKey = key;
                    goodValue = value;
                }
            } else {
                left = middle + 1;
            }
        }
        if (accurately || goodKey == null) {
            return null;
        }
        return to != null && goodKey.compareTo(to) >= 0 ? null : new BaseEntry<>(goodKey, goodValue);
    }

    private void savaData() throws IOException {
        Path pathToData = pathToDirectory.resolve(DATA_FILE + numberOfFiles + FILE_EXTENSION);
        Path pathToOffsets = pathToDirectory.resolve(META_FILE + numberOfFiles + FILE_EXTENSION);
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
            numberOfFiles++;
        }
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
        private final List<RandomAccessFile> readers = new ArrayList<>(numberOfFiles);
        private final List<BaseEntry<String>> currents = new ArrayList<>(numberOfFiles + 1);
        private final int[] nextEntryToRead = new int[numberOfFiles];
        private final Iterator<BaseEntry<String>> dataMapIterator;
        private final String to;

        public MergeIterator(String from, String to) throws IOException {
            this.to = to;
            this.dataMapIterator = getDataMapIterator(from, to);
            proceedMeta();
            for (int fileNumber = 0; fileNumber < numberOfFiles; fileNumber++) {
                Path path = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
                RandomAccessFile reader = new RandomAccessFile(path.toFile(), "r");
                readers.add(reader);
                if (from != null) {
                    BaseEntry<String> closest = findValidClosest(from, to, reader, fileNumber, nextEntryToRead);
                    currents.add(closest);
                    if (closest == null) {
                        reader.close();
                        readers.set(fileNumber, null);
                    }
                } else {
                    currents.add(readEntry(fileNumber));
                }
            }
            currents.add(dataMapIterator.hasNext() ? dataMapIterator.next() : null);
        }

        @Override
        public boolean hasNext() {
            return currents.stream().anyMatch(Objects::nonNull);
        }

        @Override
        public BaseEntry<String> next() {
            String minKey = "";
            int minI = 0;
            for (int i = numberOfFiles; i >= 0; i--) {
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
                for (int i = 0; i < numberOfFiles + 1; i++) {
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
            for (int i = 0; i < numberOfFiles; i++) {
                if (offsets.get(i) != null) {
                    continue;
                }
                offsets.set(i, readOffsets(i));
            }
        }

        private BaseEntry<String> readEntry(int fileNumber) throws IOException {
            if (fileNumber == numberOfFiles) {
                return dataMapIterator.hasNext() ? dataMapIterator.next() : null;
            }
            RandomAccessFile reader = readers.get(fileNumber);
            long[] curOffsets = offsets.get(fileNumber);
            reader.seek(curOffsets[nextEntryToRead[fileNumber]]);
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
            nextEntryToRead[fileNumber]++;
            return new BaseEntry<>(key, value);
        }
    }
}