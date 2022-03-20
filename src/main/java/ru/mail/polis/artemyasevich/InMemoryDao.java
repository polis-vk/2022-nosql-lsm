package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
        if (to != null && to.equals(from)) {
            return Collections.emptyIterator();
        }
        processAllMeta();
        return new MergeIterator(from, to, numberOfFiles, getDataMapIterator(from, to), pathToDirectory, offsets);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = dataMap.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        for (int fileNumber = numberOfFiles - 1; fileNumber >= 0; fileNumber--) {
            if (offsets.get(fileNumber) == null) {
                offsets.set(fileNumber, readOffsets(fileNumber));
            }
            entry = getFromFile(key, fileNumber);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
        }
        return null;
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

    private BaseEntry<String> getFromFile(String key, int fileNumber) throws IOException {
        Path path = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
        long[] fileOffsets = offsets.get(fileNumber);
        BaseEntry<String> res = null;
        try (RandomAccessFile reader = new RandomAccessFile(path.toFile(), "r")) {
            int left = 0;
            int middle;
            int right = fileOffsets.length - 2;
            while (left <= right) {
                middle = (right - left) / 2 + left;
                long pos = fileOffsets[middle];
                reader.seek(pos);
                String entryKey = reader.readUTF();
                int comparison = key.compareTo(entryKey);
                if (comparison == 0) {
                    String entryValue = reader.getFilePointer() == fileOffsets[middle + 1] ? null : reader.readUTF();
                    res = new BaseEntry<>(entryKey, entryValue);
                    break;
                } else if (comparison > 0) {
                    left = middle + 1;
                } else {
                    right = middle - 1;
                }
            }
        }
        return res;
    }

    private void savaData() throws IOException {
        Iterator<BaseEntry<String>> dataIterator = dataMap.values().iterator();
        if (!dataIterator.hasNext()) {
            return;
        }
        Path pathToData = pathToDirectory.resolve(DATA_FILE + numberOfFiles + FILE_EXTENSION);
        Path pathToOffsets = pathToDirectory.resolve(META_FILE + numberOfFiles + FILE_EXTENSION);
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToData, writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToOffsets, writeOptions)
             ))) {
            Entry<String> entry = dataIterator.next();
            dataStream.writeUTF(entry.key());
            if (entry.value() != null) {
                dataStream.writeUTF(entry.value());
            }
            metaStream.writeInt(dataMap.size());
            int currentBytes = dataStream.size();
            int currentRepeats = 1;
            int bytesWrittenTotal = dataStream.size();
            while (dataIterator.hasNext()) {
                entry = dataIterator.next();
                dataStream.writeUTF(entry.key());
                if (entry.value() != null) {
                    dataStream.writeUTF(entry.value());
                }
                int bytesWritten = dataStream.size() - bytesWrittenTotal;
                if (bytesWritten == currentBytes) {
                    currentRepeats++;
                } else {
                    metaStream.writeInt(currentRepeats);
                    metaStream.writeInt(currentBytes);
                    currentBytes = bytesWritten;
                    currentRepeats = 1;
                }
                bytesWrittenTotal = dataStream.size();
            }
            metaStream.writeInt(currentRepeats);
            metaStream.writeInt(currentBytes);
            numberOfFiles++;
        }
    }

    private long[] readOffsets(int fileNumber) throws IOException {
        long[] fileOffsets;
        try (DataInputStream metaStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(pathToDirectory.resolve(META_FILE + fileNumber + FILE_EXTENSION))))) {
            int dataSize = metaStream.readInt();
            fileOffsets = new long[dataSize + 1];

            long currentOffset = 0;
            fileOffsets[0] = currentOffset;
            int i = 1;
            while (metaStream.available() > 0) {
                int numberOfEntries = metaStream.readInt();
                int entryBytesSize = metaStream.readInt();
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets[i] = currentOffset;
                    i++;
                }
            }
        }
        return fileOffsets;
    }

    private void processAllMeta() throws IOException {
        for (int i = 0; i < numberOfFiles; i++) {
            if (offsets.get(i) == null) {
                offsets.set(i, readOffsets(i));
            }
        }
    }
}
