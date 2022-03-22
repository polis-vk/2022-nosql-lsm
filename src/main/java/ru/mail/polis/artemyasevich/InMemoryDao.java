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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private final static OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Path pathToDirectory;
    private final Map<Integer, List<Long>> offsets;
    private boolean allMetaProcessed;
    private int numberOfFiles;

    public InMemoryDao(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        this.numberOfFiles = files == null ? 0 : files.length / 2;
        this.offsets = new HashMap<>(numberOfFiles);
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
        if (!allMetaProcessed) {
            processAllMeta();
        }
        List<PeekIterator> iterators = new ArrayList<>(numberOfFiles);
        iterators.add(new PeekIterator(getDataMapIterator(from, to), 0));
        for (int fileNumber = 0; fileNumber < numberOfFiles; fileNumber++) {
            Path path = pathToFile(fileNumber, DATA_FILE);
            List<Long> fileOffsets = Collections.unmodifiableList(offsets.get(fileNumber));
            int sourceNumber = numberOfFiles - fileNumber;
            iterators.add(new PeekIterator(new FileIterator(from, to, path, fileOffsets), sourceNumber));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = dataMap.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        for (int fileNumber = numberOfFiles - 1; fileNumber >= 0; fileNumber--) {
            if (offsets.get(fileNumber) == null) {
                offsets.put(fileNumber, readOffsets(fileNumber));
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
        List<Long> fileOffsets = offsets.get(fileNumber);
        BaseEntry<String> res = null;
        try (RandomAccessFile reader = new RandomAccessFile(pathToFile(fileNumber, DATA_FILE).toFile(), "r")) {
            int left = 0;
            int right = fileOffsets.size() - 2;
            while (left <= right) {
                int middle = (right - left) / 2 + left;
                long pos = fileOffsets.get(middle);
                reader.seek(pos);
                String entryKey = reader.readUTF();
                int comparison = key.compareTo(entryKey);
                if (comparison == 0) {
                    String entryValue = reader.getFilePointer() == fileOffsets.get(middle + 1)
                            ? null : reader.readUTF();
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
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToFile(numberOfFiles, DATA_FILE), writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToFile(numberOfFiles, META_FILE), writeOptions)
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

    private List<Long> readOffsets(int fileNumber) throws IOException {
        List<Long> fileOffsets;
        try (DataInputStream metaStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(pathToFile(fileNumber, META_FILE))))) {
            int dataSize = metaStream.readInt();
            fileOffsets = new ArrayList<>(dataSize + 1);

            long currentOffset = 0;
            fileOffsets.add(currentOffset);
            while (metaStream.available() > 0) {
                int numberOfEntries = metaStream.readInt();
                int entryBytesSize = metaStream.readInt();
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets.add(currentOffset);
                }
            }
        }
        return fileOffsets;
    }

    private Path pathToFile(int fileNumber, String fileName) {
        return pathToDirectory.resolve(fileName + fileNumber + FILE_EXTENSION);
    }

    private void processAllMeta() throws IOException {
        for (int i = 0; i < numberOfFiles; i++) {
            if (offsets.get(i) == null) {
                offsets.put(i, readOffsets(i));
            }
        }
        allMetaProcessed = true;
    }
}
