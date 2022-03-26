package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Path pathToDirectory;
    private final List<DaoFile> daoFiles;
    private final ThreadLocal<ByteBuffer> threadLocalBuffer;

    public InMemoryDao(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int numberOfFiles = files == null ? 0 : files.length / 2;
        this.daoFiles = new ArrayList<>(numberOfFiles);
        int maxEntrySize = initFiles(numberOfFiles);
        this.threadLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(maxEntrySize));
    }

    public InMemoryDao() {
        pathToDirectory = null;
        daoFiles = null;
        threadLocalBuffer = null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (to != null && to.equals(from)) {
            return Collections.emptyIterator();
        }
        List<PeekIterator> iterators = new ArrayList<>(daoFiles.size());
        iterators.add(new PeekIterator(getDataMapIterator(from, to), 0));
        for (int fileNumber = 0; fileNumber < daoFiles.size(); fileNumber++) {
            int sourceNumber = daoFiles.size() - fileNumber;
            iterators.add(new PeekIterator(new FileIterator(from, to, daoFiles.get(fileNumber), threadLocalBuffer.get()), sourceNumber));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = dataMap.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        for (int fileNumber = daoFiles.size() - 1; fileNumber >= 0; fileNumber--) {
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
        flush();
        for (DaoFile daoFile : daoFiles) {
            daoFile.close();
        }
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

    private BaseEntry<String> getFromFile(String keyToFind, int fileNumber) throws IOException {
        DaoFile daoFile = daoFiles.get(fileNumber);
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            InMemoryDao.fillBufferWithEntry(daoFile, threadLocalBuffer.get(), middle);
            String key = InMemoryDao.readKeyFromBuffer(threadLocalBuffer.get());
            int comparison = keyToFind.compareTo(key);
            if (comparison < 0) {
                right = middle - 1;
            } else if (comparison > 0) {
                left = middle + 1;
            } else {
                String value = InMemoryDao.readValueFromBuffer(daoFile, threadLocalBuffer.get(), middle);
                return new BaseEntry<>(key, value);
            }
        }
        return null;
    }

    private void savaData() throws IOException {
        if (dataMap.isEmpty()) {
            return;
        }
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToFile(daoFiles.size(), DATA_FILE), writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToFile(daoFiles.size(), META_FILE), writeOptions)
             ))) {

            metaStream.writeInt(dataMap.size());
            Iterator<BaseEntry<String>> dataIterator = dataMap.values().iterator();
            BaseEntry<String> entry = dataIterator.next();
            int currentRepeats = 1;
            int currentBytes = writeEntryInStream(dataStream, entry);

            while (dataIterator.hasNext()) {
                entry = dataIterator.next();
                int bytesWritten = writeEntryInStream(dataStream, entry);
                if (bytesWritten == currentBytes) {
                    currentRepeats++;
                    continue;
                }
                metaStream.writeInt(currentRepeats);
                metaStream.writeInt(currentBytes);
                currentBytes = bytesWritten;
                currentRepeats = 1;
            }
            metaStream.writeInt(currentRepeats);
            metaStream.writeInt(currentBytes);
        }
    }

    //key|value|valueSize|keySize or key|keySize if value == null
    private int writeEntryInStream(DataOutputStream dataStream, BaseEntry<String> entry) throws IOException {
        long before = dataStream.size();
        dataStream.writeBytes(entry.key());
        long current = dataStream.size();
        short keySize = (short) (current - before);
        current = dataStream.size();
        if (entry.value() != null) {
            dataStream.writeBytes(entry.value());
            short valueSize = (short) (dataStream.size() - current);
            dataStream.writeShort(valueSize);
        }
        dataStream.writeShort(keySize);
        return (int) (dataStream.size() - before);
    }

    static void fillBufferWithEntry(DaoFile daoFile, ByteBuffer buffer, int index) throws IOException {
        buffer.clear();
        buffer.limit(daoFile.entrySize(index));
        daoFile.getChannel().read(buffer, daoFile.getOffset(index));
        buffer.flip();
    }

    static String readKeyFromBuffer(ByteBuffer buffer) {
        short keySize = readLastBytesAsShort(buffer);
        buffer.limit(keySize);
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

    static String readValueFromBuffer(DaoFile daoFile, ByteBuffer buffer, int index) {
        int entrySize = daoFile.entrySize(index);
        buffer.limit(entrySize - Short.BYTES);
        if (!buffer.hasRemaining()) {
            return null;
        }
        short valueSize = readLastBytesAsShort(buffer);
        if (valueSize == 0) {
            return "";
        }
        buffer.limit(entrySize - Short.BYTES * 2);
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

    private static short readLastBytesAsShort(ByteBuffer buffer) {
        byte high = buffer.get(buffer.limit() - Short.BYTES);
        byte low = buffer.get(buffer.limit() - Short.BYTES + 1);
        return (short) ((high << 8) + low);
    }

    private Path pathToFile(int fileNumber, String fileName) {
        return pathToDirectory.resolve(fileName + fileNumber + FILE_EXTENSION);
    }

    private int initFiles(int numberOfFiles) throws IOException {
        int maxSize = 0;
        for (int i = 0; i < numberOfFiles; i++) {
            DaoFile daoFile = new DaoFile(pathToFile(i, DATA_FILE), pathToFile(i, META_FILE));
            if (daoFile.maxEntrySize() > maxSize) {
                maxSize = daoFile.maxEntrySize();
            }
            daoFiles.add(daoFile);
        }
        return maxSize;
    }
}
