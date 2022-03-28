package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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

public class StringDao implements Dao<String, BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Path pathToDirectory;
    private final List<DaoFile> daoFiles;
    private final ThreadLocal<ByteBuffer> threadLocalBuffer;

    public StringDao(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int numberOfFiles = files == null ? 0 : files.length / 2;
        this.daoFiles = new ArrayList<>(numberOfFiles);
        int maxEntrySize = initFiles(numberOfFiles);
        this.threadLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(maxEntrySize));
    }

    public StringDao() {
        pathToDirectory = null;
        daoFiles = null;
        threadLocalBuffer = null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (to != null && to.equals(from)) {
            return Collections.emptyIterator();
        }
        List<PeekIterator> iterators = new ArrayList<>(daoFiles.size() + 1);
        iterators.add(new PeekIterator(getDataMapIterator(from, to), 0));
        for (int fileNumber = 0; fileNumber < daoFiles.size(); fileNumber++) {
            int sourceNumber = daoFiles.size() - fileNumber;
            DaoFile daoFile = daoFiles.get(fileNumber);
            iterators.add(new PeekIterator(new FileIterator(from, to, daoFile, threadLocalBuffer.get()), sourceNumber));
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
            DaoFile daoFile = daoFiles.get(fileNumber);
            int entryIndex = getEntryIndex(key, threadLocalBuffer.get(), daoFile);
            if (entryIndex > daoFile.getLastIndex()) {
                continue;
            }
            entry = readEntryFromBuffer(threadLocalBuffer.get(), daoFile, entryIndex);
            if (entry.key().equals(key)) {
                return entry.value() == null ? null : entry;
            }
        }
        return null;
    }

    @Override
    public void compact() throws IOException {
        Iterator<BaseEntry<String>> mergeIterator = get(null, null);
        savaData(mergeIterator);
        int filesBefore = daoFiles.size();
        closeFiles();
        daoFiles.clear();
        dataMap.clear();
        for (int i = 0; i < filesBefore; i++) {
            Files.delete(pathToFile(i, DATA_FILE));
            Files.delete(pathToFile(i, META_FILE));
        }
        Files.move(pathToFile(filesBefore, DATA_FILE), pathToFile(0, DATA_FILE));
        Files.move(pathToFile(filesBefore, META_FILE), pathToFile(0, META_FILE));
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        dataMap.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        savaData(dataMap.values().iterator());
        dataMap.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
        closeFiles();
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

    static int getEntryIndex(String keyToFind, ByteBuffer buffer, DaoFile daoFile) throws IOException {
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            StringDao.fillBufferWithEntry(buffer, daoFile, middle);
            String key = StringDao.readKeyFromBuffer(buffer);
            int comparison = keyToFind.compareTo(key);
            if (comparison < 0) {
                right = middle - 1;
            } else if (comparison > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }
        return left;
    }

    private void savaData(Iterator<BaseEntry<String>> iterator) throws IOException {
        if (!iterator.hasNext()) {
            return;
        }
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToFile(daoFiles.size(), DATA_FILE), writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToFile(daoFiles.size(), META_FILE), writeOptions)
             ))) {
            BaseEntry<String> entry = iterator.next();
            int currentRepeats = 1;
            int currentBytes = writeEntryInStream(dataStream, entry);
            int size = 1;
            while (iterator.hasNext()) {
                entry = iterator.next();
                size++;
                System.out.println(size + ": " + entry);
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
            metaStream.writeInt(size);
        }
    }

    //keySize|key|valueSize|value or key|keySize if value == null
    private int writeEntryInStream(DataOutputStream dataStream, BaseEntry<String> entry) throws IOException {
        int keySize = entry.key().length() * 2;
        int valueBlockSize = 0;
        dataStream.writeShort(keySize);
        dataStream.writeChars(entry.key());
        if (entry.value() != null) {
            int valueSize = entry.value().length() * 2;
            valueBlockSize = valueSize + Short.BYTES;
            dataStream.writeShort(valueSize);
            dataStream.writeChars(entry.value());
        }
        return keySize + Short.BYTES + valueBlockSize;
    }

    static BaseEntry<String> readEntryFromBuffer(ByteBuffer buffer, DaoFile daoFile, int index) throws IOException {
        StringDao.fillBufferWithEntry(buffer, daoFile, index);
        String key = readKeyFromBuffer(buffer);
        buffer.limit(daoFile.entrySize(index));
        String value = null;
        if (buffer.hasRemaining()) {
            short valueSize = buffer.getShort();
            value = valueSize == 0 ? "" : buffer.asCharBuffer().toString();
        }
        return new BaseEntry<>(key, value);
    }

    private static String readKeyFromBuffer(ByteBuffer buffer) {
        short keySize = buffer.getShort();
        buffer.limit(keySize + Short.BYTES);
        String key = buffer.asCharBuffer().toString();
        buffer.position(Short.BYTES + keySize);
        return key;
    }

    private static void fillBufferWithEntry(ByteBuffer buffer, DaoFile daoFile, int index) throws IOException {
        buffer.clear();
        buffer.limit(daoFile.entrySize(index));
        daoFile.getChannel().read(buffer, daoFile.getOffset(index));
        buffer.flip();
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

    private void closeFiles() throws IOException {
        for (DaoFile daoFile : daoFiles) {
            daoFile.close();
        }
    }

}
