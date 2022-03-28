package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class StringDao implements Dao<String, BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Map<Thread, ByteBuffer> buffers;
    private final Path pathToDirectory;
    private final List<DaoFile> daoFiles;
    private final int bufferSize;

    public StringDao(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int numberOfFiles = files == null ? 0 : files.length / 2;
        this.daoFiles = new ArrayList<>(numberOfFiles);
        bufferSize = initFiles(numberOfFiles);
        buffers = Collections.synchronizedMap(new WeakHashMap<>());

    }

    public StringDao() {
        pathToDirectory = null;
        daoFiles = null;
        buffers = null;
        bufferSize = 0;
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
            ByteBuffer buffer = buffers.computeIfAbsent(Thread.currentThread(), k -> ByteBuffer.allocate(bufferSize));
            iterators.add(new PeekIterator(new FileIterator(from, to, daoFile, buffer), sourceNumber));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = dataMap.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        ByteBuffer buffer = buffers.computeIfAbsent(Thread.currentThread(), k -> ByteBuffer.allocate(bufferSize));
        for (int fileNumber = daoFiles.size() - 1; fileNumber >= 0; fileNumber--) {
            DaoFile daoFile = daoFiles.get(fileNumber);
            int entryIndex = getEntryIndex(key, buffer, daoFile);
            if (entryIndex > daoFile.getLastIndex()) {
                continue;
            }
            entry = readEntryFromBuffer(buffer, daoFile, entryIndex);
            if (entry.key().equals(key)) {
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

    static int getEntryIndex(String key, ByteBuffer buffer, DaoFile daoFile) throws IOException {
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            fillBufferWithEntry(buffer, daoFile, middle);
            CharBuffer keyToFind = CharBuffer.wrap(key);
            CharBuffer middleKey = StringDao.keyFilledBuffer(buffer);
            int comparison = keyToFind.compareTo(middleKey);
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
        fillBufferWithEntry(buffer, daoFile, index);
        String key = keyFilledBuffer(buffer).toString();
        buffer.limit(daoFile.entrySize(index));
        String value = null;
        if (buffer.hasRemaining()) {
            short valueSize = buffer.getShort();
            value = valueSize == 0 ? "" : buffer.asCharBuffer().toString();
        }
        return new BaseEntry<>(key, value);
    }

    private static CharBuffer keyFilledBuffer(ByteBuffer buffer) {
        short keySize = buffer.getShort();
        buffer.limit(keySize + Short.BYTES);
        CharBuffer key = buffer.asCharBuffer();
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
