package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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

public class Storage {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final Map<Thread, ByteBuffer> entryBuffer;
    private final Map<Thread, CharBuffer> searchedKeyBuffer;
    private final Path pathToDirectory;
    private final List<DaoFile> daoFiles;
    private final int bufferSize;

    Storage(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int numberOfFiles = files == null ? 0 : files.length / 2;
        this.daoFiles = new ArrayList<>(numberOfFiles);
        this.bufferSize = initFiles(numberOfFiles);
        this.entryBuffer = Collections.synchronizedMap(new WeakHashMap<>());
        this.searchedKeyBuffer = Collections.synchronizedMap(new WeakHashMap<>());
    }

    BaseEntry<String> get(String key) throws IOException {
        if (key.length() > maxKeyLength()) {
            return null;
        }
        for (int fileNumber = daoFiles.size() - 1; fileNumber >= 0; fileNumber--) {
            DaoFile daoFile = daoFiles.get(fileNumber);
            int entryIndex = getEntryIndex(key, daoFile);
            if (entryIndex > daoFile.getLastIndex()) {
                continue;
            }
            BaseEntry<String> entry = readEntry(daoFile, entryIndex);
            if (entry.key().equals(key)) {
                return entry.value() == null ? null : entry;
            }
        }
        return null;
    }

    Iterator<BaseEntry<String>> iterate(String from, String to) throws IOException {
        List<PeekIterator> iterators = new ArrayList<>(daoFiles.size());
        for (int fileNumber = 0; fileNumber < daoFiles.size(); fileNumber++) {
            int sourceNumber = daoFiles.size() - fileNumber;
            iterators.add(new PeekIterator(new FileIterator(from, to, daoFiles.get(fileNumber)), sourceNumber));
        }
        return new MergeIterator(iterators);
    }

    void close() throws IOException {
        for (DaoFile daoFile : daoFiles) {
            daoFile.close();
        }
    }

    void savaData(Map<String, BaseEntry<String>> dataMap) throws IOException {
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

    private int getEntryIndex(String key, DaoFile daoFile) throws IOException {
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            fillBufferWithEntry(daoFile, middle);
            CharBuffer middleKey = bufferAsKeyOnly();
            CharBuffer keyToFind = fillAndGetKeyBuffer(key);
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

    private BaseEntry<String> readEntry(DaoFile daoFile, int entryIndex) throws IOException {
        ByteBuffer buffer = getEntryBuffer();
        fillBufferWithEntry(daoFile, entryIndex);
        String key = bufferAsKeyOnly().toString();
        buffer.limit(daoFile.entrySize(entryIndex));
        String value = null;
        if (buffer.hasRemaining()) {
            short valueSize = buffer.getShort();
            value = valueSize == 0 ? "" : buffer.asCharBuffer().toString();
        }
        return new BaseEntry<>(key, value);
    }

    private CharBuffer bufferAsKeyOnly() {
        ByteBuffer buffer = getEntryBuffer();
        short keySize = buffer.getShort();
        buffer.limit(keySize + Short.BYTES);
        CharBuffer key = buffer.asCharBuffer();
        buffer.position(Short.BYTES + keySize);
        return key;
    }

    private void fillBufferWithEntry(DaoFile daoFile, int entryIndex) throws IOException {
        ByteBuffer buffer = getEntryBuffer();
        buffer.clear();
        buffer.limit(daoFile.entrySize(entryIndex));
        daoFile.getChannel().read(buffer, daoFile.getOffset(entryIndex));
        buffer.flip();
    }

    private ByteBuffer getEntryBuffer() {
        return entryBuffer.computeIfAbsent(Thread.currentThread(), k -> ByteBuffer.allocate(bufferSize));
    }

    private CharBuffer fillAndGetKeyBuffer(String key) {
        CharBuffer buffer = searchedKeyBuffer.computeIfAbsent(Thread.currentThread(),
                k -> CharBuffer.allocate(maxKeyLength()));
        buffer.clear();
        buffer.put(key);
        buffer.flip();
        return buffer;
    }

    private int maxKeyLength() {
        //The length of the longest entry with null value
        return bufferSize / 2 - Short.BYTES / 2;
    }

    private Path pathToFile(int fileNumber, String fileName) {
        return pathToDirectory.resolve(fileName + fileNumber + FILE_EXTENSION);
    }

    private int initFiles(int files) throws IOException {
        int maxSize = 0;
        for (int i = 0; i < files; i++) {
            DaoFile daoFile = new DaoFile(pathToFile(i, DATA_FILE), pathToFile(i, META_FILE));
            if (daoFile.maxEntrySize() > maxSize) {
                maxSize = daoFile.maxEntrySize();
            }
            daoFiles.add(daoFile);
        }
        return maxSize;
    }

    private class FileIterator implements Iterator<BaseEntry<String>> {
        private final DaoFile daoFile;
        private final String to;
        private int entryToRead;
        private BaseEntry<String> next;

        public FileIterator(String from, String to, DaoFile daoFile) throws IOException {
            this.daoFile = daoFile;
            this.to = to;
            this.entryToRead = from == null ? 0 : getEntryIndex(from, daoFile);
            this.next = getNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public BaseEntry<String> next() {
            BaseEntry<String> nextToGive = next;
            try {
                next = getNext();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return nextToGive;
        }

        private BaseEntry<String> getNext() throws IOException {
            if (daoFile.getOffset(entryToRead) == daoFile.sizeOfFile()) {
                return null;
            }
            BaseEntry<String> entry = readEntry(daoFile, entryToRead);
            if (to != null && entry.key().compareTo(to) >= 0) {
                return null;
            }
            entryToRead++;
            return entry;
        }
    }
}
