package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static int fileCounter = 1;
    private static final int BUFFER_FLUSH_LIMIT = 256;
    private static final OpenOption[] writeOptions = new OpenOption[]{
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
    };
    private static final String dataFileName = "daoData";
    private static final String dataFileExtension = ".txt";
    private final Config config;
    private final ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();

    public InMemoryDao() {
        config = null;
    }

    public InMemoryDao(Config config) {
        this.config = config;
    }

    class MergeIterator implements Iterator<BaseEntry<String>> {
        private final TreeMap<Path, Map.Entry<BufferedReader, String>> filesReadMap = new TreeMap<>();
        private final ConcurrentSkipListMap<String, BaseEntry<String>> tempData = new ConcurrentSkipListMap<>();
        private final Iterator<BaseEntry<String>> inMemoryIterator;
        private final String from;
        private final String to;
        private String inMemoryLastKey;
        boolean isFromNull;
        boolean isToNull;

        public MergeIterator(Config config, String from, String to) throws IOException {
            this.from = from;
            this.to = to;
            isFromNull = from == null;
            isToNull = to == null;
            for (int i = 1; i < fileCounter; i++) {
                Path path = config.basePath().resolve(dataFileName + i + dataFileExtension);
                BufferedReader bufferedReader;
                try {
                    bufferedReader = Files.newBufferedReader(path);
                } catch (NoSuchFileException e) {
                    continue;
                }
                String fileMinKey = readKey(bufferedReader);
                String fileMaxKey = readKey(bufferedReader);
                BaseEntry<String> firstEntry = null;
                if (isFromNull) {
                    firstEntry = readEntry(bufferedReader);
                } else if (from.compareTo(fileMaxKey) <= 0) {
                    firstEntry = ceilKey(path, bufferedReader, from);
                }
                if (firstEntry != null && (isToNull || to.compareTo(fileMinKey) >= 0)) {
                    tempData.put(firstEntry.key(), firstEntry);
                    Map.Entry<BufferedReader, String> lastReadElem = new AbstractMap.SimpleEntry<>(bufferedReader, firstEntry.key());
                    filesReadMap.put(path, lastReadElem);
                }
            }
            inMemoryIterator = getInMemoryDataIterator(from, to);
            if (inMemoryIterator.hasNext()) {
                BaseEntry<String> entry = inMemoryIterator.next();
                tempData.put(entry.key(), entry);
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
                for (Map.Entry<BufferedReader, String> readerWithLastElemPair : filesReadMap.values()) {
                    if (readerWithLastElemPair.getValue().equals(firstEntry.key())) {
                        BaseEntry<String> newEntry = readEntry(readerWithLastElemPair.getKey());
                        if (newEntry != null) {
                            tempData.put(newEntry.key(), newEntry);
                            readerWithLastElemPair.setValue(newEntry.key());
                        }
                    }
                }
                if (inMemoryIterator.hasNext() && inMemoryLastKey.equals(firstEntry.key())) {
                    BaseEntry<String> newEntry = inMemoryIterator.next();
                    tempData.put(newEntry.key(), newEntry);
                    inMemoryLastKey = newEntry.key();
                }
                if (tempData.isEmpty() || (!isToNull && tempData.firstKey().compareTo(to) >= 0)) {
                    tempData.clear();
                    for (Map.Entry<BufferedReader, String> readerWithLastElemPair : filesReadMap.values()) {
                        readerWithLastElemPair.getKey().close();
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
            Path path = config.basePath().resolve(dataFileName + i + dataFileExtension);
            BaseEntry<String> entry = searchInFile(path, key);
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
        Path dataFilePath = config.basePath().resolve(dataFileName + fileCounter++ + dataFileExtension);
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(dataFilePath, UTF_8, writeOptions)) {
            int bufferedEntriesCounter = 0;
            String fileMinKey = data.firstKey();
            String fileMaxKey = data.lastKey();
            bufferedFileWriter.write(intToCharArray(fileMinKey.length()));
            bufferedFileWriter.write(fileMinKey);
            bufferedFileWriter.write(intToCharArray(fileMaxKey.length()));
            bufferedFileWriter.write(fileMaxKey);
            bufferedFileWriter.write(intToCharArray(0));
            for (BaseEntry<String> baseEntry : data.values()) {
                bufferedFileWriter.write(intToCharArray(baseEntry.key().length()));
                bufferedFileWriter.write(baseEntry.key());
                bufferedFileWriter.write(baseEntry.value() + '\n');
                bufferedFileWriter.write(intToCharArray(Integer.BYTES + Integer.BYTES + baseEntry.key().length() + baseEntry.value().length() + 1));
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

    private static BaseEntry<String> ceilKey(Path path, BufferedReader bufferedReader, String key) throws IOException {
        int keyLength;
        int prevEntryLength;
        char[] keyChars;
        String currentKey;
        long left = 0;
        long right = Files.size(path) - Integer.BYTES;
        long mid;
        while (left <= right) {
            mid = (left + right) / 2;
            bufferedReader.mark((int) right);
            bufferedReader.skip(mid - left);
            int readBytes = bufferedReader.readLine().length();
            prevEntryLength = readUnsignedInt(bufferedReader);
            if (mid + prevEntryLength >= right) {
                bufferedReader.reset();
                bufferedReader.skip(Integer.BYTES);
                right = mid - prevEntryLength + readBytes + 1;
            }
            keyLength = readUnsignedInt(bufferedReader);
            keyChars = new char[keyLength];
            bufferedReader.read(keyChars);
            currentKey = new String(keyChars);
            String currentValue = bufferedReader.readLine();
            int compareResult = key.compareTo(currentKey);
            if (compareResult == 0) {
                return new BaseEntry<>(currentKey, currentValue);
            }
            if (compareResult > 0) {
                bufferedReader.mark(0);
                bufferedReader.skip(Integer.BYTES);
                keyLength = readUnsignedInt(bufferedReader);
                if (keyLength == -1) {
                    return null;
                }
                keyChars = new char[keyLength];
                bufferedReader.read(keyChars);
                String nextKey = new String(keyChars);
                if (key.compareTo(nextKey) <= 0) {
                    return new BaseEntry<>(nextKey, bufferedReader.readLine());
                }
                left = mid - prevEntryLength + readBytes;
                bufferedReader.reset();
            } else {
                right = mid + readBytes;
                bufferedReader.reset();
            }
        }
        return null;
    }

    private static BaseEntry<String> readEntry(BufferedReader bufferedReader) throws IOException {
        bufferedReader.skip(Integer.BYTES);
        int keyLength = readUnsignedInt(bufferedReader);
        if (keyLength == -1) {
            return null;
        }
        char[] keyChars = new char[keyLength];
        bufferedReader.read(keyChars);
        return new BaseEntry<>(new String(keyChars), bufferedReader.readLine());
    }

    private static String readKey(BufferedReader bufferedReader) throws IOException {
        char[] keyChars;
        int keyLength;
        keyLength = readUnsignedInt(bufferedReader);
        keyChars = new char[keyLength];
        bufferedReader.read(keyChars);
        return new String(keyChars);
    }

    private static char[] intToCharArray(int k) {
        char[] writeBuffer = new char[Integer.BYTES];
        writeBuffer[0] = (char) (k >>> 24);
        writeBuffer[1] = (char) (k >>> 16);
        writeBuffer[2] = (char) (k >>> 8);
        writeBuffer[3] = (char) (k >>> 0);
        return writeBuffer;
    }

    public static int readUnsignedInt(BufferedReader bufferedReader) throws IOException {
        int ch1 = bufferedReader.read();
        int ch2 = bufferedReader.read();
        int ch3 = bufferedReader.read();
        int ch4 = bufferedReader.read();
        if (ch1 == -1 || ch2 == -1 || ch3 == -1 || ch4 == -1) {
            return -1;
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    private static BaseEntry<String> searchInFile(Path path, String key) throws IOException {
        try (BufferedReader bufferedReader = Files.newBufferedReader(path, UTF_8)) {
            String fileMinKey = readKey(bufferedReader);
            String fileMaxKey = readKey(bufferedReader);
            if (key.compareTo(fileMinKey) < 0 || key.compareTo(fileMaxKey) > 0) {
                return null;
            }
            int keyLength;
            int prevEntryLength;
            char[] keyChars;
            String currentKey;
            long left = 0;
            long right = Files.size(path) - Integer.BYTES;
            long mid;
            while (left <= right) {
                mid = (left + right) / 2;
                bufferedReader.mark((int) right);
                bufferedReader.skip(mid - left);
                int readBytes = bufferedReader.readLine().length();
                prevEntryLength = readUnsignedInt(bufferedReader);
                if (mid + prevEntryLength >= right) {
                    bufferedReader.reset();
                    bufferedReader.skip(Integer.BYTES);
                    right = mid - prevEntryLength + readBytes + 1;
                }
                keyLength = readUnsignedInt(bufferedReader);
                keyChars = new char[keyLength];
                bufferedReader.read(keyChars);
                currentKey = new String(keyChars);
                int compareResult = key.compareTo(currentKey);
                if (compareResult > 0) {
                    left = mid - prevEntryLength + readBytes;
                } else if (compareResult < 0) {
                    right = mid + readBytes;
                    bufferedReader.reset();
                } else {
                    return new BaseEntry<>(currentKey, bufferedReader.readLine());
                }
            }
            return null;
        } catch (NoSuchFileException e) {
            return null;
        }
    }

}
