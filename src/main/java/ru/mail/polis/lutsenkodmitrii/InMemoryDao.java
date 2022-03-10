package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final Logger logger = Logger.getLogger(InMemoryDao.class.getName());
    private static final int MAX_FILES_NUMBER = 20;
    private static final int BUFFER_FLUSH_LIMIT = 256;
    private static final OpenOption[] writeOptions = new OpenOption[]{
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
    };
    private final Path keyRangesPath;
    private final Config config;
    private final ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<String, String> keyRangesMap = new ConcurrentSkipListMap<>();
    private int oneFileDataSize;
    private int leastFileDataSize;

    public InMemoryDao() {
        keyRangesPath = null;
        config = null;
    }

    public InMemoryDao(Config config) {
        this.config = config;
        keyRangesPath = config.basePath().resolve("daoKeyRanges.txt");
        if (Files.exists(keyRangesPath)) {
            loadKeyRangesMap();
        }
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        if (from == null && to == null) {
            return data.values().iterator();
        }
        if (from == null) {
            return data.headMap(to).values().iterator();
        }
        if (to == null) {
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (data.containsKey(key)) {
            return data.get(key);
        }
        return getEntryFromFileByKey(key, getPathOfDataFile(key));
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (data.isEmpty()) {
            return;
        }
        int filesNumber = Math.min(MAX_FILES_NUMBER, data.size());
        oneFileDataSize = data.size() / filesNumber;
        leastFileDataSize = oneFileDataSize + data.size() % filesNumber;
        List<BaseEntry<String>> dataList = new ArrayList<>(data.values());
        ExecutorService executor = Executors.newCachedThreadPool();
        CountDownLatch countDownLatch = new CountDownLatch(filesNumber);
        Path[] paths = new Path[filesNumber];
        keyRangesMap = new ConcurrentSkipListMap<>();

        for (int i = 0; i < paths.length; i++) {
            paths[i] = config.basePath().resolve("daoData" + (i + 1) + ".txt");
        }
        for (int i = 0; i < filesNumber; i++) {
            int fileIndex = i;
            executor.execute(() -> {
                writeToFile(paths[fileIndex], fileIndex, dataList, countDownLatch);
            });
        }
        try {
            countDownLatch.await();
            writeKeyRangesMapToFile();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Interrupted while flush", e);
            throw new RuntimeException("Interrupted while flush");
            // Пробросить дальше тут не получиться не меняя сигнатуру метода в Dao
            // В следующей версии эту многопоточку уберу скорее всего поэтому и это уйдет
        }
    }

    private void writeToFile(Path path, int fileIndex,
                             List<BaseEntry<String>> dataList,
                             CountDownLatch countDownLatch) {
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(path, UTF_8, writeOptions)) {
            List<BaseEntry<String>> subList;
            if (fileIndex == 0) {
                subList = dataList.subList(0, leastFileDataSize);
            } else {
                int k = leastFileDataSize + (fileIndex - 1) * oneFileDataSize;
                subList = dataList.subList(k, k + oneFileDataSize);
            }
            bufferedFileWriter.write(intToCharArray(subList.size()));
            String stingToWrite;
            int bufferedEntriesCounter = 0;
            for (BaseEntry<String> baseEntry : subList) {
                stingToWrite = baseEntry.key() + baseEntry.value() + '\n';
                bufferedFileWriter.write(intToCharArray(baseEntry.key().length()));
                bufferedFileWriter.write(intToCharArray(baseEntry.value().length()));
                bufferedEntriesCounter++;
                bufferedFileWriter.write(stingToWrite);
                if (bufferedEntriesCounter % BUFFER_FLUSH_LIMIT == 0) {
                    bufferedFileWriter.flush();
                }
            }
            bufferedFileWriter.flush();
            keyRangesMap.put(subList.get(0).key(), path.toString());
            countDownLatch.countDown();
        } catch (IOException e) {
            logger.log(Level.SEVERE,
                    "Writing to file " + path.getFileName() + " with index " + fileIndex + " failed: ", e);
        }
    }

    private BaseEntry<String> getEntryFromFileByKey(String key, Path fileContainsKeyPath) throws IOException {
        if (fileContainsKeyPath == null) {
            return null;
        }
        try (BufferedReader bufferedReader = Files.newBufferedReader(fileContainsKeyPath, UTF_8)) {
            int currentKeyLength;
            int valueLength;
            char[] currentKeyChars;
            String currentKey;
            int size = readInt(bufferedReader);
            for (int i = 0; i < size; i++) {
                currentKeyLength = readInt(bufferedReader);
                valueLength = readInt(bufferedReader);
                currentKeyChars = new char[currentKeyLength];
                bufferedReader.read(currentKeyChars);
                currentKey = new String(currentKeyChars);
                if (currentKey.equals(key)) {
                    char[] valueChars = new char[valueLength];
                    bufferedReader.read(valueChars);
                    return new BaseEntry<>(currentKey, new String(valueChars));
                }
                bufferedReader.skip(valueLength + 1);
            }
        }
        return null;
    }

    private void loadKeyRangesMap() {
        try (BufferedReader bufferedReader = Files.newBufferedReader(keyRangesPath, UTF_8)) {
            int keyLength;
            int valueLength;
            char[] keyChars;
            char[] valueChars;
            int size = readInt(bufferedReader);
            for (int i = 0; i < size; i++) {
                keyLength = readInt(bufferedReader);
                valueLength = readInt(bufferedReader);
                keyChars = new char[keyLength];
                valueChars = new char[valueLength];
                bufferedReader.read(keyChars);
                bufferedReader.read(valueChars);
                keyRangesMap.put(new String(keyChars), new String(valueChars));
            }
        } catch (IOException e) {
            throw new RuntimeException("Load Key Ranges map fail", e);
        }
    }

    private Path getPathOfDataFile(String key) {
        Map.Entry<String, String> keyPathEntry = keyRangesMap.floorEntry(key);
        return keyPathEntry == null ? null : Path.of(keyPathEntry.getValue());
    }

    private void writeKeyRangesMapToFile() throws IOException {
        try (BufferedWriter bufferedFileWriter = Files.newBufferedWriter(keyRangesPath, UTF_8, writeOptions)) {
            bufferedFileWriter.write(intToCharArray(keyRangesMap.size()));
            for (Map.Entry<String, String> entry : keyRangesMap.entrySet()) {
                bufferedFileWriter.write(intToCharArray(entry.getKey().length()));
                bufferedFileWriter.write(intToCharArray(entry.getValue().length()));
                bufferedFileWriter.write(entry.getKey());
                bufferedFileWriter.write(entry.getValue());
            }
            bufferedFileWriter.flush();
        }
    }

    private static char[] intToCharArray(int k) {
        char[] writeBuffer = new char[Integer.BYTES];
        writeBuffer[0] = (char) (k >>> 24);
        writeBuffer[1] = (char) (k >>> 16);
        writeBuffer[2] = (char) (k >>> 8);
        writeBuffer[3] = (char) (k >>> 0);
        return writeBuffer;
    }

    public static int readInt(BufferedReader bufferedReader) throws IOException {
        int ch1 = bufferedReader.read();
        int ch2 = bufferedReader.read();
        int ch3 = bufferedReader.read();
        int ch4 = bufferedReader.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }
}
