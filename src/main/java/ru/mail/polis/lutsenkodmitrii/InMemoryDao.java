package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final int MAX_FILES_NUMBER = 20;
    private static final int BUFFER_FLUSH_LIMIT = 256;
    private static final OpenOption[] writeOptions = new OpenOption[]{
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
    };
    private final ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();
    private final Path keyRangesPath;
    private final Config config;

    public InMemoryDao() {
        keyRangesPath = null;
        config = null;
    }

    public InMemoryDao(Config config) {
        this.config = config;
        keyRangesPath = config.basePath().resolve("daoKeyRanges.txt");
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
    public BaseEntry<String> get(String key) {
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
        int oneFileDataSize = data.size() / filesNumber;
        int leastFileDataSize = oneFileDataSize + data.size() % filesNumber;
        List<BaseEntry<String>> dataList = data.values().parallelStream().toList();
        ExecutorService executor = Executors.newCachedThreadPool();
        CountDownLatch countDownLatch = new CountDownLatch(filesNumber);
        Path[] paths = new Path[filesNumber];

        for (int i = 0; i < paths.length; i++) {
            paths[i] = config.basePath().resolve("daoData" + (i + 1) + ".txt");
        }
        try (BufferedWriter bufferedKeyRangesFileWriter
                     = Files.newBufferedWriter(keyRangesPath, StandardCharsets.UTF_8, writeOptions)) {
            bufferedKeyRangesFileWriter.write(intToCharArray(filesNumber));
            bufferedKeyRangesFileWriter.flush();
            for (int i = 0; i < filesNumber; i++) {
                int fileIndex = i;
                executor.execute(() -> {
                    writeToFile(oneFileDataSize, leastFileDataSize, dataList, countDownLatch,
                            paths[fileIndex], bufferedKeyRangesFileWriter, fileIndex);
                });
            }
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
        executor.shutdown();
    }

    private void writeToFile(int oneFileDataSize, int leastFileDataSize, List<BaseEntry<String>> dataList,
                             CountDownLatch countDownLatch, Path path,
                             BufferedWriter bufferedKeyRangesFileWriter, int fileIndex) {
        try (BufferedWriter bufferedFileWriter
                     = Files.newBufferedWriter(path, StandardCharsets.UTF_8, writeOptions)) {
            List<BaseEntry<String>> list;
            if (fileIndex == 0) {
                list = dataList.subList(0, leastFileDataSize);
            } else {
                int k = leastFileDataSize + (fileIndex - 1) * oneFileDataSize;
                list = dataList.subList(k, k + oneFileDataSize);
            }
            bufferedFileWriter.write(intToCharArray(list.size()));
            String stingToWrite;
            int counter = 0;
            for (BaseEntry<String> baseEntry : list) {
                stingToWrite = baseEntry.key() + baseEntry.value() + '\n';
                bufferedFileWriter.write(intToCharArray(baseEntry.key().length()));
                bufferedFileWriter.write(intToCharArray(baseEntry.value().length()));
                counter++;
                bufferedFileWriter.write(stingToWrite);
                if (counter % BUFFER_FLUSH_LIMIT == 0) {
                    bufferedFileWriter.flush();
                }
            }
            bufferedFileWriter.flush();
            String firstKey = list.get(0).key();
            String lastKey = list.get(list.size() - 1).key();
            String pathInString = path.toString();
            synchronized (bufferedKeyRangesFileWriter) {
                bufferedKeyRangesFileWriter.write(intToCharArray(firstKey.length()));
                bufferedKeyRangesFileWriter.write(intToCharArray(lastKey.length()));
                bufferedKeyRangesFileWriter.write(intToCharArray(pathInString.length()));
                bufferedKeyRangesFileWriter.write(firstKey);
                bufferedKeyRangesFileWriter.write(lastKey);
                bufferedKeyRangesFileWriter.write(pathInString + '\n');
                bufferedKeyRangesFileWriter.flush();
                countDownLatch.countDown();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private BaseEntry<String> getEntryFromFileByKey(String key, Path fileContainsKeyPath) {
        if (fileContainsKeyPath == null) {
            return null;
        }
        try (BufferedReader bufferedReader = Files.newBufferedReader(fileContainsKeyPath, StandardCharsets.UTF_8)) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Path getPathOfDataFile(String key) {
        try (BufferedReader bufferedReader = Files.newBufferedReader(keyRangesPath, StandardCharsets.UTF_8)) {
            int firstKeyLength;
            int lastKeyLength;
            int pathLength;
            char[] firstKeyBytes;
            char[] lastKeyBytes;
            String firstKey;
            String lastKey;
            int size = readInt(bufferedReader);
            for (int i = 0; i < size; i++) {
                firstKeyLength = readInt(bufferedReader);
                lastKeyLength = readInt(bufferedReader);
                pathLength = readInt(bufferedReader);
                firstKeyBytes = new char[firstKeyLength];
                lastKeyBytes = new char[lastKeyLength];
                bufferedReader.read(firstKeyBytes);
                bufferedReader.read(lastKeyBytes);
                firstKey = new String(firstKeyBytes);
                lastKey = new String(lastKeyBytes);
                if (key.compareTo(firstKey) >= 0 && key.compareTo(lastKey) <= 0) {
                    char[] pathBytes = new char[pathLength];
                    bufferedReader.read(pathBytes);
                    return Path.of(new String(pathBytes));
                }
                bufferedReader.skip(pathLength + 1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
