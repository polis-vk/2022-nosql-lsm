package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DaoReader implements Closeable {

    private final RandomAccessFile reader;
    private final long[] offsets;
    private String endReadFactor;
    private int startReadIndex;

    public DaoReader(Path pathToDataFile, Path pathToOffsetsFile) throws IOException {
        this.reader = new RandomAccessFile(pathToDataFile.toString(), "r");
        this.offsets = initOffsets(pathToOffsetsFile);
    }

    public BaseEntry<String> findByKey(String key) throws IOException {
        int start = 0;
        int finish = offsets.length;
        while (start <= finish) {
            int middle = start + (finish - start) / 2;
            if (middle >= offsets.length) {
                return null;
            }
            reader.seek(offsets[middle]);
            String currentKey = reader.readUTF();
            int comparison = currentKey.compareTo(key);
            if (comparison < 0) {
                start = middle + 1;
            } else if (comparison == 0) {
                boolean hasValue = reader.readBoolean();
                if (!hasValue) {
                    return new BaseEntry<>(currentKey, null);
                }
                return new BaseEntry<>(currentKey, reader.readUTF());
            } else {
                finish = middle - 1;
            }
        }
        return null;
    }

    public BaseEntry<String> readNextEntry() throws IOException {
        if (startReadIndex < offsets.length && startReadIndex != -1) {
            reader.seek(offsets[startReadIndex]);
            startReadIndex += 1;
            String currentKey = reader.readUTF();
            if (endReadFactor != null && currentKey.compareTo(endReadFactor) >= 0) {
                return null;
            } else {
                boolean hasValue = reader.readBoolean();
                if (!hasValue) {
                    return new BaseEntry<>(currentKey, null);
                }
                return new BaseEntry<>(currentKey, reader.readUTF());
            }
        } else {
            return null;
        }
    }

    public int findNearestStartIndex(String from, String to) throws IOException {
        int start = 0;
        int finish = offsets.length;
        int resultIndex = -1;
        while (start <= finish) {
            int middle = start + (finish - start) / 2;
            if (middle >= offsets.length) {
                return resultIndex;
            }
            reader.seek(offsets[middle]);
            String currentKey = reader.readUTF();
            int comparisonWithFrom = currentKey.compareTo(from);
            if (comparisonWithFrom < 0) {
                start = middle + 1;
            } else if (comparisonWithFrom == 0) {
                resultIndex = middle;
                break;
            } else {
                finish = middle - 1;
                if (to == null || currentKey.compareTo(to) < 0) {
                    resultIndex = middle;
                }
            }
        }
        return resultIndex;
    }

    public void setEndReadFactor(String endReadFactor) {
        this.endReadFactor = endReadFactor;
    }

    public void setStartReadIndex(String from, String to) throws IOException {
        this.startReadIndex = from == null ? 0 : findNearestStartIndex(from, to);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private long[] initOffsets(Path pathToOffsetsFile) throws IOException {
        long[] fileOffsets;
        try (DataInputStream offsetsFileReader = new DataInputStream(
                new BufferedInputStream(
                        Files.newInputStream(
                                pathToOffsetsFile,
                                StandardOpenOption.READ
                        )))) {
            fileOffsets = new long[offsetsFileReader.readInt()];
            for (int j = 0; j < fileOffsets.length; j++) {
                fileOffsets[j] = offsetsFileReader.readLong();
            }
        }
        return fileOffsets;
    }
}
