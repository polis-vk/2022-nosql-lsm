package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public class DaoReader {

    private final Path pathToDataFile;
    private final String endReadFactor;
    private int startReadIndex;
    private BaseEntry<String> nextEntry;
    private final long[] offsets;

    public DaoReader(Path pathToDataFile, Path pathToOffsetsFile, String from, String to) throws IOException {
        this.pathToDataFile = pathToDataFile;
        this.endReadFactor = to;
        try (RandomAccessFile reader = new RandomAccessFile(pathToOffsetsFile.toString(), "r")) {
            this.offsets = new long[reader.readInt()];
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = reader.readLong();
            }
        }
        this.startReadIndex = from == null ? 0 : findByRange(from, to);
        readNextEntry();
    }

    public DaoReader(Path pathToDataFile, Path pathToOffsetsFile) throws IOException {
        this.pathToDataFile = pathToDataFile;
        this.endReadFactor = null;
        try (RandomAccessFile reader = new RandomAccessFile(pathToOffsetsFile.toString(), "r")) {
            this.offsets = new long[reader.readInt()];
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = reader.readLong();
            }
        }
    }

    public BaseEntry<String> findByKey(String key) throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(pathToDataFile.toString(), "r")) {
            int start = 0;
            int finish = offsets.length;
            BaseEntry<String> result = null;
            while (start <= finish) {
                int middle = start + (finish - start) / 2;
                reader.seek(offsets[middle]);
                BaseEntry<String> current = new BaseEntry<>(reader.readUTF(), reader.readUTF());
                int comparison = current.key().compareTo(key);
                if (comparison < 0) {
                    start = middle + 1;
                } else if (comparison == 0) {
                    result = current;
                    break;
                } else {
                    finish = middle - 1;
                }
            }
            return result;
        }
    }

    public BaseEntry<String> readNextEntry() throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(pathToDataFile.toString(), "r")) {
            BaseEntry<String> result = nextEntry;
            if (startReadIndex < offsets.length && startReadIndex != -1) {
                reader.seek(offsets[startReadIndex]);
                String currentKey = reader.readUTF();
                if (endReadFactor != null && currentKey.compareTo(endReadFactor) >= 0) {
                    nextEntry = null;
                } else {
                    nextEntry = new BaseEntry<>(currentKey, reader.readUTF());
                }
                startReadIndex += 1;
            } else {
                nextEntry = null;
            }
            return result;
        }
    }

    private int findByRange(String from, String to) throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(pathToDataFile.toString(), "r")) {
            int start = 0;
            int finish = offsets.length;
            int resultIndex = -1;
            while (start <= finish) {
                int middle = start + (finish - start) / 2;
                if (middle >= offsets.length) {
                    return resultIndex;
                }
                reader.seek(offsets[middle]);
                BaseEntry<String> current = new BaseEntry<>(reader.readUTF(), reader.readUTF());
                int comparisonWithFrom = current.key().compareTo(from);
                if (comparisonWithFrom < 0) {
                    start = middle + 1;
                } else if (comparisonWithFrom == 0) {
                    resultIndex = middle;
                    break;
                } else {
                    finish = middle - 1;
                    if (to == null || current.key().compareTo(to) < 0) {
                        resultIndex = middle;
                    }
                }
            }
            return resultIndex;
        }
    }
}
