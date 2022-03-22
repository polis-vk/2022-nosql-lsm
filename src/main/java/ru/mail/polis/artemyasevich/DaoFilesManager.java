package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DaoFilesManager {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final Path pathToDirectory;
    private final long[][] offsets;
    private boolean allMetaProcessed;
    private int numberOfFiles;

    public DaoFilesManager(Path pathToDirectory) {
        File[] files = pathToDirectory.toFile().listFiles();
        this.pathToDirectory = pathToDirectory;
        this.numberOfFiles = files == null ? 0 : files.length / 2;
        this.offsets = new long[numberOfFiles][];
    }

    public void savaData(Map<String, BaseEntry<String>> dataMap) throws IOException {
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
            metaStream.writeInt(dataMap.size());
            dataStream.writeUTF(entry.key());
            if (entry.value() != null) {
                dataStream.writeUTF(entry.value());
            }
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

    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry;
        for (int fileNumber = numberOfFiles - 1; fileNumber >= 0; fileNumber--) {
            if (offsets[fileNumber] == null) {
                offsets[fileNumber] = readOffsets(fileNumber);
            }
            RandomAccessFile reader = new RandomAccessFile(pathToFile(fileNumber, DATA_FILE).toFile(), "r");
            entry = findValidClosest(key, null, reader, offsets[fileNumber], null);
            reader.close();
            if (entry != null) {
                return entry.value() == null || !entry.key().equals(key) ? null : entry;
            }
        }
        return null;
    }

    public List<Iterator<BaseEntry<String>>> iterators(String from, String to) throws IOException {
        if (!allMetaProcessed) {
            processAllMeta();
        }
        List<Iterator<BaseEntry<String>>> iterators = new ArrayList<>(numberOfFiles + 1);
        for (int fileNumber = 0; fileNumber < numberOfFiles; fileNumber++) {
            iterators.add(new FileIterator(from, to, pathToFile(fileNumber, DATA_FILE), offsets[fileNumber]));
        }
        return iterators;
    }

    public long[] readOffsets(int fileNumber) throws IOException {
        long[] fileOffsets;
        try (DataInputStream metaStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(pathToFile(fileNumber, META_FILE))))) {
            int dataSize = metaStream.readInt();
            fileOffsets = new long[dataSize + 1];
            long currentOffset = 0;
            fileOffsets[0] = currentOffset;
            int i = 1;
            while (metaStream.available() > 0) {
                int numberOfEntries = metaStream.readInt();
                int entryBytesSize = metaStream.readInt();
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets[i] = currentOffset;
                    i++;
                }
            }
        }
        return fileOffsets;
    }

    private void processAllMeta() throws IOException {
        for (int i = 0; i < numberOfFiles; i++) {
            if (offsets[i] == null) {
                offsets[i] = readOffsets(i);
            }
        }
        allMetaProcessed = true;
    }

    private Path pathToFile(int fileNumber, String fileName) {
        return pathToDirectory.resolve(fileName + fileNumber + FILE_EXTENSION);
    }

    private static BaseEntry<String> findValidClosest(String from, String to, RandomAccessFile reader, long[] offsets,
                                                      int[] entryToReadWrapper) throws IOException {
        int left = 0;
        int right = offsets.length - 2;
        String validKey = null;
        String validValue = null;
        int validEntryIndex = 0;
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            reader.seek(offsets[middle]);
            String key = reader.readUTF();
            int comparison = from.compareTo(key);
            if (comparison <= 0) {
                String value = reader.getFilePointer() == offsets[middle + 1] ? null : reader.readUTF();
                if (comparison == 0) {
                    if (entryToReadWrapper != null) {
                        entryToReadWrapper[0] = middle + 1;
                    }
                    return new BaseEntry<>(key, value);
                } else {
                    right = middle - 1;
                    validKey = key;
                    validValue = value;
                    validEntryIndex = middle;
                }
            } else {
                left = middle + 1;
            }
        }
        if (validKey == null) {
            return null;
        }
        if (entryToReadWrapper != null) {
            entryToReadWrapper[0] = validEntryIndex + 1;
        }
        return to != null && validKey.compareTo(to) >= 0 ? null : new BaseEntry<>(validKey, validValue);
    }

    private static class FileIterator implements Iterator<BaseEntry<String>> {
        private final RandomAccessFile reader;
        private final long[] offsets;
        private final String to;
        private final int[] entryToRead;
        private BaseEntry<String> next;

        private FileIterator(String from, String to, Path pathToData, long[] offsets) throws IOException {
            this.entryToRead = new int[1];
            this.offsets = offsets;
            this.reader = new RandomAccessFile(pathToData.toFile(), "r");
            this.to = to;
            BaseEntry<String> entry = from == null
                    ? readEntry() : findValidClosest(from, to, reader, offsets, entryToRead);
            if (entry == null) {
                reader.close();
            }
            next = entry;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public BaseEntry<String> next() {
            BaseEntry<String> nextToGive = next;
            try {
                next = readEntry();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return nextToGive;
        }

        private BaseEntry<String> readEntry() throws IOException {
            reader.seek(offsets[entryToRead[0]]);
            if (reader.getFilePointer() == offsets[offsets.length - 1]) {
                reader.close();
                return null;
            }
            String key = reader.readUTF();
            if (to != null && key.compareTo(to) >= 0) {
                reader.close();
                return null;
            }
            String value = reader.getFilePointer() == offsets[entryToRead[0] + 1] ? null : reader.readUTF();
            entryToRead[0]++;
            return new BaseEntry<>(key, value);
        }
    }

}
