package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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

    private final Map<Thread, EntryReadWriter> entryReadWriter;
    private final Path pathToDirectory;
    private final List<DaoFile> daoFiles;
    private final int bufferSize;

    Storage(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int numberOfFiles = files == null ? 0 : files.length / 2;
        this.daoFiles = new ArrayList<>(numberOfFiles);
        this.bufferSize = initFiles(numberOfFiles);
        this.entryReadWriter = Collections.synchronizedMap(new WeakHashMap<>());
    }

    BaseEntry<String> get(String key) throws IOException {
        EntryReadWriter entryReader = getEntryReadWriter();
        if (key.length() > entryReader.maxKeyLength()) {
            return null;
        }
        for (int fileNumber = daoFiles.size() - 1; fileNumber >= 0; fileNumber--) {
            DaoFile daoFile = daoFiles.get(fileNumber);
            int entryIndex = getEntryIndex(key, daoFile);
            if (entryIndex > daoFile.getLastIndex()) {
                continue;
            }
            BaseEntry<String> entry = entryReader.readEntryFromChannel(daoFile, entryIndex);
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

    void retainOnlyCompactedFile() throws IOException {
        close();
        int filesBefore = daoFiles.size();
        daoFiles.clear();
        for (int i = 0; i < filesBefore; i++) {
            Files.delete(pathToFile(i, DATA_FILE));
            Files.delete(pathToFile(i, META_FILE));
        }
        Files.move(pathToFile(filesBefore, DATA_FILE), pathToFile(0, DATA_FILE));
        Files.move(pathToFile(filesBefore, META_FILE), pathToFile(0, META_FILE));
    }

    void flush(Iterator<BaseEntry<String>> dataIterator) throws IOException {
        savaData(dataIterator);
        int filesBefore = daoFiles.size();
        daoFiles.add(new DaoFile(pathToFile(filesBefore, DATA_FILE), pathToFile(filesBefore, META_FILE)));
    }

    void close() throws IOException {
        for (DaoFile daoFile : daoFiles) {
            daoFile.close();
        }
    }

    void savaData(Iterator<BaseEntry<String>> dataIterator) throws IOException {
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToFile(daoFiles.size(), DATA_FILE), writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToFile(daoFiles.size(), META_FILE), writeOptions)
             ))) {
            EntryReadWriter entryWriter = getEntryReadWriter();
            BaseEntry<String> entry = dataIterator.next();
            int entriesCount = 1;
            int currentRepeats = 1;
            int currentBytes = entryWriter.writeEntryInStream(dataStream, entry);

            while (dataIterator.hasNext()) {
                entry = dataIterator.next();
                entriesCount++;
                int bytesWritten = entryWriter.writeEntryInStream(dataStream, entry);
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
            metaStream.writeInt(entriesCount);
        }
    }

    private int getEntryIndex(String key, DaoFile daoFile) throws IOException {
        EntryReadWriter entryReader = getEntryReadWriter();
        int left = 0;
        int right = daoFile.getLastIndex();
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            CharBuffer middleKey = entryReader.bufferAsKeyOnly(daoFile, middle);
            CharBuffer keyToFind = entryReader.fillAndGetKeyBuffer(key);
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

    private EntryReadWriter getEntryReadWriter() {
        return entryReadWriter.computeIfAbsent(Thread.currentThread(), thread -> new EntryReadWriter(bufferSize));
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
        private final EntryReadWriter entryReader;
        private final DaoFile daoFile;
        private final String to;
        private int entryToRead;
        private BaseEntry<String> next;

        public FileIterator(String from, String to, DaoFile daoFile) throws IOException {
            this.daoFile = daoFile;
            this.to = to;
            this.entryToRead = from == null ? 0 : getEntryIndex(from, daoFile);
            this.entryReader = getEntryReadWriter();
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
            BaseEntry<String> entry = entryReader.readEntryFromChannel(daoFile, entryToRead);
            if (to != null && entry.key().compareTo(to) >= 0) {
                return null;
            }
            entryToRead++;
            return entry;
        }
    }

}
