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
import java.nio.file.StandardCopyOption;
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
    private static final int COMPACTED_INDEX = -1;
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final Map<Thread, EntryReadWriter> entryReadWriter;
    private final Path pathToDirectory;
    private final List<DaoFile> daoFiles;
    private final int bufferSize;

    Storage(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int daoFilesCount = files == null ? 0 : files.length / 2;
        resolveCompactionIfNeeded(daoFilesCount);
        this.daoFiles = new ArrayList<>(daoFilesCount);
        this.bufferSize = initFiles(daoFilesCount);
        this.entryReadWriter = Collections.synchronizedMap(new WeakHashMap<>());
    }

    BaseEntry<String> get(String key) throws IOException {
        EntryReadWriter entryReader = getEntryReadWriter();
        if (key.length() > entryReader.maxKeyLength()) {
            return null;
        }
        for (int i = daoFiles.size() - 1; i >= 0; i--) {
            DaoFile daoFile = daoFiles.get(i);
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
        for (int i = 0; i < daoFiles.size(); i++) {
            int sourceNumber = daoFiles.size() - i;
            iterators.add(new PeekIterator(new FileIterator(from, to, daoFiles.get(i)), sourceNumber));
        }
        return new MergeIterator(iterators);
    }

    void compact(Iterator<BaseEntry<String>> mergeIterator) throws IOException {
        savaData(mergeIterator, pathToData(COMPACTED_INDEX), pathToMeta(COMPACTED_INDEX));
        closeFiles();
        int filesBefore = daoFiles.size();
        daoFiles.clear();
        retainOnlyCompactedFile(filesBefore);
        Files.move(pathToData(COMPACTED_INDEX), pathToData(0), StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToMeta(COMPACTED_INDEX), pathToMeta(0), StandardCopyOption.ATOMIC_MOVE);
    }

    void flush(Iterator<BaseEntry<String>> dataIterator) throws IOException {
        Path pathToData = pathToData(daoFiles.size());
        Path pathToMeta = pathToMeta(daoFiles.size());
        savaData(dataIterator, pathToData, pathToMeta);
        daoFiles.add(new DaoFile(pathToData, pathToMeta));
    }

    void closeFiles() throws IOException {
        for (DaoFile daoFile : daoFiles) {
            daoFile.close();
        }
    }

    private void savaData(Iterator<BaseEntry<String>> dataIterator,
                          Path pathToData, Path pathToMeta) throws IOException {
        try (DataOutputStream dataStream = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(pathToData, writeOptions)));
             DataOutputStream metaStream = new DataOutputStream(new BufferedOutputStream(
                     Files.newOutputStream(pathToMeta, writeOptions)
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

    private int initFiles(int daoFilesCount) throws IOException {
        int maxSize = 0;
        for (int i = 0; i < daoFilesCount; i++) {
            DaoFile daoFile = new DaoFile(pathToData(i), pathToMeta(i));
            if (daoFile.maxEntrySize() > maxSize) {
                maxSize = daoFile.maxEntrySize();
            }
            daoFiles.add(daoFile);
        }
        return maxSize;
    }

    private void resolveCompactionIfNeeded(int daoFilesCount) throws IOException {
        boolean incorrectDataFileExists = Files.exists(pathToData(COMPACTED_INDEX));
        boolean incorrectMetaFileExists = Files.exists(pathToMeta(COMPACTED_INDEX));
        if (!incorrectDataFileExists && !incorrectMetaFileExists) {
            return;
        }
        retainOnlyCompactedFile(daoFilesCount - 1);
        if (incorrectDataFileExists) {
            Files.move(pathToData(COMPACTED_INDEX), pathToData(0));
        }
        if (incorrectMetaFileExists) {
            Files.move(pathToMeta(COMPACTED_INDEX), pathToMeta(0));
        }
    }

    private void retainOnlyCompactedFile(int daoFilesCount) throws IOException {
        for (int i = 0; i < daoFilesCount; i++) {
            Files.delete(pathToData(i));
            Files.delete(pathToMeta(i));
        }
    }

    private Path pathToMeta(int fileNumber) {
        return pathToDirectory.resolve(META_FILE + fileNumber + FILE_EXTENSION);
    }

    private Path pathToData(int fileNumber) {
        return pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
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
