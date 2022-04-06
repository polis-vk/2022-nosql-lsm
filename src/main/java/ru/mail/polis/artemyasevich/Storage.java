package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.nio.file.DirectoryStream;
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
    private static final String COMPACTED = "compacted_";
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final Map<Thread, EntryReadWriter> entryReadWriter;
    private final Path pathToDirectory;
    private final List<DaoFile> daoFiles;
    private final int bufferSize;

    Storage(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int filesCount = files == null ? 0 : files.length;
        boolean compactionResolved = resolveCompactionIfNeeded(filesCount);
        int daoFiles = compactionResolved ? 1 : filesCount / 2;
        this.daoFiles = new ArrayList<>(daoFiles);
        this.bufferSize = initFiles(daoFiles);
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
        Path compactedData = pathToFile(COMPACTED + DATA_FILE);
        Path compactedMeta = pathToFile(COMPACTED + META_FILE);
        savaData(mergeIterator, compactedData, compactedMeta);
        closeFiles();
        daoFiles.clear();
        retainOnlyCompactedFiles();
        Files.move(compactedData, pathToData(0), StandardCopyOption.ATOMIC_MOVE);
        Files.move(compactedMeta, pathToMeta(0), StandardCopyOption.ATOMIC_MOVE);
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

    private boolean resolveCompactionIfNeeded(int filesInDirectory) throws IOException {
        Path compactedData = pathToFile(COMPACTED + DATA_FILE);
        Path compactedMeta = pathToFile(COMPACTED + META_FILE);
        boolean incorrectDataFileExists = Files.exists(compactedData);
        boolean incorrectMetaFileExists = Files.exists(compactedMeta);
        if (!incorrectDataFileExists && !incorrectMetaFileExists) {
            return false;
        }
        if (filesInDirectory > 2) {
            retainOnlyCompactedFiles();
        }
        if (incorrectDataFileExists) {
            Files.move(compactedData, pathToData(0));
        }
        if (incorrectMetaFileExists) {
            Files.move(compactedMeta, pathToMeta(0));
        }
        return true;
    }

    private void retainOnlyCompactedFiles() throws IOException {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(pathToDirectory)) {
            for (Path path : directoryStream) {
                String fileName = path.getFileName().toString();
                if (fileName.startsWith(DATA_FILE) || fileName.startsWith(META_FILE)) {
                    Files.delete(path);
                }
            }
        }
    }

    private Path pathToMeta(int fileNumber) {
        return pathToFile(META_FILE + fileNumber);
    }

    private Path pathToData(int fileNumber) {
        return pathToFile(DATA_FILE + fileNumber);
    }

    private Path pathToFile(String fileName) {
        return pathToDirectory.resolve(fileName + FILE_EXTENSION);
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
