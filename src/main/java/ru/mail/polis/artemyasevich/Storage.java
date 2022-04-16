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
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Storage {
    private static final String DATA_FILE = "data";
    private static final String META_FILE = "meta";
    private static final String FILE_EXTENSION = ".txt";
    private static final String COMPACTED = "compacted_";
    private static final int DEFAULT_BUFFER_SIZE = 64;
    private static final OpenOption[] writeOptions = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};
    private final AtomicInteger daoFilesCount = new AtomicInteger();
    private final Map<Thread, EntryReadWriter> entryReadWriter;
    private final Path pathToDirectory;
    private final List<DaoFile> filesToClose;
    private final Deque<DaoFile> daoFiles;
    private int bufferSize;

    Storage(Config config) throws IOException {
        this.pathToDirectory = config.basePath();
        File[] files = pathToDirectory.toFile().listFiles();
        int filesCount = files == null ? 0 : files.length;
        boolean compactionResolved = resolveCompactionIfNeeded(filesCount);
        int daoFilesCount = compactionResolved ? 1 : filesCount / 2;
        this.daoFiles = new ConcurrentLinkedDeque<>();
        this.daoFilesCount.set(daoFilesCount);
        this.filesToClose = new ArrayList<>(daoFilesCount);
        int maxEntrySize = initFiles(daoFilesCount);
        this.bufferSize = maxEntrySize == 0 ? DEFAULT_BUFFER_SIZE : maxEntrySize;
        this.entryReadWriter = Collections.synchronizedMap(new WeakHashMap<>());
    }

    BaseEntry<String> get(String key) throws IOException {
        EntryReadWriter entryReader = getEntryReadWriter();
        if (key.length() > entryReader.maxKeyLength()) {
            return null;
        }
        for (DaoFile daoFile : daoFiles) {
            int entryIndex = getEntryIndex(key, daoFile);
            if (entryIndex > daoFile.getLastIndex()) {
                continue;
            }
            BaseEntry<String> entry = entryReader.readEntryFromChannel(daoFile, entryIndex);
            if (entry.key().equals(key)) {
                return entry.value() == null ? null : entry;
            }
            if (daoFile.isCompacted()) {
                break;
            }
        }
        return null;
    }

    Iterator<BaseEntry<String>> iterate(String from, String to) throws IOException {
        List<PeekIterator> peekIterators = new ArrayList<>(daoFiles.size());
        int i = 0;
        for (DaoFile daoFile : daoFiles) {
            int sourceNumber = i;
            peekIterators.add(new PeekIterator(new FileIterator(from, to, daoFile), sourceNumber));
            i++;
            if (daoFile.isCompacted()) {
                break;
            }
        }
        return new MergeIterator(peekIterators);
    }

    void compact() {
        if (daoFiles.peek() == null || daoFiles.peek().isCompacted()) {
            return;
        }
        int number = daoFilesCount.getAndIncrement();
        Path compactedData = pathToData(number);
        Path compactedMeta = pathToMeta(number);
        int sizeBefore = daoFiles.size();
        try {
            savaData(iterate(null, null), compactedData, compactedMeta);
            daoFiles.addFirst(new DaoFile(compactedData, compactedMeta, true, number));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (int i = 0; i < sizeBefore; i++) {
            DaoFile removed = daoFiles.removeLast();
            filesToClose.add(removed);
        }
        //Теперь все новые запросы get будут идти на новый компакт файл, старые когда-нибудь завершатся
    }

    void flush(Iterator<BaseEntry<String>> dataIterator) throws IOException {
        int number = daoFilesCount.getAndIncrement();
        Path pathToData = pathToData(number);
        Path pathToMeta = pathToMeta(number);
        int maxEntrySize = savaData(dataIterator, pathToData, pathToMeta);

        if (maxEntrySize > bufferSize) {
            entryReadWriter.forEach((key, value) -> value.increaseBufferSize(maxEntrySize));
            bufferSize = maxEntrySize;
        }
        daoFiles.addFirst(new DaoFile(pathToData, pathToMeta, false, number));
    }

    void close() throws IOException {
        boolean compactedPresent = false;
        Iterator<DaoFile> daoFileIterator = daoFiles.iterator();

        while (daoFileIterator.hasNext()) {
            DaoFile daoFile = daoFileIterator.next();
            if (compactedPresent) {
                Files.delete(daoFile.pathToFile());
                Files.delete(daoFile.pathToMeta());
                daoFileIterator.remove();
            }
            if (daoFile.isCompacted()) {
                compactedPresent = true;
            }
            daoFile.close();
        }

        for (DaoFile oldFile : filesToClose) {
            oldFile.close();
            Files.delete(oldFile.pathToMeta());
            Files.delete(oldFile.pathToFile());
        }
        filesToClose.clear();
        Iterator<DaoFile> iterator = daoFiles.descendingIterator();
        int i = 0;
        while(iterator.hasNext()){
            DaoFile daoFile = iterator.next();
            Files.move(daoFile.pathToFile(), pathToData(i), StandardCopyOption.ATOMIC_MOVE);
            Files.move(daoFile.pathToMeta(), pathToMeta(i), StandardCopyOption.ATOMIC_MOVE);
            i++;
        }
        daoFiles.clear();
    }

    private int savaData(Iterator<BaseEntry<String>> dataIterator,
                         Path pathToData, Path pathToMeta) throws IOException {
        int maxEntrySize = 0;
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
                if (bytesWritten > maxEntrySize) {
                    maxEntrySize = bytesWritten;
                }
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
        return maxEntrySize;
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
            DaoFile daoFile = new DaoFile(pathToData(i), pathToMeta(i), false, 0);
            if (daoFile.maxEntrySize() > maxSize) {
                maxSize = daoFile.maxEntrySize();
            }
            daoFiles.addFirst(daoFile);
        }
        return maxSize;
    }

    private boolean resolveCompactionIfNeeded(int filesInDirectory) throws IOException {
        //TODO implement
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
