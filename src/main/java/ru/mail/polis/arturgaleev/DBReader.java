package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class DBReader implements AutoCloseable {
    private static final String DB_FILES_EXTENSION = ".txt";
    private final List<FileDBReader> fileReaders;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public DBReader(Path dbDirectoryPath) throws IOException {
        fileReaders = getFileDBReaders(dbDirectoryPath);
    }

    private static List<FileDBReader> getFileDBReaders(Path dbDirectoryPath) throws IOException {
        List<FileDBReader> fileDBReaderList = new ArrayList<>();
        try (Stream<Path> files = Files.list(dbDirectoryPath)
                .filter(path -> path.getFileName().toString().endsWith(DB_FILES_EXTENSION))
                .sorted(Comparator.comparingLong(path -> {
                    String fileName = path.getFileName().toString();
                    return Long.parseLong(fileName.substring(0, fileName.length() - 4));
                }))
        ) {
            for (Path path : files.toList()) {
                FileDBReader fileDBReader = new FileDBReader(path);
                if (fileDBReader.checkIfFileCorrupted()) {
                    throw new FileSystemException("File with path: " + path + " is corrupted");
                }
                fileDBReaderList.add(fileDBReader);
            }
        }
        fileDBReaderList.sort(Comparator.comparing(FileDBReader::getFileID));
        return fileDBReaderList;
    }

    public boolean hasNoReaders() {
        lock.readLock().lock();
        try {
            return fileReaders.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getBiggestFileId() {
        lock.readLock().lock();
        try {
            if (fileReaders.isEmpty()) {
                return -1;
            }
            return fileReaders.get(fileReaders.size() - 1).getFileID();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (fileReaders.isEmpty()) {
                return Collections.emptyIterator();
            }
            List<PriorityPeekingIterator<Entry<MemorySegment>>> iterators = new ArrayList<>(fileReaders.size());
            for (FileDBReader reader : fileReaders) {
                FileDBReader.FileIterator fromToIterator = reader.getFromToIterator(from, to);
                if (fromToIterator.hasNext()) {
                    iterators.add(new PriorityPeekingIterator<>(fromToIterator.getFileId(), fromToIterator));
                }
            }
            return new MergeIterator<>(iterators, MemorySegmentComparator.INSTANCE);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        lock.readLock().lock();
        try {
            for (int i = fileReaders.size() - 1; i >= 0; i--) {
                Entry<MemorySegment> entryByKey = fileReaders.get(i).getEntryByKey(key);
                if (entryByKey != null) {
                    if (entryByKey.value() == null) {
                        return null;
                    } else {
                        return entryByKey;
                    }
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clearAndSet(FileDBReader newReader) throws IOException {
        lock.writeLock().lock();
        try {
            add(newReader);
            for (int i = 0; i < fileReaders.size() - 1; i++) {
                fileReaders.get(i).deleteFile();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void add(FileDBReader newReader) {
        lock.writeLock().lock();
        try {
            fileReaders.add(newReader);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        for (FileDBReader fileReader : fileReaders) {
            fileReader.close();
        }
    }
}
