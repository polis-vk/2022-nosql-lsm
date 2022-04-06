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
import java.util.stream.Stream;

public class DBReader implements AutoCloseable {
    private static final String DB_FILES_EXTENSION = ".txt";
    private final Path dbDirectoryPath;
    private List<FileDBReader> fileReaders;

    private static List<FileDBReader> getFileDBReaders(Path dbDirectoryPath) throws IOException {
        List<FileDBReader> fileDBReaderList = new ArrayList<>();
        try (Stream<Path> files = Files.list(dbDirectoryPath)) {
            List<Path> paths = files
                    .filter(path -> path.toString().endsWith(DB_FILES_EXTENSION))
                    .toList();
            for (Path path : paths) {
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

    public DBReader(Path dbDirectoryPath) throws IOException {
        this.dbDirectoryPath = dbDirectoryPath;
        fileReaders = getFileDBReaders(dbDirectoryPath);
    }

    public void updateReadersList() throws IOException {
        fileReaders = getFileDBReaders(dbDirectoryPath);
    }

    public long getReadersCount() {
        return fileReaders.size();
    }

    public boolean hasReaders() {
        return fileReaders.isEmpty();
    }

    public long getBiggestFileId() {
        if (fileReaders.isEmpty()) {
            return -1;
        }
        long max = fileReaders.get(0).getFileID();
        for (FileDBReader reader : fileReaders) {
            if (reader.getFileID() > max) {
                max = reader.getFileID();
            }
        }
        return max;
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
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
    }

    public Entry<MemorySegment> get(MemorySegment key) {
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
    }

    @Override
    public void close() throws IOException {
        for (FileDBReader fileReader : fileReaders) {
            fileReader.close();
        }
    }
}
