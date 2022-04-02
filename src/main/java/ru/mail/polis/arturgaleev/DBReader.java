package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class DBReader implements AutoCloseable {
    private static final String DB_FILES_EXTENSION = ".txt";
    private final List<FileDBReader> fileReaders;

    public DBReader(Path dbDirectoryPath) throws IOException {
        if (!Files.isDirectory(dbDirectoryPath)) {
            throw new NotDirectoryException(dbDirectoryPath + " is not a directory with DB files");
        }
        fileReaders = getFileDBReaders(dbDirectoryPath);
    }

    public int getNumberOfFiles() {
        return fileReaders.size();
    }

    private List<FileDBReader> getFileDBReaders(Path dbDirectoryPath) throws IOException {
        List<FileDBReader> fileDBReaderList = new ArrayList<>();
        try (Stream<Path> files = Files.list(dbDirectoryPath)) {
            List<Path> paths = files
                    .filter(path -> path.toString().endsWith(DB_FILES_EXTENSION))
                    .toList();
            for (Path path : paths) {
                fileDBReaderList.add(new FileDBReader(path));
            }
        }
        fileDBReaderList.sort(Comparator.comparing(FileDBReader::getFileID));
        return fileDBReaderList;
    }

    public MergeIterator get(MemorySegment from, MemorySegment to) {
        List<PriorityPeekingIterator<Entry<MemorySegment>>> iterators = new ArrayList<>(fileReaders.size());
        for (FileDBReader reader : fileReaders) {
            FileDBReader.FileIterator fromToIterator = reader.getFromToIterator(from, to);
            if (fromToIterator.hasNext()) {
                iterators.add(new PriorityPeekingIterator<>(fromToIterator.getFileId(), fromToIterator));
            }
        }
        return new MergeIterator(iterators);
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
