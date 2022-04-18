package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMap =
            new ConcurrentSkipListMap<>(String::compareTo);

    private static final String FILE_NAME = "cache";
    private static final String FILE_NAME_COPY = "cacheCopy";
    private static final int YOUNGEST_GENERATION = 0;
    private static final int FILLER_FOR_OFFSETS_OFFSET = 0;
    private final Deque<Path> listOfFiles = new ArrayDeque<>();
    private final Path directoryPath;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public InMemoryDao(Config config) throws IOException {
        directoryPath = config.basePath();
        File[] arrayOfFiles = config.basePath().toFile().listFiles();
        if(arrayOfFiles != null) {
            Arrays.sort(arrayOfFiles, Comparator.comparingLong(File::lastModified));
            for (File file : arrayOfFiles) {
                listOfFiles.add(file.toPath());
            }
        }
    }

    @Override
    public BaseEntry<String> get(String key) {
        BaseEntry<String> resultFromMap = stringConcurrentSkipListMap.get(key);

        if (resultFromMap != null) {
            return resultFromMap.value() == null ? null : resultFromMap;
        }

        int generation = 1;
        Iterator<Path> listOfFilesReversed = listOfFiles.descendingIterator();

        while (listOfFilesReversed.hasNext()) {
            BaseEntry<String> tmp =
                    new FilePeekIterator(listOfFilesReversed.next(), null, null, generation++)
                            .findValueByKey(key);
            if (tmp != null) {
                return tmp.value() == null ? null : tmp;
            }
        }
        return null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        FilePeekIterator stringConcurrentSkipListMapIterator;
        Deque<FilePeekIterator> listOfIterators = new ArrayDeque<>();
        if (from == null && to == null) {
            stringConcurrentSkipListMapIterator
                    = new FilePeekIterator(getIterator(stringConcurrentSkipListMap), YOUNGEST_GENERATION);
        } else if (from == null) {
            stringConcurrentSkipListMapIterator
                    = new FilePeekIterator(getIterator(stringConcurrentSkipListMap.headMap(to)), YOUNGEST_GENERATION);
        } else if (to == null) {
            stringConcurrentSkipListMapIterator
                    = new FilePeekIterator(
                    getIterator(stringConcurrentSkipListMap.tailMap(from, true)), YOUNGEST_GENERATION);
        } else {
            stringConcurrentSkipListMapIterator
                    = new FilePeekIterator(getIterator(stringConcurrentSkipListMap.subMap(from, to)), YOUNGEST_GENERATION);
        }

        if (stringConcurrentSkipListMapIterator.hasNext()) {
            listOfIterators.add(stringConcurrentSkipListMapIterator);
        }

        int generation = 1;
        Iterator<Path> listOfFilesReversed = listOfFiles.descendingIterator();
        while (listOfFilesReversed.hasNext()) {
            FilePeekIterator tmp = new FilePeekIterator(listOfFilesReversed.next(), from, to, generation++);
            if (tmp.hasNext()) {
                listOfIterators.add(tmp);
            }
        }

        return listOfIterators.isEmpty() ? Collections.emptyIterator() : new MergeIterator(listOfIterators);
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        stringConcurrentSkipListMap.put(entry.key(), entry);
    }

    private static Iterator<BaseEntry<String>> getIterator(ConcurrentNavigableMap<String, BaseEntry<String>> map) {
        return map.values().iterator();
    }

    @Override
    public void flush() throws IOException {
        try (DataOutputStream writer
                     = new DataOutputStream(
                new BufferedOutputStream(
                        Files.newOutputStream(directoryPath.resolve(listOfFiles.size() + FILE_NAME))));
             DataOutputStream writerCopy
                     = new DataOutputStream(
                     new BufferedOutputStream(
                             Files.newOutputStream(directoryPath.resolve(listOfFiles.size() + FILE_NAME_COPY))))) {
            writer.writeInt(stringConcurrentSkipListMap.size());
            writerCopy.writeInt(stringConcurrentSkipListMap.size());
            ArrayList<Integer> offsets = new ArrayList<>();
            writerCopy.writeLong(FILLER_FOR_OFFSETS_OFFSET);
            for (BaseEntry<String> entry : stringConcurrentSkipListMap.values()) {
                offsets.add(writerCopy.size());
                if(entry.value() != null) {
                    writerCopy.writeBoolean(true);
                    writerCopy.writeUTF(entry.key());
                    writerCopy.writeUTF(entry.value());
                } else {
                    writerCopy.writeBoolean(false);
                    writerCopy.writeUTF(entry.key());
                }
            }
            long keyValueSize = writerCopy.size();
            writer.writeLong(keyValueSize);
            for (BaseEntry<String> entry : stringConcurrentSkipListMap.values()) {
                if (entry.value() != null) {
                    writer.writeBoolean(true);
                    writer.writeUTF(entry.key());
                    writer.writeUTF(entry.value());
                } else {
                    writer.writeBoolean(false);
                    writer.writeUTF(entry.key());
                }
            }
            for (long offset : offsets) {
                writer.writeLong(offset);
            }

            Files.delete(directoryPath.resolve(listOfFiles.size() + FILE_NAME_COPY));
        } finally {
            stringConcurrentSkipListMap.clear();
        }
    }
}
