package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMap =
            new ConcurrentSkipListMap<>(String::compareTo);

    private static final String FILE_NAME = "cache";
    private static final int FILLER_FOR_OFFSETS_OFFSET = 0;
    private final Deque<Path> listOfFiles = new ArrayDeque<>();
    private final Path directoryPath;

    public InMemoryDao(Config config) throws IOException {
        directoryPath = config.basePath();
        String[] arrayOfFiles = config.basePath().toFile().list();
        if (arrayOfFiles != null) {
            for (int i = arrayOfFiles.length - 1; i >= 0; i--) {
                listOfFiles.add(config.basePath().resolve(arrayOfFiles[i]));
            }
        }
    }

    @Override
    public BaseEntry<String> get(String key) {
        BaseEntry<String> resultFromMap = stringConcurrentSkipListMap.get(key);

        if (resultFromMap != null) {
            return resultFromMap;
        }

        for (Path file : listOfFiles) {
            BaseEntry<String> tmp = new FilePeekIterator(file, null, null).findValueByKey(key);
            if (tmp != null) {
                return tmp;
            }
        }

        return null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        FilePeekIterator stringConcurrentSkipListMapIterator;
        Deque<FilePeekIterator> listOfIterators = new ArrayDeque<>();
        if (from == null && to == null) {
            stringConcurrentSkipListMapIterator = new FilePeekIterator(getIterator(stringConcurrentSkipListMap));
        } else if (from == null) {
            stringConcurrentSkipListMapIterator
                    = new FilePeekIterator(getIterator(stringConcurrentSkipListMap.headMap(to)));
        } else if (to == null) {
            stringConcurrentSkipListMapIterator
                    = new FilePeekIterator(getIterator(stringConcurrentSkipListMap.tailMap(from, true)));
        } else {
            stringConcurrentSkipListMapIterator
                    = new FilePeekIterator(getIterator(stringConcurrentSkipListMap.subMap(from, to)));
        }
        if (stringConcurrentSkipListMapIterator.hasNext()) {
            listOfIterators.add(stringConcurrentSkipListMapIterator);
        }

        for (Path file : listOfFiles) {
            FilePeekIterator tmp = new FilePeekIterator(file, from, to);
            if (tmp.hasNext()) {
                listOfIterators.add(tmp);
            }
        }

        return (listOfIterators.isEmpty()) ? Collections.emptyIterator() : new MergeIterator(listOfIterators);
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
        try (RandomAccessFile writer
                     = new RandomAccessFile(directoryPath.resolve(listOfFiles.size() + FILE_NAME).toString(), "rw")) {
            writer.writeInt(stringConcurrentSkipListMap.size());
            ArrayList<Long> offsets = new ArrayList<>();
            writer.writeLong(FILLER_FOR_OFFSETS_OFFSET);
            for (BaseEntry<String> entry : stringConcurrentSkipListMap.values()) {
                offsets.add(writer.getFilePointer());
                writer.writeUTF(entry.key());
                writer.writeUTF(entry.value());
            }
            long keyValueSize = writer.getFilePointer();
            writer.seek(Integer.BYTES);
            writer.writeLong(keyValueSize);
            writer.seek(keyValueSize);
            for (long offset : offsets) {
                writer.writeLong(offset);
            }
        }
    }
}
