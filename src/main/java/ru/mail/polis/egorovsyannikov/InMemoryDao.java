package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMap =
            new ConcurrentSkipListMap<>(String::compareTo);

    private static final String FILE_NAME = "cache";
    private static final int NUMBER_OF_BYTES_IN_KEY_VALUE_PAIR_IN_FILE = 4;
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
        try (DataOutputStream writer
                     = new DataOutputStream(Files.newOutputStream(directoryPath.resolve(listOfFiles.size() + FILE_NAME)))) {
            writer.writeInt(stringConcurrentSkipListMap.size());
            ArrayList<Integer> offsets = new ArrayList<>();
            int keyValueSize = writer.size();
            for (BaseEntry<String> entry : stringConcurrentSkipListMap.values()) {
                keyValueSize += entry.key().length() + entry.value().length() + NUMBER_OF_BYTES_IN_KEY_VALUE_PAIR_IN_FILE;
            }
            writer.writeInt(keyValueSize + Integer.BYTES);
            for (BaseEntry<String> entry : stringConcurrentSkipListMap.values()) {
                offsets.add(writer.size());
                writer.writeUTF(entry.key());
                writer.writeUTF(entry.value());
            }
            for (int offset : offsets) {
                writer.writeInt(offset);
            }
        }
    }
}
