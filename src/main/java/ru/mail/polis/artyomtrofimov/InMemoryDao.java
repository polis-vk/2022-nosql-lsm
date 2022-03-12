package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, Entry<String>> {
    private static final String FILENAME = "db.dat";
    private static final String INDEX_FILE_NAME = "index.ind";
    private final ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private final Path dataPath;
    private final Path indexPath;
    private volatile boolean commit;


    public InMemoryDao(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("Config shouldn't be null");
        }
        dataPath = config.basePath().resolve(FILENAME);
        indexPath = config.basePath().resolve(INDEX_FILE_NAME);
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) throws IOException {
        boolean isFromNull = from == null;
        boolean isToNull = to == null;
        if (isFromNull && isToNull) {
            return data.values().iterator();
        }
        if (isFromNull) {
            return data.headMap(to).values().iterator();
        }
        if (isToNull) {
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<String> get(String key) throws IOException {
        Entry<String> entry = data.get(key);
        if (entry == null) {
            entry = findInFileByKey(key);
            if (entry != null) {
                data.put(entry.key(), entry);
            }
            return entry;
        }
        return entry;
    }

    private Entry<String> findInFileByKey(String key) throws IOException {
        try (RandomAccessFile input = new RandomAccessFile(dataPath.toString(), "r");
             RandomAccessFile indexInput = new RandomAccessFile(indexPath.toString(), "r")) {
            input.seek(0);
            String line;
            int size = input.readInt();
            long left = 0;
            long right = size;
            long mid;
            while (left < right) {
                mid = left + (right - left) / 2;
                indexInput.seek(mid * Long.BYTES);
                input.seek(indexInput.readLong());
                line = input.readUTF();
                int delimiterIndex = line.indexOf(' ');
                if (delimiterIndex == -1) {
                    continue;
                }
                int keyLength = Integer.parseInt(line, 0, delimiterIndex, 10);
                int entryDelimiter = delimiterIndex + keyLength + 1;
                String currentKey = line.substring(delimiterIndex + 1, entryDelimiter);
                int keyComparing = key.compareTo(currentKey);
                if (keyComparing == 0) {
                    return new BaseEntry<>(currentKey, line.substring(entryDelimiter));
                } else if (keyComparing < 0) {
                    right = mid;
                } else {
                    left = mid;
                }
            }
            return null;
        } catch (FileNotFoundException | EOFException e) {
            return null;
        }
    }

    @Override
    public void upsert(Entry<String> entry) {
        data.put(entry.key(), entry);
        commit = false;
    }

    @Override
    public void flush() throws IOException {
        if (commit) {
            return;
        }
        try (RandomAccessFile output = new RandomAccessFile(dataPath.toString(), "rw");
             RandomAccessFile indexOut = new RandomAccessFile(indexPath.toString(), "rw")) {
            output.seek(0);
            StringBuilder result = new StringBuilder();
            output.writeInt(data.size());
            for (Entry<String> value : data.values()) {
                result.append(value.key().length()).append(' ').append(value.key()).append(value.value());
                indexOut.writeLong(output.getFilePointer());
                output.writeUTF(result.toString());
                result.setLength(0);
            }
        }
        commit = true;
    }

    private static class EntryIterator implements Iterator<Entry<String>> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<String> next() {
            return null;
        }
    }
}
