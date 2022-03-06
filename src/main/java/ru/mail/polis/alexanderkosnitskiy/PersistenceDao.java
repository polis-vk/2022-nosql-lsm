package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final String EXTENSION = ".seg";

    private final String file = File.separator + "data";
    private final Config config;
    private ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> memory;
    private int currentFile;
    private boolean readerMode;

    public PersistenceDao(Config config) throws IOException {
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> temp = null;
        String[] str = new File(config.basePath().toString()).list();
        if (str == null) {
            currentFile = 0;
        } else {
            currentFile = str.length;
        }
        try (DaoReader in = new DaoReader(config.basePath() + file + currentFile + ".txt")) {
            temp = in.readMap();
        } catch (FileNotFoundException e) {
            temp = new ConcurrentSkipListMap<>(Comparator.naturalOrder());
        }
        readerMode = false;
        this.config = config;
        memory = temp;
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        Iterator<BaseEntry<ByteBuffer>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return findInFiles(key);
        }
        BaseEntry<ByteBuffer> next = iterator.next();
        if (next.key().equals(key)) {
            return next;
        }
        return findInFiles(key);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return memory.values().iterator();
        }
        if (from == null) {
            return memory.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return memory.tailMap(from, true).values().iterator();
        }
        return memory.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (readerMode) {
            readerMode = false;
            memory.clear();
        }
        if (memory.size() >= 50000) {
            try {
                flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        memory.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        try (DaoWriter out = new DaoWriter(config.basePath().toString() + file + currentFile + ".txt")) {
            out.writeMap(memory);
            memory.clear();
            currentFile++;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private BaseEntry<ByteBuffer> findInFiles(ByteBuffer key) throws IOException {
        if (!readerMode) {
            readerMode = true;
            flush();
        }
        for (int i = currentFile - 1; i >= 0; i--) {
            try (DaoReader in = new DaoReader(config.basePath().toString() + file + i + ".txt")) {
                Map<ByteBuffer, BaseEntry<ByteBuffer>> map = in.readMap();
                BaseEntry<ByteBuffer> value = map.get(key);
                if (value != null) {
                    memory = new ConcurrentSkipListMap<>(map);
                    return value;
                }
            } catch (FileNotFoundException e) {
                return null;
            }
        }
        return null;
    }

}
