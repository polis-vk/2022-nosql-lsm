package ru.mail.polis.alexanderkosnitskiy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final Config config;
    private ConcurrentNavigableMap<ByteBuffer, ByteBuffer> memory;
    private int currentFile = 0;
    private boolean readerMode = false;

    public PersistenceDao(Config config) {
        ConcurrentNavigableMap<ByteBuffer, ByteBuffer> temp = null;
        try {
            currentFile = new File(config.basePath().toString()).list().length;
        } catch (NullPointerException e) {
            currentFile = 0;
        }
        try (FileInputStream in = new FileInputStream(config.basePath().toString()
                + "/data" + currentFile + ".txt");
             ObjectInputStream reader = new ObjectInputStream(in)) {
            Map<ByteBuffer, ByteBuffer> map = ((Map<byte[], byte[]>) reader.readObject()).entrySet().stream()
                    .collect(Collectors.toMap(k -> ByteBuffer.wrap(k.getKey()), v -> ByteBuffer.wrap(v.getValue())));
            temp = new ConcurrentSkipListMap<>(map);
        } catch (FileNotFoundException | ClassNotFoundException e) {
            temp = new ConcurrentSkipListMap<>(Comparator.naturalOrder());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.config = config;
        memory = temp;
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        Iterator<BaseEntry<ByteBuffer>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return readFromStorage(key);
        }
        BaseEntry<ByteBuffer> next = iterator.next();
        if (next.key().equals(key)) {
            return next;
        }
        return readFromStorage(key);
    }

    private BaseEntry<ByteBuffer> readFromStorage(ByteBuffer key) {
        ByteBuffer res = null;
        try {
            res = findInFiles(key);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (res != null) {
            return new BaseEntry<>(key, res);
        }
        return null;
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return new DaoIterator(memory.entrySet().iterator());
        }
        if (from == null) {
            return new DaoIterator(memory.headMap(to, false).entrySet().iterator());
        }
        if (to == null) {
            return new DaoIterator(memory.tailMap(from, true).entrySet().iterator());
        }
        return new DaoIterator(memory.subMap(from, true, to, false).entrySet().iterator());
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (readerMode) {
            readerMode = false;
            memory.clear();
        }
        if (memory.size() >= 25000) {
            try {
                flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        memory.put(entry.key(), entry.value());
    }

    @Override
    public void flush() throws IOException {
        try (FileOutputStream out = new FileOutputStream(config.basePath().toString()
                + "/data" + currentFile + ".txt");
             ObjectOutputStream writer = new ObjectOutputStream(out)) {
            HashMap<byte[], byte[]> map = (HashMap<byte[], byte[]>) memory.entrySet().stream()
                    .collect(Collectors.toMap(k -> k.getKey().array(), v -> v.getValue().array()));
            writer.writeObject(map);
            memory.clear();
            currentFile++;
        }
    }

    private ByteBuffer findInFiles(ByteBuffer key) throws IOException {
        if (!readerMode) {
            readerMode = true;
            flush();
        }
        for (int i = currentFile - 1; i >= 0; i--) {
            try (FileInputStream in = new FileInputStream(config.basePath().toString()
                    + "/data" + i + ".txt");
                 ObjectInputStream reader = new ObjectInputStream(in)) {
                Map<ByteBuffer, ByteBuffer> map = ((Map<byte[], byte[]>) reader.readObject()).entrySet().stream()
                        .collect(Collectors.toMap(k -> ByteBuffer.wrap(k.getKey()), v -> ByteBuffer.wrap(v.getValue())));
                ByteBuffer value = map.get(key);
                if (value != null) {
                    memory = new ConcurrentSkipListMap<>(map);
                    return value;
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    static class DaoIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator;

        private DaoIterator(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            Map.Entry<ByteBuffer, ByteBuffer> temp = iterator.next();
            return new BaseEntry<>(temp.getKey(), temp.getValue());
        }
    }

}
