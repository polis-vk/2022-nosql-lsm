package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final ConcurrentNavigableMap<ByteBuffer, ByteBuffer> storage;
    private final Config config;

    public PersistenceDao(Config config) {
        ConcurrentNavigableMap<ByteBuffer, ByteBuffer> temp = null;
        try(FileInputStream in = new FileInputStream(config.basePath().toString() + "/data.txt");
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
        storage = temp;
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return new DaoIterator(storage.entrySet().iterator());
        }
        if (from == null) {
            return new DaoIterator(storage.headMap(to, false).entrySet().iterator());
        }
        if (to == null) {
            return new DaoIterator(storage.tailMap(from, true).entrySet().iterator());
        }
        return new DaoIterator(storage.subMap(from, true, to, false).entrySet().iterator());
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        storage.put(entry.key(), entry.value());
    }

    @Override
    public void flush() throws IOException {
        try(FileOutputStream out = new FileOutputStream(config.basePath().toString() + "/data.txt");
            ObjectOutputStream writer = new ObjectOutputStream(out)) {
            Map<byte[], byte[]> map = storage.entrySet().stream()
                    .collect(Collectors.toMap(k -> k.getKey().array(), v -> v.getValue().array()));
            writer.writeObject(map);
        }
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
