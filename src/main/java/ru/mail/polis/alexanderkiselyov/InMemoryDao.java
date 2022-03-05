package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final NavigableMap<byte[], BaseEntry<byte[]>> pairs;
    private final Config config;
    private final int bufferSize = 20 * Character.BYTES;
    private static final String FILE_NAME = "myData.txt";

    public InMemoryDao(Config config) {
        this.config = config;
        pairs = new ConcurrentSkipListMap<>(Arrays::compare);
        try (FileInputStream fis = new FileInputStream(String.valueOf(config.basePath().resolve(FILE_NAME)));
             BufferedInputStream reader = new BufferedInputStream(fis, bufferSize)) {
            while (reader.available() != 0) {
                int keyLength = reader.read();
                byte[] key = reader.readNBytes(keyLength);
                int valueLength = reader.read();
                byte[] value = reader.readNBytes(valueLength);
                pairs.put(key, new BaseEntry<>(key, value));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return pairs.values().iterator();
        } else if (from == null) {
            return pairs.headMap(to).values().iterator();
        } else if (to == null) {
            return pairs.tailMap(from).values().iterator();
        }
        return pairs.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) {
        Iterator<BaseEntry<byte[]>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        BaseEntry<byte[]> next = iterator.next();
        if (Arrays.equals(next.key(), key)) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        pairs.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(config.basePath().resolve(FILE_NAME)));
             BufferedOutputStream writer = new BufferedOutputStream(fos, bufferSize)) {
            for (var pair : pairs.entrySet()) {
                writer.write(pair.getKey().length);
                writer.write(pair.getKey());
                writer.write(pair.getValue().value().length);
                writer.write(pair.getValue().value());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
