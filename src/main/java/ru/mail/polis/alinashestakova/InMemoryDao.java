package ru.mail.polis.alinashestakova;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final SortedMap<MemorySegment, BaseEntry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private File[] files;

    public InMemoryDao(Config config) {
        Path path = config.basePath();
        this.files = path.toFile().listFiles();

        int count = path.toFile().listFiles() == null ? 0 : this.files.length;
        try {
            Files.createFile(path.resolve(Path.of("storage" + (++count) + ".txt")));
            this.files = path.toFile().listFiles();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (storage.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return storage.values().iterator();
        } else if (from == null) {
            return storage.headMap(to).values().iterator();
        } else if (to == null) {
            return storage.tailMap(from).values().iterator();
        }

        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        BaseEntry<MemorySegment> result = storage.get(key);
        return result == null ? getFromFile(key) : result;
    }

    public BaseEntry<MemorySegment> getFromFile(MemorySegment key) {
        if (this.files == null) {
            return null;
        }

        for (int i = this.files.length - 1; i >= 0; i--) {
            try (InputStream input = Files.newInputStream(this.files[i].toPath())) {
                while (true) {
                    int size = input.read();
                    if (size < 0) {
                        break;
                    }
                    byte[] rkey = new byte[size];
                    input.read(rkey);
                    size = input.read();
                    byte[] rvalue = new byte[size];
                    input.read(rvalue);

                    if (Arrays.equals(rkey, key.toByteArray())) {
                        return new BaseEntry<>(key, MemorySegment.ofArray(rvalue));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }

        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        try (OutputStream output = Files.newOutputStream(this.files[this.files.length - 1].toPath());
             BufferedOutputStream buffer = new BufferedOutputStream(output, 128)) {
            for (BaseEntry<MemorySegment> memorySegmentBaseEntry : storage.values()) {
                buffer.write((int) memorySegmentBaseEntry.key().byteSize());
                buffer.write(memorySegmentBaseEntry.key().toByteArray());
                buffer.write((int) memorySegmentBaseEntry.value().byteSize());
                buffer.write(memorySegmentBaseEntry.value().toByteArray());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            long offset = o1.mismatch(o2);
            if (offset == -1) {
                return 0;
            }

            Byte b = MemoryAccess.getByteAtOffset(o1, offset);
            return b.compareTo(MemoryAccess.getByteAtOffset(o2, offset));
        }
    }
}
