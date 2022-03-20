package ru.mail.polis.alinashestakova;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final SortedMap<MemorySegment, BaseEntry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final Path path;
    private int filesCount;

    public InMemoryDao(Config config) throws IOException {
        this.path = config.basePath();

        if (Files.exists(this.path)) {
            try (Stream<Path> files = Files.list(this.path)) {
                this.filesCount = (int) files.count();
            }
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
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        BaseEntry<MemorySegment> result = storage.get(key);
        return result == null ? getFromFile(key) : result;
    }

    public BaseEntry<MemorySegment> getFromFile(MemorySegment key) throws IOException {
        if (this.filesCount == 0) {
            return null;
        }

        for (int i = this.filesCount; i > 0; i--) {
            Path tmp = this.path.resolve(Paths.get("storage" + i + ".txt"));
            if (!Files.exists(tmp)) {
                continue;
            }

            try (InputStream input = Files.newInputStream(tmp)) {
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

                    if (new MemorySegmentComparator().compare(MemorySegment.ofArray(rkey), key) == 0) {
                        return new BaseEntry<>(key, MemorySegment.ofArray(rvalue));
                    }
                }
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
    public void flush() throws IOException {
        Path newFile = Files.createFile(path.resolve("storage" + (++this.filesCount) + ".txt"));

        try (OutputStream output = Files.newOutputStream(newFile);
             BufferedOutputStream buffer = new BufferedOutputStream(output, 102400)) {
            for (BaseEntry<MemorySegment> memorySegmentBaseEntry : storage.values()) {
                buffer.write((byte) memorySegmentBaseEntry.key().byteSize());
                buffer.write(memorySegmentBaseEntry.key().toByteArray());
                buffer.write((byte) memorySegmentBaseEntry.value().byteSize());
                buffer.write(memorySegmentBaseEntry.value().toByteArray());
            }
        }
    }

    public static class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            long offset = o1.mismatch(o2);
            if (offset == -1) {
                return 0;
            }
            if (offset == o1.byteSize()) {
                return -1;
            }
            if (offset == o2.byteSize()) {
                return 1;
            }
            return Byte.compareUnsigned(
                    MemoryAccess.getByteAtOffset(o1, offset),
                    MemoryAccess.getByteAtOffset(o2, offset)
            );
        }
    }
}
