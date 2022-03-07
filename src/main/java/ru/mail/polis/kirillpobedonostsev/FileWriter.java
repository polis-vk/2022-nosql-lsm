package ru.mail.polis.kirillpobedonostsev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileWriter {
    private final Path dataFilename;
    private final Path indexFilename;

    public FileWriter(Path filename, Path indexFilename) {
        this.dataFilename = filename;
        this.indexFilename = indexFilename;
    }

    public void write(ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> map,
                      ConcurrentMap<MemorySegment, Long> index) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(dataFilename.toFile(), "rw")) {
            long readBytes = 0;
            if (file.length() != 0) {
                file.seek(file.length() - 1);
                readBytes = file.length() - 1;
            }
            for (BaseEntry<MemorySegment> entry : map.values()) {
                write(file, entry);
                index.put(entry.key(), readBytes);
                readBytes = readBytes + Long.BYTES * 2 + entry.key().byteSize() + entry.value().byteSize();
            }
        }
    }

    private void write(RandomAccessFile file, BaseEntry<MemorySegment> entry) throws IOException {
        file.writeLong(entry.key().byteSize());
        file.write(entry.key().toByteArray());
        file.writeLong(entry.value().byteSize());
        file.write(entry.value().toByteArray());
    }

    public void write(ConcurrentMap<MemorySegment, Long> map) {
        try (RandomAccessFile file = new RandomAccessFile(indexFilename.toFile(), "rw")) {
            for (Map.Entry<MemorySegment, Long> entry : map.entrySet()) {
                file.writeLong(entry.getKey().byteSize());
                file.write(entry.getKey().toByteArray());
                file.writeLong(entry.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
