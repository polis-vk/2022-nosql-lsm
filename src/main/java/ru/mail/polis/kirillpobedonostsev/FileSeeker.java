package ru.mail.polis.kirillpobedonostsev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;

public class FileSeeker {
    private final Path dataFilename;
    private final Path indexFilename;

    public FileSeeker(Path filename, Path indexFilename) {
        this.dataFilename = filename;
        this.indexFilename = indexFilename;
    }

    public BaseEntry<MemorySegment> get(MemorySegment key, Long offset) {
        if (offset == null) {
            return null;
        }
        MemorySegment value;
        try (RandomAccessFile file = new RandomAccessFile(dataFilename.toFile(), "r")) {
            file.seek(offset);
            long keyLength = file.readLong();
            file.seek(offset + keyLength + Long.BYTES);
            long valueLength = file.readLong();
            value = MemorySegment.mapFile(dataFilename, offset + keyLength + Long.BYTES * 2, valueLength,
                    FileChannel.MapMode.READ_ONLY, ResourceScope.newSharedScope());
            return new BaseEntry<>(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void fill(ConcurrentMap<MemorySegment, Long> map) {
        long readBytes = 0;
        try (RandomAccessFile file = new RandomAccessFile(indexFilename.toFile(), "r")) {
            for (long length = file.length(); length > 0; length -= readBytes) {
                long keyLength = file.readLong();
                MemorySegment key = MemorySegment.mapFile(indexFilename, readBytes + Long.BYTES, keyLength,
                        FileChannel.MapMode.READ_ONLY, ResourceScope.newSharedScope());
                file.seek(readBytes + keyLength + Long.BYTES);
                long value = file.readLong();
                readBytes = readBytes + Long.BYTES * 2 + keyLength;
                map.put(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
