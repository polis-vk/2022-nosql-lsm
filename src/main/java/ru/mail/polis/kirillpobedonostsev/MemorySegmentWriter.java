package ru.mail.polis.kirillpobedonostsev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class MemorySegmentWriter {
    private final Path dataFilename;
    private final Path indexFilename;

    public MemorySegmentWriter(Path filename, Path indexFilename) {
        this.dataFilename = filename;
        this.indexFilename = indexFilename;
    }

    public void write(BaseEntry<MemorySegment> entry) throws IOException {
        Files.write(dataFilename, ByteBuffer.allocate(Long.BYTES).putLong(entry.key().byteSize()).array(), StandardOpenOption.APPEND);
        Files.write(dataFilename, entry.key().toByteArray(), StandardOpenOption.APPEND);
        Files.write(dataFilename, ByteBuffer.allocate(Long.BYTES).putLong(entry.value().byteSize()).array(), StandardOpenOption.APPEND);
        Files.write(dataFilename, entry.value().toByteArray(), StandardOpenOption.APPEND);
    }

    public void write(ConcurrentMap<MemorySegment, Long> map) {
        try (RandomAccessFile file = new RandomAccessFile(indexFilename.toFile(), "rw")) {
            for (Map.Entry<MemorySegment, Long> entry : map.entrySet()) {
                file.writeLong(entry.getKey().byteSize());
                file.write(entry.getKey().toByteArray());
                file.writeLong(entry.getValue());
            }
        } catch (FileNotFoundException ignored) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
