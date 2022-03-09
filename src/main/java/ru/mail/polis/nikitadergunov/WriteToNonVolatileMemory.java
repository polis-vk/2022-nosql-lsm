package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;

public class WriteToNonVolatileMemory {

    private final FileChannel writeChannel;

    public WriteToNonVolatileMemory(Config config) throws IOException {
        Path pathToTable = config.basePath().resolve("table");
        Files.deleteIfExists(pathToTable);
        writeChannel = FileChannel.open(pathToTable, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    public void write(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        for (Entry<MemorySegment> entry : storage.values()) {
            writeMemorySegment(entry.key());
            writeMemorySegment(entry.value());
        }
    }

    private void writeLong(long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        buffer.position(0);
        writeChannel.write(buffer);
    }

    private void writeMemorySegment(MemorySegment memorySegment) throws IOException {
        if (memorySegment == null) {
            writeLong(-1);
            return;
        }
        writeLong(memorySegment.byteSize());
        writeChannel.write(memorySegment.asByteBuffer());
    }

}
