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
        writeChannel = FileChannel.open(pathToTable, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    public void write(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        ByteBuffer allocateBuffer = ByteBuffer.allocate(Long.BYTES);
        for (Entry<MemorySegment> entry : storage.values()) {
            writeMemorySegment(entry.key(), allocateBuffer);
            writeMemorySegment(entry.value(), allocateBuffer);
        }
    }

    private void writeLong(long value, ByteBuffer allocateBuffer) throws IOException {
        allocateBuffer.putLong(0, value);
        allocateBuffer.position(0);
        writeChannel.write(allocateBuffer);
    }

    private void writeMemorySegment(MemorySegment memorySegment, ByteBuffer allocateBuffer) throws IOException {
        if (memorySegment == null) {
            writeLong(-1, allocateBuffer);
            return;
        }
        writeLong(memorySegment.byteSize(), allocateBuffer);
        writeChannel.write(memorySegment.asByteBuffer());
    }

}
