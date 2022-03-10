package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;

public class WriteToNonVolatileMemory implements AutoCloseable {

    private MemorySegment writeMemorySegment;
    private MemorySegment indexesMemorySegment;
    private final ResourceScope scope = ResourceScope.newConfinedScope();
    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    public WriteToNonVolatileMemory(Config config,
                                    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage,
                                    long sizeInBytes) throws IOException {
        Path pathToTable = config.basePath().resolve("table");
        Path pathToIndexes = config.basePath().resolve("indexes");
        Files.deleteIfExists(pathToTable);
        try (FileChannel ignored = openFile(pathToTable);
             FileChannel ignored2 = openFile(pathToIndexes)) {
            this.storage = storage;
            writeMemorySegment = MemorySegment.mapFile(pathToTable, 0,
                    sizeInBytes + Long.BYTES * storage.size() * 2L,
                    FileChannel.MapMode.READ_WRITE, scope);
            indexesMemorySegment = MemorySegment.mapFile(pathToIndexes, 0,
                    (long) storage.size() * Long.BYTES, FileChannel.MapMode.READ_WRITE, scope);
        } catch (NoSuchFileException e) {
            writeMemorySegment = null;
        }

    }

    private FileChannel openFile(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void write() {
        long offset = 0;
        long offsetIndexes = 0;
        boolean valueIsNull;
        long keySizeInBytes;
        long valueSizeInBytes;
        for (Entry<MemorySegment> entry : storage.values()) {
            MemoryAccess.setLongAtOffset(indexesMemorySegment, offsetIndexes, offset);
            offsetIndexes += Long.BYTES;
            keySizeInBytes = entry.key().byteSize();
            MemoryAccess.setLongAtOffset(writeMemorySegment, offset, keySizeInBytes);
            offset += Long.BYTES;
            valueIsNull = entry.value() == null;
            if (valueIsNull) {
                valueSizeInBytes = -1;
            } else {
                valueSizeInBytes = entry.value().byteSize();
            }
            MemoryAccess.setLongAtOffset(writeMemorySegment, offset, valueSizeInBytes);
            offset += Long.BYTES;

            writeMemorySegment.asSlice(offset).copyFrom(entry.key());
            offset += keySizeInBytes;

            if (!valueIsNull) {
                writeMemorySegment.asSlice(offset).copyFrom(entry.value());
                offset += valueSizeInBytes;
            }
        }
    }

    @Override
    public void close() {
        scope.close();
    }
}
