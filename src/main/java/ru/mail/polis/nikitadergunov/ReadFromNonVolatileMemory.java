package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class ReadFromNonVolatileMemory implements AutoCloseable{

    private MemorySegment readMemorySegment;
    private boolean isExist;
    private final ResourceScope scope = ResourceScope.newConfinedScope();

    public ReadFromNonVolatileMemory(Config config) throws IOException {
        Path pathToTable = config.basePath().resolve("table");
        try (FileChannel readChannel = FileChannel.open(pathToTable, StandardOpenOption.READ)) {
            readMemorySegment = MemorySegment.mapFile(pathToTable, 0,
                    readChannel.size(), FileChannel.MapMode.READ_ONLY, scope);
            isExist = true;
        } catch (NoSuchFileException e) {
            readMemorySegment = null;
            isExist = false;
        }
    }

    public boolean isExist() {
        return isExist;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        long offset = 0;
        MemorySegment readKey;
        MemorySegment readValue = null;
        long lengthMemorySegment;

        while (offset < readMemorySegment.byteSize()) {
            lengthMemorySegment = readMemorySegment.asSlice(offset, Long.BYTES).asByteBuffer().getLong();
            offset += Long.BYTES;
            readKey = readMemorySegment.asSlice(offset, lengthMemorySegment);
            offset += lengthMemorySegment;

            lengthMemorySegment = readMemorySegment.asSlice(offset, Long.BYTES).asByteBuffer().getLong();
            offset += Long.BYTES;
            if (lengthMemorySegment != -1) {
                readValue = readMemorySegment.asSlice(offset, lengthMemorySegment);
                offset += lengthMemorySegment;
            }

            if (InMemoryDao.comparator(key, readKey) == 0) {
                return new BaseEntry<>(key, readValue);
            }
        }
        return null;
    }

    @Override
    public void close() {
        scope.close();
    }
}
