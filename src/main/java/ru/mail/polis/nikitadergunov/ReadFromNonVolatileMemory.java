package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemoryAccess;
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
    private final ResourceScope scope = ResourceScope.newSharedScope();

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
        MemorySegment readValue;
        long lengthKey;
        long lengthValue;

        while (offset < readMemorySegment.byteSize()) {
            lengthKey = MemoryAccess.getLongAtOffset(readMemorySegment, offset);
            offset += Long.BYTES;
            lengthValue = MemoryAccess.getLongAtOffset(readMemorySegment, offset);
            offset += Long.BYTES;
            if (lengthValue == -1) {
                offset += lengthKey;
                continue;
            }
            readKey = readMemorySegment.asSlice(offset, lengthKey);
            offset += lengthKey;
            if (key.byteSize() != lengthKey) {
                offset += lengthValue;
                continue;
            }
            readValue = readMemorySegment.asSlice(offset, lengthValue);
            offset += lengthValue;
            if (InMemoryDao.comparator(key, readKey) == 0) {
                return new BaseEntry<>(key, readValue);
            }
        }
        return null;
    }

    @Override
    public void close() {
        if (!scope.isAlive()) {
            return;
        }
        scope.close();
    }
}
