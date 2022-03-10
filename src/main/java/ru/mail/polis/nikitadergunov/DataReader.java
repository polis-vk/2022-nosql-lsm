package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class DataReader implements AutoCloseable {
    public MemorySegment dataMemorySegment;
    private MemorySegment indexedMemorySegment;
    private boolean isExist;
    private final ResourceScope scope = ResourceScope.globalScope();

    public DataReader(Path pathData, Path pathIndexes) throws IOException {
        try(FileChannel readChannel = FileChannel.open(pathData, StandardOpenOption.READ);
        FileChannel indexesChannel = FileChannel.open(pathIndexes, StandardOpenOption.READ)) {
            dataMemorySegment = MemorySegment.mapFile(pathData, 0,
                    readChannel.size(), FileChannel.MapMode.READ_ONLY, scope);
            indexedMemorySegment = MemorySegment.mapFile(pathIndexes, 0, indexesChannel.size(), FileChannel.MapMode.READ_ONLY, scope);
            isExist = true;
        } catch (NoSuchFileException e) {
            dataMemorySegment = null;
            indexedMemorySegment = null;
            isExist = false;
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        WrapperMemorySegment readKey = binarySearch(key);
        long offset = readKey.offset;
        MemorySegment keyMemorySegment = readKey.memorySegment;
        long valueSize = MemoryAccess.getLongAtOffset(dataMemorySegment, offset);
        return valueSize == -1 ? new BaseEntry<>(keyMemorySegment, null) :
                new BaseEntry<>(keyMemorySegment, dataMemorySegment.asSlice(offset + Long.BYTES, valueSize));
    }

    public WrapperMemorySegment getFirst() {
        return getKeyByIndex(0);
    }

    public WrapperMemorySegment binarySearch(MemorySegment key) {
        long left = 0;
        long right = indexedMemorySegment.byteSize() / Long.BYTES - 1;
        long middle;
        WrapperMemorySegment readKey = null;
        while (left < right) {
            middle = (left + right) / 2;
            readKey = getKeyByIndex(getOffset(middle * Long.BYTES));
            if (InMemoryDao.comparator(readKey.memorySegment, key) >= 0) {
                right = middle;
            } else {
                left = middle + 1;
            }
        }

        readKey = getKeyByIndex(getOffset(left * Long.BYTES));
        if (InMemoryDao.comparator(readKey.memorySegment, key) < 0) {
            return new WrapperMemorySegment(null, dataMemorySegment.byteSize(), 0);
        }

        return getKeyByIndex(getOffset(left * Long.BYTES));
    }

    private long getOffset(long offset) {
        return MemoryAccess.getLongAtOffset(indexedMemorySegment, offset);
    }

    private WrapperMemorySegment getKeyByIndex(long offset) {
        long lengthKey = MemoryAccess.getLongAtOffset(dataMemorySegment, offset);
        offset += Long.BYTES;
        return new WrapperMemorySegment(dataMemorySegment.asSlice(offset, lengthKey), offset + lengthKey, lengthKey);
    }

    @Override
    public void close() {
        if (!scope.isAlive()) {
            return;
        }
        scope.close();
    }

    public boolean isExist() {
        return isExist;
    }

    public static class WrapperMemorySegment {
        public final MemorySegment memorySegment;
        public final long offset;
        public final long lengthMemorySegment;
        public Entry<MemorySegment> nowEntry;

        public WrapperMemorySegment(MemorySegment memorySegment, long offset, long lengthMemorySegment) {
            this.memorySegment = memorySegment;
            this.offset = offset;
            this.lengthMemorySegment = lengthMemorySegment;
        }
    }

    public static class DataReaderIterator implements Iterator<Entry<MemorySegment>> {
        private long nowPositionOffset;
        private final long finishPositionOffset;
        private final MemorySegment dataMemorySegment;
        private Entry<MemorySegment> nowValue;
        private boolean hasLast;

        public DataReaderIterator(long nowPositionOffset, long finishPositionOffset, MemorySegment dataMemorySegment) {
            this.nowPositionOffset = nowPositionOffset;
            this.finishPositionOffset = finishPositionOffset;
            this.dataMemorySegment = dataMemorySegment;
            hasLast = nowPositionOffset < finishPositionOffset;
        }

        @Override
        public boolean hasNext() {
            boolean resultCompare = nowPositionOffset < finishPositionOffset;
            if (resultCompare) {
                return true;
            }
            if (hasLast) {
                hasLast = false;
                return true;
            }
            return nowValue != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            boolean resultCompare = nowPositionOffset < finishPositionOffset;
            if (resultCompare) {
                long lengthMemorySegment = MemoryAccess.getLongAtOffset(dataMemorySegment, nowPositionOffset);
                nowPositionOffset += Long.BYTES;
                MemorySegment key = dataMemorySegment.asSlice(nowPositionOffset, lengthMemorySegment);
                nowPositionOffset += lengthMemorySegment;
                lengthMemorySegment = MemoryAccess.getLongAtOffset(dataMemorySegment, nowPositionOffset);
                nowPositionOffset += Long.BYTES;
                if (lengthMemorySegment == -1) {
                    return new BaseEntry<>(key, null);
                }
                MemorySegment value = dataMemorySegment.asSlice(nowPositionOffset, lengthMemorySegment);
                nowPositionOffset += lengthMemorySegment;
                nowValue = new BaseEntry<>(key, value);
                return nowValue;
            }
            if (nowValue != null) {
                Entry<MemorySegment> copyCurrentValue = nowValue;
                nowValue = null;
                return copyCurrentValue;
            }
            return null;

        }

        public Entry<MemorySegment> getNow() {
            return nowValue;
        }
    }

}
