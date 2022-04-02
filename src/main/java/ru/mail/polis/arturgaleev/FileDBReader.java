package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileDBReader implements AutoCloseable {

    private final long size;
    private final long fileID;
    private final ResourceScope scope;
    private final MemorySegment pageData;
    private final MemorySegment pageLinks;

    public FileDBReader(Path path) throws IOException {
        scope = ResourceScope.newConfinedScope();
        String fileName = path.getFileName().toString();
        fileID = Long.parseLong(fileName.substring(0, fileName.length() - 4));
        MemorySegment page = MemorySegment.mapFile(path, 0, Files.size(path), FileChannel.MapMode.READ_ONLY, scope);
        size = MemoryAccess.getLongAtOffset(page, 0);
        pageData = page.asSlice(Long.BYTES * (1 + size));
        pageLinks = page.asSlice(Long.BYTES, Long.BYTES * size);
    }

    public long getFileID() {
        return fileID;
    }

    private Entry<MemorySegment> readEntryByLink(long linkPos) {
        long currentLinkPos = linkPos;
        long keyLength = MemoryAccess.getLongAtOffset(pageData, currentLinkPos);
        currentLinkPos += Long.BYTES;
        long valueLength = MemoryAccess.getLongAtOffset(pageData, currentLinkPos);
        currentLinkPos += Long.BYTES;
        return new BaseEntry<>(pageData.asSlice(currentLinkPos, keyLength),
                ((valueLength == -1) ? null : pageData.asSlice(currentLinkPos + keyLength, valueLength)));
    }

    public Entry<MemorySegment> readEntryByPos(long pos) {
        if (pos < 0 || pos >= size) {
            return null;
        }
        return readEntryByLink(MemoryAccess.getLongAtOffset(pageLinks, pos * Long.BYTES));
    }

    protected MemorySegment readKeyByLink(long linkPos) {
        long currentLinkPos = linkPos;
        long keyLength = MemoryAccess.getLongAtOffset(pageData, currentLinkPos);
        currentLinkPos += 2 * Long.BYTES;
        return pageData.asSlice(currentLinkPos, keyLength);
    }

    public MemorySegment readKeyByPos(long pos) {
        if (pos < 0 || pos >= size) {
            return null;
        }
        return readKeyByLink(MemoryAccess.getLongAtOffset(pageLinks, pos * Long.BYTES));
    }

    private long getPosByKey(MemorySegment key) {
        long low = 0;
        long high = size - 1;
        long mid;
        long result;
        while (low <= high) {
            mid = low + ((high - low) / 2);
            result = MemorySegmentComparator.INSTANCE.compare(readKeyByPos(mid), key);
            if (result < 0) {
                low = mid + 1;
            } else if (result > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return low;
    }

    public Entry<MemorySegment> getEntryByKey(MemorySegment key) {
        Entry<MemorySegment> entry = readEntryByPos(getPosByKey(key));
        if (entry == null) {
            return null;
        }
        return MemorySegmentComparator.INSTANCE.compare(entry.key(), key) == 0 ? entry : null;
    }

    public FileIterator getFromToIterator(MemorySegment fromBuffer, MemorySegment toBuffer) {
        if (fromBuffer == null && toBuffer == null) {
            return new FileIterator(0, size);
        } else if (fromBuffer == null) {
            return new FileIterator(0, getPosByKey(toBuffer));
        } else if (toBuffer == null) {
            return new FileIterator(getPosByKey(fromBuffer), size);
        } else {
            return new FileIterator(getPosByKey(fromBuffer), getPosByKey(toBuffer));
        }
    }

    FileIterator getIteratorByPos(long pos) {
        return new FileIterator(pos);
    }

    @Override
    public void close() throws IOException {
        scope.close();
    }

    public class FileIterator implements java.util.Iterator<Entry<MemorySegment>> {
        private long lastPos = size;
        private long currentPos = -1;

        private FileIterator() {
        }

        private FileIterator(long currentPos) {
            this.currentPos = currentPos;
        }

        private FileIterator(long currentPos, long lastPos) {
            this.lastPos = lastPos;
            this.currentPos = currentPos;
        }

        public long getFileId() {
            return fileID;
        }

        @Override
        public boolean hasNext() {
            return currentPos >= 0 && currentPos < size && currentPos < lastPos;
        }

        @Override
        public Entry<MemorySegment> next() {
            return readEntryByPos(currentPos++);
        }
    }
}
