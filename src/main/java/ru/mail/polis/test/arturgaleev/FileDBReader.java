package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class FileDBReader implements AutoCloseable {

    private final RandomAccessFile reader;
    private final int size;
    private int fileID = -1;
    private final FileChannel channel;
    private final MappedByteBuffer page;
    private final MappedByteBuffer pageData;
    private final MappedByteBuffer pageLinks;

    public FileDBReader(Path path) throws IOException {
        reader = new RandomAccessFile(path.toFile(), "r");
        channel = reader.getChannel();
        page = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        page.load();
        size = page.getInt(0);

        pageData = page.slice(Integer.BYTES * (1 + size), page.limit() - Integer.BYTES * (1 + size));
        pageLinks = page.slice(Integer.BYTES, Integer.BYTES * size);
    }

    protected BaseEntry<ByteBuffer> readEntry(int pos) {
        int currentPos = pos;
        int keyLength = pageData.getInt(currentPos);
        currentPos += Integer.BYTES;
        int valueLength = pageData.getInt(currentPos);
        currentPos += Integer.BYTES;
        return new BaseEntry<>(pageData.slice(currentPos, keyLength),
                ((valueLength == -1) ? null : pageData.slice(currentPos + keyLength, valueLength)));
    }

    public BaseEntry<ByteBuffer> getEntryByPos(int pos) {
        if (pos < 0 || pos >= size) {
            return null;
        }
        return readEntry(pageLinks.slice(pos * Integer.BYTES, Integer.BYTES).getInt());
    }

    private int getPosByKey(ByteBuffer key) {
        int low = 0;
        int high = size - 1;
        int mid;
        while (low <= high) {
            mid = low + ((high - low) / 2);
            int result = getEntryByPos(mid).key().compareTo(key);
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

    public BaseEntry<ByteBuffer> getEntryByKey(ByteBuffer key) {
        BaseEntry<ByteBuffer> entry = getEntryByPos(getPosByKey(key));
        if (entry == null) {
            return null;
        }
        return entry.key().equals(key) ? entry : null;
    }

    public PeakingIterator getIteratorByKey(ByteBuffer key) {
        return new PeakingIterator(getPosByKey(key));
    }

    public PeakingIterator getFromToIterator(ByteBuffer fromBuffer, ByteBuffer toBuffer) {
        if (fromBuffer == null && toBuffer == null) {
            return new PeakingIterator(0, size);
        } else if (fromBuffer == null) {
            return new PeakingIterator(0, getPosByKey(toBuffer));
        } else if (toBuffer == null) {
            return new PeakingIterator(getPosByKey(fromBuffer), size);
        } else {
            return new PeakingIterator(getPosByKey(fromBuffer), getPosByKey(toBuffer));
        }
    }

    // protected because Im using for my local tests
    protected PeakingIterator getIteratorByPos(int pos) {
        return new PeakingIterator(pos);
    }

    @Override
    public void close() throws IOException {
        channel.close();
        reader.close();
    }

    public String toString(ByteBuffer in) {
        ByteBuffer data = in.asReadOnlyBuffer();
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // If equals to -1 -> id doesn't set
    // else return file id
    public int getFileID() {
        return fileID;
    }

    // sets file id
    public void setFileID(int fileID) {
        this.fileID = fileID;
    }

    public class PeakingIterator implements java.util.Iterator<BaseEntry<ByteBuffer>> {
        private int lastPos = size;
        private int currentPos = -1;
        private BaseEntry<ByteBuffer> current = null;

        private PeakingIterator() {
        }

        private PeakingIterator(int currentPos) {
            this.currentPos = currentPos;
        }

        private PeakingIterator(int currentPos, int lastPos) {
            this.lastPos = lastPos;
            this.currentPos = currentPos;
        }

        public BaseEntry<ByteBuffer> peek() {
            if (current == null) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                current = getEntryByPos(currentPos++);
            }
            return current;
        }

        public int getFileId() {
            return fileID;
        }

        @Override
        public boolean hasNext() {
            return current != null || (currentPos >= 0 && currentPos < size && currentPos < lastPos);
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            BaseEntry<ByteBuffer> peek = peek();
            current = null;
            return peek;
        }
    }
}
