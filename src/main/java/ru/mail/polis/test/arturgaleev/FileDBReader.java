package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class FileDBReader implements AutoCloseable {

    private final int size;
    private final int fileID;
    private final RandomAccessFile reader;
    private final FileChannel channel;
    private final MappedByteBuffer pageData;
    private final MappedByteBuffer pageLinks;

    public FileDBReader(Path path) throws IOException {
        String fileName = path.getFileName().toString();
        fileID = Integer.parseInt(fileName.substring(0, fileName.length() - 4));
        reader = new RandomAccessFile(path.toFile(), "r");
        channel = reader.getChannel();
        MappedByteBuffer page = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        size = page.getInt(0);

        pageData = page.slice(Integer.BYTES * (1 + size), page.limit() - Integer.BYTES * (1 + size));
        pageLinks = page.slice(Integer.BYTES, Integer.BYTES * size);
    }

    public int getFileID() {
        return fileID;
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

    public FileIterator getIteratorByKey(ByteBuffer key) {
        return new FileIterator(getPosByKey(key));
    }

    public FileIterator getFromToIterator(ByteBuffer fromBuffer, ByteBuffer toBuffer) {
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

    FileIterator getIteratorByPos(int pos) {
        return new FileIterator(pos);
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

    public class FileIterator implements java.util.Iterator<BaseEntry<ByteBuffer>> {
        private int lastPos = size;
        private int currentPos = -1;
        private BaseEntry<ByteBuffer> current;

        private FileIterator() {
        }

        private FileIterator(int currentPos) {
            this.currentPos = currentPos;
        }

        private FileIterator(int currentPos, int lastPos) {
            this.lastPos = lastPos;
            this.currentPos = currentPos;
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
            return getEntryByPos(currentPos++);
        }
    }
}
