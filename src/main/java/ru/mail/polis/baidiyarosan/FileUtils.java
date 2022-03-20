package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileUtils {

    public static final int NULL_SIZE_FLAG = -1;

    private FileUtils(){
        // Utility class
    }

    public static int sizeOfEntry(BaseEntry<ByteBuffer> entry) {
        return 2 * Integer.BYTES + entry.key().capacity() + (entry.value() == null ? 0 : entry.value().capacity());
    }

    public static int readInt(FileChannel in, ByteBuffer temp) throws IOException {
        temp.clear();
        in.read(temp);
        return temp.flip().getInt();
    }

    public static long readLong(FileChannel in, ByteBuffer temp) throws IOException {
        temp.clear();
        in.read(temp);
        return temp.flip().getLong();
    }

    public static ByteBuffer readBuffer(FileChannel in, int size) throws IOException {
        if (size == NULL_SIZE_FLAG) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        in.read(buffer);
        return buffer.flip();
    }

    public static ByteBuffer readBuffer(FileChannel in, ByteBuffer temp) throws IOException {
        return readBuffer(in, FileUtils.readInt(in, temp));
    }

    public static ByteBuffer readBuffer(FileChannel in, long pos, ByteBuffer temp) throws IOException {
        in.position(pos);
        return readBuffer(in, FileUtils.readInt(in, temp));
    }

    public static ByteBuffer writeEntryToBuffer(ByteBuffer buffer, BaseEntry<ByteBuffer> entry) {
        buffer.putInt(entry.key().capacity()).put(entry.key());
        if (entry.value() == null) {
            buffer.putInt(NULL_SIZE_FLAG);
        } else {
            buffer.putInt(entry.value().capacity()).put(entry.value());
        }
        return buffer.flip();
    }

}
