package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public final class FileUtils {

    public static final int NULL_SIZE_FLAG = -1;

    public static final String DATA_FILE_HEADER = "data";

    public static final String INDEX_FOLDER = "indexes";

    public static final String INDEX_FILE_HEADER = "index";

    public static final String FILE_EXTENSION = ".log";

    private FileUtils() {
        // Utility class
    }


    public static int getFileNumber(Path pathToFile) {
        String number = pathToFile.getFileName().toString()
                .replaceFirst(DATA_FILE_HEADER, "")
                .replaceFirst(FILE_EXTENSION, "");
        return Integer.parseInt(number);
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


    public static int getStartIndex(FileChannel in, long[] indexes, ByteBuffer key, ByteBuffer temp)
            throws IOException {
        int min = 0;
        int max = indexes.length - 1;
        int mid;
        int comparison;
        while (min <= max) {
            if (key.compareTo(readBuffer(in, indexes[min], temp)) <= 0) {
                return min;
            }
            comparison = key.compareTo(readBuffer(in, indexes[max], temp));
            if (comparison > 0) {
                return -1;
            }
            if (comparison == 0) {
                return max;
            }
            mid = min + (max - min) / 2;
            comparison = key.compareTo(readBuffer(in, indexes[mid], temp));
            if (comparison == 0) {
                return mid;
            }
            if (comparison > 0) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }
        return max;
    }

    public static int getEndIndex(FileChannel in, long[] indexes, ByteBuffer key, ByteBuffer temp)
            throws IOException {
        int min = 0;
        int max = indexes.length - 1;
        int mid;
        while (min <= max) {
            if (key.compareTo(readBuffer(in, indexes[min], temp)) <= 0) {
                return -1;
            }
            if (key.compareTo(readBuffer(in, indexes[max], temp)) > 0) {
                return max;
            }
            mid = min + 1 + (max - min) / 2;
            if (key.compareTo(readBuffer(in, indexes[mid], temp)) > 0) {
                min = mid;
            } else {
                max = mid - 1;
            }
        }
        return min;
    }

}
