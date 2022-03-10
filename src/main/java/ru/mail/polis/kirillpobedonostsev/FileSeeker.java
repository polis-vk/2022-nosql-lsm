package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSeeker {
    private final Path dataPath;
    private final Path indexPath;

    public FileSeeker(Path dataPath, Path indexPath) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
    }

    public BaseEntry<ByteBuffer> tryFind(ByteBuffer key) throws IOException {
        if (Files.notExists(dataPath)) {
            return null;
        }
        ByteBuffer value = null;
        try (RandomAccessFile dataFile = new RandomAccessFile(dataPath.toFile(), "r");
             RandomAccessFile indexFile = new RandomAccessFile(indexPath.toFile(), "r");
             FileChannel channel = dataFile.getChannel()) {
            long low = 0;
            long high = indexFile.length() / Long.BYTES;
            while (low <= high) {
                long mid = (high + low) / 2;
                indexFile.seek(mid * Long.BYTES);
                long offset = indexFile.readLong();
                dataFile.seek(offset);
                int keyLength = dataFile.readInt();
                ByteBuffer readKey = ByteBuffer.allocate(keyLength);
                channel.read(readKey);
                readKey.rewind();
                int compareResult = readKey.compareTo(key);
                if (compareResult > 0) {
                    high = mid - 1;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    int valueLength = dataFile.readInt();
                    value = ByteBuffer.allocate(valueLength);
                    channel.read(value);
                    value.rewind();
                    break;
                }
            }
        }
        return value == null ? null : new BaseEntry<>(key, value);
    }
}
