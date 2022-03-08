package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class FileSeeker {
    private final Path dataFilename;

    public FileSeeker(Path filename) {
        this.dataFilename = filename;
    }

    public BaseEntry<ByteBuffer> tryFind(ByteBuffer key) {
        ByteBuffer value = null;
        try (RandomAccessFile file = new RandomAccessFile(dataFilename.toFile(), "r");
             FileChannel channel = file.getChannel()) {
            while (channel.position() < channel.size()){
                int keyLength = file.readInt();
                ByteBuffer readKey = ByteBuffer.allocate(keyLength);
                channel.read(readKey);
                readKey.flip();
                int valueLength = file.readInt();
                if (readKey.compareTo(key) == 0) {
                    value = ByteBuffer.allocate(valueLength);
                    channel.read(value);
                } else {
                    file.skipBytes(valueLength);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value == null ? null : new BaseEntry<>(key, value);
    }
}
