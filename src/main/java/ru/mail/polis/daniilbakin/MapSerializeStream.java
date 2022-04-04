package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static ru.mail.polis.daniilbakin.Storage.DATA_FILE_NAME;
import static ru.mail.polis.daniilbakin.Storage.INDEX_FILE_NAME;

public class MapSerializeStream implements Closeable {

    private final FileChannel mapChannel;
    private final FileChannel indexesChannel;


    public MapSerializeStream(Config config, int dataCount, int startIndexFile) throws IOException {
        int newIndex = (startIndexFile == -1) ? 0 : startIndexFile + dataCount;
        Path mapPath = config.basePath().resolve(DATA_FILE_NAME + newIndex);
        Path indexesPath = config.basePath().resolve(INDEX_FILE_NAME + newIndex);
        Files.deleteIfExists(mapPath);
        Files.deleteIfExists(indexesPath);
        mapChannel = (FileChannel) Files.newByteChannel(mapPath, Set.of(WRITE, CREATE_NEW));
        indexesChannel = (FileChannel) Files.newByteChannel(indexesPath, Set.of(WRITE, CREATE_NEW));
    }

    //Constructor for compact method
    public MapSerializeStream(Config config, String prefix) throws IOException {
        Path mapPath = config.basePath().resolve(prefix + DATA_FILE_NAME);
        Path indexesPath = config.basePath().resolve(prefix + INDEX_FILE_NAME);
        Files.deleteIfExists(mapPath);
        Files.deleteIfExists(indexesPath);
        mapChannel = (FileChannel) Files.newByteChannel(mapPath, Set.of(WRITE, CREATE_NEW));
        indexesChannel = (FileChannel) Files.newByteChannel(indexesPath, Set.of(WRITE, CREATE_NEW));
    }

    @Override
    public void close() throws IOException {
        mapChannel.close();
        indexesChannel.close();
    }

    public void serializeData(Iterator<BaseEntry<ByteBuffer>> dataIterator) throws IOException {
        int indexObjPosition = 0;
        ByteBuffer localBuffer = ByteBuffer.allocate(512);
        ByteBuffer indexBuffer = ByteBuffer.allocate(Integer.BYTES);
        while (dataIterator.hasNext()) {
            BaseEntry<ByteBuffer> entry = dataIterator.next();
            int valueCapacity = (entry.value() == null) ? 0 : entry.value().capacity();
            int bufferSize = entry.key().capacity() + valueCapacity + Integer.BYTES * 2;

            if (localBuffer.capacity() < bufferSize) {
                localBuffer = ByteBuffer.allocate(bufferSize);
            } else {
                localBuffer.clear();
            }
            writeEntry(entry, localBuffer);
            localBuffer.flip();
            indexBuffer.putInt(indexObjPosition);
            indexBuffer.flip();
            indexObjPosition += bufferSize;
            mapChannel.write(localBuffer.slice(0, bufferSize));
            indexesChannel.write(indexBuffer);
            indexBuffer.clear();
        }
        mapChannel.force(false);
        indexesChannel.force(false);
    }

    private void writeEntry(BaseEntry<ByteBuffer> entry, ByteBuffer localBuffer) {
        writeByteBuffer(entry.key(), localBuffer);
        writeByteBuffer(entry.value(), localBuffer);
    }

    private void writeByteBuffer(ByteBuffer buffer, ByteBuffer localBuffer) {
        if (buffer == null) {
            localBuffer.putInt(-1);
            return;
        }
        localBuffer.putInt(buffer.capacity());
        localBuffer.put(buffer);
    }

}
