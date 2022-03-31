package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static ru.mail.polis.daniilbakin.Storage.DATA_FILE_NAME;
import static ru.mail.polis.daniilbakin.Storage.INDEX_FILE_NAME;

public class MapSerializeStream implements Closeable {

    private final FileChannel mapChannel;
    private final FileChannel indexesChannel;

    public MapSerializeStream(Config config, int dataCount) throws IOException {
        Path mapPath = config.basePath().resolve(DATA_FILE_NAME + dataCount);
        Path indexesPath = config.basePath().resolve(INDEX_FILE_NAME + dataCount);
        mapChannel = (FileChannel) Files.newByteChannel(mapPath, Set.of(WRITE, CREATE_NEW));
        indexesChannel = (FileChannel) Files.newByteChannel(indexesPath, Set.of(WRITE, CREATE_NEW));
    }

    @Override
    public void close() throws IOException {
        mapChannel.close();
        indexesChannel.close();
    }

    public void serializeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        int indexObjPosition = 0;
        ByteBuffer localBuffer = ByteBuffer.allocate(512);
        ByteBuffer indexBuffer = ByteBuffer.allocate(Integer.BYTES);
        for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry : data.entrySet()) {
            int valueCapacity = (entry.getValue().value() == null) ? 0 : entry.getValue().value().capacity();
            int bufferSize = entry.getKey().capacity() + valueCapacity + Integer.BYTES * 2;
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

    private void writeEntry(Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry, ByteBuffer localBuffer) {
        writeByteBuffer(entry.getKey(), localBuffer);
        writeByteBuffer(entry.getValue().value(), localBuffer);
    }

    private void writeByteBuffer(ByteBuffer buffer, ByteBuffer localBuffer) {
        if (buffer == null) {
            writeInt(-1, localBuffer);
            return;
        }
        buffer.position(buffer.arrayOffset());
        writeInt(buffer.capacity(), localBuffer);
        localBuffer.put(buffer);
    }

    private void writeInt(int i, ByteBuffer localBuffer) {
        localBuffer.putInt(i);
    }

}
