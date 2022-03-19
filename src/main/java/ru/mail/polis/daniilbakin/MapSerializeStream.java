package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class MapSerializeStream {

    private final FileChannel mapChannel;
    private final FileChannel indexesChannel;

    public MapSerializeStream(Config config, int dataCount) throws IOException {
        Path mapPath = config.basePath().resolve("myLog" + dataCount);
        Path indexesPath = config.basePath().resolve("indexes" + dataCount);
        mapChannel = (FileChannel) Files.newByteChannel(mapPath, Set.of(WRITE, CREATE));
        indexesChannel = (FileChannel) Files.newByteChannel(indexesPath, Set.of(WRITE, CREATE));
    }

    public void close() throws IOException {
        mapChannel.close();
        indexesChannel.close();
    }

    public void serializeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        int[] indexes = writeMap(data);
        ByteBuffer buffer = ByteBuffer.allocate(indexes.length * Integer.BYTES);
        for (int i : indexes) {
            buffer.putInt(i);
        }
        buffer.flip();
        indexesChannel.write(buffer);
        indexesChannel.force(false);
    }

    /**
     * Return: array of indexes objects location.
     */
    private int[] writeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        int[] indexes = new int[data.size()];
        int i = 0;
        int indexObjPosition = 0;
        for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry : data.entrySet()) {
            indexes[i++] = indexObjPosition;
            int valueCapacity = (entry.getValue().value() == null) ? 0 : entry.getValue().value().capacity();
            ByteBuffer localBuffer = ByteBuffer.allocate(
                    entry.getKey().capacity() + valueCapacity + Integer.BYTES * 2
            );
            writeEntry(entry, localBuffer);
            localBuffer.flip();
            indexObjPosition += localBuffer.capacity();
            mapChannel.write(localBuffer);
        }
        mapChannel.force(false);
        return indexes;
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
