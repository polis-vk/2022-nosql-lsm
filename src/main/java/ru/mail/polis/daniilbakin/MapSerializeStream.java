package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

public class MapSerializeStream {

    private final FileChannel mapChannel;
    private final FileChannel indexesChannel;

    public MapSerializeStream(Path map, Path indexes) throws IOException {
        mapChannel = (FileChannel) Files.newByteChannel(map, Set.of(StandardOpenOption.WRITE));
        indexesChannel = (FileChannel) Files.newByteChannel(indexes, Set.of(StandardOpenOption.WRITE));
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
    protected int[] writeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        int[] indexes = new int[data.size()];
        int i = 0;
        int indexObjPosition = 0;
        for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry : data.entrySet()) {
            indexes[i++] = indexObjPosition;
            ByteBuffer localBuffer = ByteBuffer.allocate(
                    entry.getKey().capacity() + entry.getValue().value().capacity() + Integer.BYTES * 2
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
        buffer.position(buffer.arrayOffset());
        writeInt(buffer.capacity(), localBuffer);
        localBuffer.put(buffer);
    }

    private void writeInt(int i, ByteBuffer localBuffer) {
        localBuffer.putInt(i);
    }

}
