package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MapDeserializeStream {

    private final MapInputStream mapReader;
    private final FileInputStream indexesReader;

    public MapDeserializeStream(File map, File indexes) throws IOException {
        indexesReader = new FileInputStream(indexes);
        mapReader = new MapInputStream(getIndexes(), map);
    }

    public BaseEntry<ByteBuffer> readByKey(ByteBuffer key) throws IOException {
        return mapReader.readByKey(key);
    }

    public void close() throws IOException {
        mapReader.close();
        indexesReader.close();
    }

    private int[] getIndexes() throws IOException {
        int[] indexes = new int[indexesReader.available() / Integer.BYTES];
        ByteBuffer buffer = ByteBuffer.allocate(indexesReader.available());
        buffer.put(indexesReader.readAllBytes());
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = buffer.getInt(i * 4);
        }
        indexesReader.close();
        return indexes;
    }

}
