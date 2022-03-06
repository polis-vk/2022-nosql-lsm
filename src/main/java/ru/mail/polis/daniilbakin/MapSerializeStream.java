package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class MapSerializeStream {

    private final MapOutputStream mapWriter;
    private final FileOutputStream indexesWriter;

    public MapSerializeStream(File map, File indexes) throws FileNotFoundException {
        mapWriter = new MapOutputStream(map);
        indexesWriter = new FileOutputStream(indexes);
    }

    public void serializeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        int[] indexes = mapWriter.writeMap(data);
        mapWriter.close();
        ByteBuffer buffer = ByteBuffer.allocate(indexes.length * Integer.BYTES);
        for (int i : indexes) {
            buffer.putInt(i);
        }
        indexesWriter.write(buffer.array());
    }

    public void close() throws IOException {
        mapWriter.close();
        indexesWriter.close();
    }

}
