package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class Storage implements Closeable {

    public static final String DATA_FILE_NAME = "myData";
    public static final String INDEX_FILE_NAME = "indexes";

    private final int numOfFiles;
    private final MapsDeserializeStream deserialize;
    private final Config config;

    protected Storage(Config config) throws IOException {
        this.config = config;
        deserialize = new MapsDeserializeStream(config);
        numOfFiles = deserialize.getNumberOfFiles();
    }

    public Iterator<BaseEntry<ByteBuffer>> get(
            ByteBuffer from, ByteBuffer to, Iterator<BaseEntry<ByteBuffer>> inMemoryIterator
    ) {
        return deserialize.getRange(from, to, new PeekIterator<>(inMemoryIterator, -1));
    }

    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        if (numOfFiles == 0) {
            return null;
        }
        BaseEntry<ByteBuffer> entry = deserialize.readByKey(key);
        if (entry != null && entry.value() != null) {
            return entry;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        deserialize.close();
    }

    public void flush(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        if (data.isEmpty()) return;
        MapSerializeStream writer = new MapSerializeStream(config, numOfFiles);
        writer.serializeMap(data);
        writer.close();
    }

}
