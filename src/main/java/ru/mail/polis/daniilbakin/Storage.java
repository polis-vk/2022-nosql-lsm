package ru.mail.polis.daniilbakin;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

public class Storage implements Closeable {

    protected final static String DATA_FILE_NAME = "myData";
    protected final static String INDEX_FILE_NAME = "indexes";
    protected final static String FILE_EXT = ".dat";

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
        MapSerializeStream writer = new MapSerializeStream(config, numOfFiles);
        writer.serializeMap(data);
        writer.close();
    }

}
