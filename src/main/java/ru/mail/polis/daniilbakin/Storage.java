package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
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

    public List<PeekIterator<BaseEntry<ByteBuffer>>> getFileIterators(ByteBuffer from, ByteBuffer to) {
        return deserialize.getIteratorsOfRange(from, to);
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
