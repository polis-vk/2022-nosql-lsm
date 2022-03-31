package ru.mail.polis.artyomscheredin;

import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

public class Storage {
    private static final String DATA_FILE_NAME = "data";
    private static final String INDEXES_FILE_NAME = "indexes";
    private static final String META_INFO_FILE_NAME = "meta";
    private static final String EXTENSION = ".txt";
    private static final int HEADER_SIZE = 1;

    private final List<Utils.MappedBuffersPair> mappedDiskData;
    private final Config config;

    public Storage(Config config) {
        this.config = config;
        this.mappedDiskData = mapDiskData();
    }

    public List<PeekIterator> getIterators() {
        List<>
        for (Utils.MappedBuffersPair pair : mappedDiskData) {
            iteratorsList.add(new PeekIterator(new PersistentDao.FileIterator(pair.getDataBuffer(),
                    pair.getIndexBuffer(), from, to), priority++));
        }
    }

    private List<Utils.MappedBuffersPair> mapDiskData() throws IOException {
        int index = readPrevIndex();
        List<Utils.MappedBuffersPair> list = new LinkedList<>();
        for (int i = 1; i <= index; i++) {
            try (FileChannel dataChannel = FileChannel
                    .open(config.basePath().resolve(DATA_FILE_NAME + i + EXTENSION));
                 FileChannel indexChannel = FileChannel
                         .open(config.basePath().resolve(INDEXES_FILE_NAME + i + EXTENSION))) {
                ByteBuffer indexBuffer = indexChannel
                        .map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
                ByteBuffer dataBuffer = dataChannel
                        .map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
                list.add(new Utils.MappedBuffersPair(dataBuffer, indexBuffer));
            }
        }
        return list;
    }
}
