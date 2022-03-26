package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryAndDiskDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection = new ConcurrentSkipListMap<>();

    private final List<MappedByteBuffer> files = new ArrayList<>();

    private final List<MappedByteBuffer> fileIndexes = new ArrayList<>();

    private final Path path;

    public MemoryAndDiskDao(Config config) throws IOException {
        this.path = config.basePath();
        Path indexesDir = path.resolve(Paths.get(FileUtils.INDEX_FOLDER));
        if (Files.notExists(indexesDir)) {
            Files.createDirectory(indexesDir);
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap =
                new PriorityQueue<>(Comparator.comparing(o -> o.peek().key()));
        Iterator<BaseEntry<ByteBuffer>> temp = FileUtils.getInMemoryIterator(collection, from, to);
        if (temp.hasNext()) {
            heap.add(new PeekIterator<>(temp, Integer.MAX_VALUE));
        }
        for (int i = 0; i < FileUtils.getPaths(path).size(); ++i) {

            // file naming starts from 1, collections ordering starts from 0;
            Path filePath = FileUtils.getDataPath(path, i + 1);
            Path indexPath = FileUtils.getIndexPath(path, i + 1);
            if (files.size() <= i || files.get(i) == null) {
                try (FileChannel in = FileChannel.open(filePath, StandardOpenOption.READ);
                     FileChannel indexes = FileChannel.open(indexPath, StandardOpenOption.READ)
                ) {
                    files.add(i, in.map(FileChannel.MapMode.READ_ONLY, 0, in.size()));
                    fileIndexes.add(i, indexes.map(FileChannel.MapMode.READ_ONLY, 0, indexes.size()));
                }
            }
            // file indexes starts with 1, but collections starts with 0
            temp = FileUtils.getInFileIterator(files.get(i), fileIndexes.get(i), from, to);
            if (temp.hasNext()) {
                heap.add(new PeekIterator<>(temp, i + 1));
            }
        }

        return new MemoryAndDiskDaoIterator(heap);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        collection.put(entry.key(), entry);
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> value = Dao.super.get(key);
        if (value == null) {
            return null;
        }
        return value.value() == null ? null : value;
    }

    @Override
    public void flush() throws IOException {
        if (collection.isEmpty()) {
            return;
        }

        FileUtils.writeOnDisk(collection, path);
    }

}
