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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryAndDiskDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection = new ConcurrentSkipListMap<>();

    private final List<MappedByteBuffer> files = new ArrayList<>();

    private final List<MappedByteBuffer> fileIndexes = new ArrayList<>();

    private final Path path;

    private final int filesCount;

    public MemoryAndDiskDao(Config config) throws IOException {
        this.path = config.basePath();
        this.filesCount = FileUtils.getPaths(path).size();
        Path indexesDir = path.resolve(Paths.get(FileUtils.INDEX_FOLDER));
        if (Files.notExists(indexesDir)) {
            Files.createDirectory(indexesDir);
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        List<PeekIterator<BaseEntry<ByteBuffer>>> list = new LinkedList<>();
        Collection<BaseEntry<ByteBuffer>> temp = FileUtils.getInMemoryCollection(collection, from, to);
        if (!temp.isEmpty()) {
            list.add(new PeekIterator<>(temp.iterator(), 0));
        }
        for (int i = 0; i < filesCount; ++i) {

            // file naming starts from 1, collections ordering starts from 0
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

            temp = FileUtils.getInFileCollection(files.get(i), fileIndexes.get(i), from, to);
            if (!temp.isEmpty()) {
                list.add(new PeekIterator<>(temp.iterator(), filesCount - i));
            }
        }

        return new MergingIterator(list);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        collection.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (collection.isEmpty()) {
            return;
        }

        FileUtils.writeOnDisk(collection, path);
    }

}
