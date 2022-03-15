package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import static ru.mail.polis.artyomscheredin.Utils.readEntry;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String DATA_FILE_NAME = "data";
    private static final String INDEXES_FILE_NAME = "indexes";
    private static final String EXTENSION = ".txt";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> inMemoryData =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final Config config;

    public PersistentDao(Config config) {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (inMemoryData.isEmpty()) {
            return Collections.emptyIterator();
        }

        lock.readLock().lock();
        try {
            List<Utils.Pair<Path>> paths = getDataPathsToRead();
            try {
                return new FileIterator(paths.get(paths.size() - 1), from, to);
            } catch (IOException e) {
                return Collections.emptyIterator();
            }
         /*   if ((from == null) && (to == null)) {
                return inMemoryData.values().iterator();
            } else if (from == null) {
                return inMemoryData.headMap(to).values().iterator();
            } else if (to == null) {
                return inMemoryData.tailMap(from).values().iterator();
            }
            return inMemoryData.subMap(from, to).values().iterator();*/
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> value = inMemoryData.get(key);
            if (value != null) {
                return value;
            }

            List<Utils.Pair<Path>> paths = getDataPathsToRead();
            if (paths == null) {
                return null;
            }

            Utils.Pair<Path> curPath;
            ListIterator<Utils.Pair<Path>> listIterator = paths.listIterator(paths.size());
            while (listIterator.hasPrevious()) {
                curPath = listIterator.previous();
                BaseEntry<ByteBuffer> entry = getEntryIfExists(curPath, key);
                if (entry != null) {
                    return entry;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<Utils.Pair<Path>> getDataPathsToRead() {
        List<Utils.Pair<Path>> list = new LinkedList<>();
        File[] files = config.basePath().toFile().listFiles();
        if ((files == null) || (files.length == 0)) {
            return null;
        }

        Pattern pattern = Pattern.compile(DATA_FILE_NAME + "[0-9]+" + EXTENSION);
        for (File el : files) {
            if (pattern.matcher(el.getName()).matches()) {
                int index = getIndex(el);
                Path indexPath = config.basePath().resolve(INDEXES_FILE_NAME + index + EXTENSION);
                Utils.Pair<Path> curPaths = new Utils.Pair<Path>(el.toPath(), indexPath);
                list.add(curPaths);
            }
        }
        return list;
    }

    private int getIndex(File file) {
        String temp = file.toString();
        temp = temp.split(DATA_FILE_NAME)[1];
        temp = temp.split(EXTENSION)[0];
        return Integer.parseInt(temp);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        lock.readLock().lock();
        try {
            inMemoryData.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            store(inMemoryData);
            inMemoryData.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private BaseEntry<ByteBuffer> getEntryIfExists(Utils.Pair<Path> paths, ByteBuffer key) throws IOException {
        ByteBuffer indexBuffer;
        ByteBuffer dataBuffer;
        try (FileChannel dataChannel = FileChannel.open(paths.dataPath());
             FileChannel indexChannel = FileChannel.open(paths.indexPath())) {
            indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            dataBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
        } catch (NoSuchFileException e) {
            return null;
        }

        int offset = Utils.findOffset(indexBuffer, dataBuffer, key, Utils.Strategy.ACCURATE_KEY_REQUIRED);
        if (offset == -1) {
            return null;
        }

        return readEntry(dataBuffer, offset);

/*
        int offset = 0;
        while (dataBuffer.remaining() > 0) {
            int keySize = dataBuffer.getInt();
            offset += Integer.BYTES;

            if (keySize != key.remaining()) {
                dataBuffer.position(offset + keySize);
                int valueSize = dataBuffer.getInt();
                dataBuffer.position(offset + valueSize);
                continue;
            }

            ByteBuffer curKey = dataBuffer.slice(offset, keySize);
            dataBuffer.position(offset + keySize);
            offset += keySize;

            int valueSize = dataBuffer.getInt();
            offset += Integer.BYTES;
            if (curKey.compareTo(key) == 0) {
                ByteBuffer curValue = dataBuffer.slice(offset, valueSize);
                return new BaseEntry<>(curKey, curValue);
            }
            dataBuffer.position(offset + valueSize);
            offset += valueSize;
        }
        return null;*/
    }



    private int getFreeIndex() {
        File[] files = config.basePath().toFile().listFiles();
        int counter = 1;
        Pattern pattern = Pattern.compile(DATA_FILE_NAME + "[0-9]+" + EXTENSION);
        for (File el : files) {
            if (pattern.matcher(el.getName()).matches()) {
                counter++;
            }
        }
        return counter;
    }

    private void store(SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        if (data == null) {
            return;
        }

        int index = getFreeIndex();
        Path pathToWriteData = config.basePath().resolve(DATA_FILE_NAME + index + EXTENSION);
        Path pathToWriteIndexes = config.basePath().resolve(INDEXES_FILE_NAME + index + EXTENSION);
        try (FileChannel dataChannel = FileChannel.open(pathToWriteData,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel indexChannel = FileChannel.open(pathToWriteIndexes,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING);) {

            ByteBuffer writeDataBuffer = mapChannelData(dataChannel);
            ByteBuffer writeIndexBuffer = mapChannelIndex(indexChannel);

            for (Entry<ByteBuffer> el : inMemoryData.values()) {
                writeIndexBuffer.putInt(writeDataBuffer.position());

                writeDataBuffer.putInt(el.key().remaining());
                writeDataBuffer.put(el.key());
                writeDataBuffer.putInt(el.value().remaining());
                writeDataBuffer.put(el.value());
            }
        }
    }

    private ByteBuffer mapChannelData(FileChannel channel) throws IOException {
        int size = 0;
        for (Entry<ByteBuffer> el : inMemoryData.values()) {
            size += el.key().remaining() + el.value().remaining();
        }
        size += 2 * inMemoryData.size() * Integer.BYTES;

        return channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
    }

    private ByteBuffer mapChannelIndex(FileChannel channel) throws IOException {
        return channel.map(FileChannel.MapMode.READ_WRITE, 0, inMemoryData.size() * Integer.BYTES);
    }
}
