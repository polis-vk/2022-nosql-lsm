package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final int NULL_OFFSET = -1;
    private static final int MINIMAL_PRIORITY = Integer.MIN_VALUE;
    private static final String DATA_EXTENSION = ".d";
    private static final String OFFSETS_EXTENSION = ".o";
    private static final int DEFAULT_ALLOCATE_BUFFER_WRITE_SIZE = 0xA00;

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entries = new ConcurrentSkipListMap<>();
    private final Path pathToData;
    private final Path pathToOffsets;
    private final int allocateBufferWriteSize;
    private final List<Path> paths;

    public PersistentDao(Config config) throws IOException {
        this(config, DEFAULT_ALLOCATE_BUFFER_WRITE_SIZE);
    }

    public PersistentDao(Config config, int allocateBufferWriteSize) throws IOException {
        this.allocateBufferWriteSize = allocateBufferWriteSize;
        Path configPath = config.basePath();
        Stream<Path> pathStream = Files.walk(configPath);
        paths = pathStream
                .filter(Files::isRegularFile)
                .map(x -> Path.of(x.toString().replaceFirst("[.][^.]+$", ""))) // delete extension
                .sorted()
                .toList();
        pathStream.close();
        pathToData = configPath.resolve(paths.size() + DATA_EXTENSION);
        pathToOffsets = configPath.resolve(paths.size() + OFFSETS_EXTENSION);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        List<PeekingPriorityIterator> iterators = new ArrayList<>();
        iterators.add(new PeekingPriorityIterator(inMemoryGet(from, to), MINIMAL_PRIORITY));
        for (int i = 0; i < paths.size(); i += 2) {
            iterators.add(
                    new PeekingPriorityIterator(
                            new FileIterator(
                                    Path.of(paths.get(i) + ".d"),
                                    Path.of(paths.get(i) + ".o"),
                                    from == null ? 0 : findPositionInFile(paths.get(i), from),
                                    Path.of(paths.get(i) + ".o").toFile().length(),
                                    to),
                            -Integer.parseInt(paths.get(i).getFileName().toString())
                    )
            );
        }
        return new MergedIterator(iterators);
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> entry = entries.get(key);
        if (entry == null) {
            for (int i = paths.size() - 1; i > 0; i -= 2) {
                BaseEntry<ByteBuffer> inFileEntry = findEntryInFile(paths.get(i), key);
                if (inFileEntry != null) {
                    return inFileEntry.value() == null ? null : inFileEntry;
                }
            }
            return null;
        }
        return entry.value() == null ? null : entry;
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entries.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        ByteBuffer buf = ByteBuffer.allocate(2 * Integer.BYTES * entries.size() + Integer.BYTES);
        int pos = 0;
        for (BaseEntry<ByteBuffer> entry : entries.values()) {
            buf.putInt(pos);
            pos += entry.key().remaining();
            if (entry.value() == null) {
                buf.putInt(NULL_OFFSET);
            } else {
                buf.putInt(pos);
            }
            pos += entry.value() == null ? 0 : entry.value().remaining();

        }
        buf.putInt(pos);
        buf.flip();
        try (
                RandomAccessFile dataAccessFile = new RandomAccessFile(pathToData.toFile(), "rw");
                FileChannel dataChannel = dataAccessFile.getChannel();
                FileOutputStream offsetsOutputStream = new FileOutputStream(pathToOffsets.toFile());
                FileChannel offsetsChannel = offsetsOutputStream.getChannel()
        ) {
            offsetsChannel.write(buf);
            ByteBuffer bufferToWrite = ByteBuffer.allocate(allocateBufferWriteSize);
            for (BaseEntry<ByteBuffer> entry : entries.values()) {
                int keyLen = entry.key().remaining();
                int valueLen = entry.value() == null ? 0 : entry.value().remaining();
                if (bufferToWrite.remaining() + keyLen + valueLen >= allocateBufferWriteSize) {
                    dataChannel.write(bufferToWrite.flip());
                    bufferToWrite.clear();
                }
                bufferToWrite.put(entry.key()).put(entry.value() == null ? ByteBuffer.allocate(0) : entry.value());
            }
            dataChannel.write(bufferToWrite.flip());
        }
    }

    private Iterator<BaseEntry<ByteBuffer>> inMemoryGet(ByteBuffer from, ByteBuffer to) {
        if (to == null && from == null) {
            return entries.values().iterator();
        }
        if (to == null) {
            return entries.tailMap(from).values().iterator();
        }
        if (from == null) {
            return entries.headMap(to).values().iterator();
        }
        return entries.subMap(from, to).values().iterator();
    }

    private long findPositionInFile(Path pathForFiles, ByteBuffer key) throws IOException {
        Path pathDat = Path.of(pathForFiles + ".d");
        Path pathOffset = Path.of(pathForFiles + ".o");
        long startIndex;
        long endIndex;
        try (
                RandomAccessFile dataReader = new RandomAccessFile(pathDat.toFile(), "r");
                FileChannel dataChannel = dataReader.getChannel();
                RandomAccessFile offsetsReader = new RandomAccessFile(pathOffset.toFile(), "r")
        ) {
            offsetsReader.seek(offsetsReader.length() - 12);
            int keyStartOffset = offsetsReader.readInt();
            int valueStartOffset = offsetsReader.readInt();
            int valueEndOffset = offsetsReader.readInt();
            if (valueStartOffset == NULL_OFFSET) {
                valueStartOffset = valueEndOffset;
            }
            ByteBuffer probableKey = readByteBuffer(valueStartOffset, keyStartOffset, dataChannel);
            if (key.compareTo(probableKey) > 0) {
                return Integer.MAX_VALUE;
            }
            startIndex = 0;
            endIndex = (offsetsReader.length() - 8) / 8;
            long midIndex;

            while (startIndex <= endIndex) {
                midIndex = startIndex + (endIndex - startIndex) / 2;
                offsetsReader.seek(midIndex * 8);
                keyStartOffset = offsetsReader.readInt();
                valueStartOffset = offsetsReader.readInt();
                valueEndOffset = offsetsReader.readInt();
                if (valueStartOffset == NULL_OFFSET) {
                    valueStartOffset = valueEndOffset;
                }
                probableKey = ByteBuffer.allocate(valueStartOffset - keyStartOffset);
                dataChannel.read(probableKey, keyStartOffset);
                probableKey.flip();
                int compareResult = probableKey.compareTo(key);
                if (compareResult == 0) {
                    return midIndex * 8;
                } else if (compareResult > 0) {
                    endIndex = midIndex - 1;
                } else {
                    startIndex = midIndex + 1;
                }
            }
        }
        return (endIndex + 1) * 8;
    }

    private BaseEntry<ByteBuffer> findEntryInFile(Path pathForFiles, ByteBuffer key) throws IOException {
        Path pathDat = Path.of(pathForFiles + ".d");
        Path pathOffset = Path.of(pathForFiles + ".o");
        try (
                RandomAccessFile dataReader = new RandomAccessFile(pathDat.toFile(), "r");
                FileChannel dataChannel = dataReader.getChannel();
                RandomAccessFile offsetsReader = new RandomAccessFile(pathOffset.toFile(), "r")
        ) {
            long startIndex = 0;
            long endIndex = (offsetsReader.length() - 8) / 8;
            long midIndex;

            while (startIndex <= endIndex) {
                midIndex = startIndex + (endIndex - startIndex) / 2;
                offsetsReader.seek(midIndex * 8);
                boolean isValueNull = false;
                int keyStartOffset = offsetsReader.readInt();
                int valueStartOffset = offsetsReader.readInt();
                int valueEndOffset = offsetsReader.readInt();
                if (valueStartOffset == NULL_OFFSET) {
                    valueStartOffset = valueEndOffset;
                    isValueNull = true;
                }
                ByteBuffer probableKey = readByteBuffer(valueStartOffset, keyStartOffset, dataChannel);
                int compareResult = probableKey.compareTo(key);
                if (compareResult == 0) {
                    ByteBuffer value = readByteBuffer(valueEndOffset, valueStartOffset, dataChannel);
                    return new BaseEntry<>(key, isValueNull ? null : value);
                } else if (compareResult > 0) {
                    endIndex = midIndex - 1;
                } else {
                    startIndex = midIndex + 1;
                }
            }
        }
        return null;
    }

    private ByteBuffer readByteBuffer(int start, int end, FileChannel dataChannel) throws IOException {
        ByteBuffer probableKey = ByteBuffer.allocate(start - end);
        dataChannel.read(probableKey, end);
        probableKey.flip();
        return probableKey;
    }
}
