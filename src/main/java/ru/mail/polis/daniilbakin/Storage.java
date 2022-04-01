package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class Storage implements Closeable {

    public static final String DATA_FILE_NAME = "myData";
    public static final String INDEX_FILE_NAME = "indexes";
    public static final String COMPACT_TMP_PREFIX = "tmpCompact_";

    private int numOfFiles;
    private int startIndexFile;
    private MapsDeserializeStream deserialize;
    private final Config config;

    protected Storage(Config config) throws IOException {
        this.config = config;
        this.numOfFiles = calcCountOfIndexFiles();
        this.startIndexFile = calcMinIndexOfFiles();
        deserialize = new MapsDeserializeStream(config, numOfFiles, startIndexFile);
    }

    public void compact(PeekIterator<BaseEntry<ByteBuffer>> inMemoryIterator) throws IOException {
        List<PeekIterator<BaseEntry<ByteBuffer>>> iterators = getFileIterators(null, null);
        iterators.add(inMemoryIterator);
        MergeIterator<ByteBuffer> mergeIterator = new MergeIterator<>(iterators);
        if (!mergeIterator.hasNext()) {
            return;
        }
        MapSerializeStream writer = new MapSerializeStream(config, COMPACT_TMP_PREFIX);
        writer.serializeData(mergeIterator);
        writer.close();

        Path basePath = config.basePath();
        int newFileIndex = (startIndexFile == -1) ? 0 : startIndexFile + numOfFiles;
        Files.move(basePath.resolve(COMPACT_TMP_PREFIX + DATA_FILE_NAME),
                basePath.resolve(DATA_FILE_NAME + newFileIndex));
        Files.move(basePath.resolve(COMPACT_TMP_PREFIX + INDEX_FILE_NAME),
                basePath.resolve(INDEX_FILE_NAME + newFileIndex));
        if (startIndexFile == -1) {
            startIndexFile = 0;
            numOfFiles = 1;
            deserialize = new MapsDeserializeStream(config, numOfFiles, startIndexFile);
            return;
        }
        deserialize.close();
        for (int i = startIndexFile; i < startIndexFile + numOfFiles; i++) {
            Files.delete(basePath.resolve(INDEX_FILE_NAME + i));
            Files.delete(basePath.resolve(DATA_FILE_NAME + i));
        }

        Files.copy(basePath.resolve(DATA_FILE_NAME + newFileIndex), basePath.resolve(DATA_FILE_NAME + 0));
        Files.move(basePath.resolve(INDEX_FILE_NAME + newFileIndex), basePath.resolve(INDEX_FILE_NAME + 0));
        Files.delete(basePath.resolve(DATA_FILE_NAME + newFileIndex));
        startIndexFile = 0;
        numOfFiles = 1;
        deserialize = new MapsDeserializeStream(config, numOfFiles, startIndexFile);
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
        MapSerializeStream writer = new MapSerializeStream(config, numOfFiles, startIndexFile);
        writer.serializeData(data.values().iterator());
        writer.close();
    }

    private int calcCountOfIndexFiles() throws IOException {
        try (Stream<Path> files = Files.list(config.basePath())
                .filter(it -> it.getFileName().toString().startsWith(INDEX_FILE_NAME))) {
            return (int) files.count();
        } catch (NoSuchFileException e) {
            return 0;
        }
    }

    private int calcMinIndexOfFiles() throws IOException {
        try (Stream<Path> files = Files.list(config.basePath())
                .filter(it -> it.getFileName().toString().startsWith(INDEX_FILE_NAME))) {
            Path minFile = files.min((o1, o2) -> {
                int index1 =
                        Integer.parseInt(o1.getFileName().toString().replaceFirst(INDEX_FILE_NAME, ""));
                int index2 =
                        Integer.parseInt(o2.getFileName().toString().replaceFirst(INDEX_FILE_NAME, ""));
                return Integer.compare(index1, index2);
            }).get().getFileName();
            return Integer.parseInt(minFile.toString().replaceFirst(INDEX_FILE_NAME, ""));
        } catch (NoSuchElementException e) {
            return -1;
        }
    }

}
