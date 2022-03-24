package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class Reader {

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data;
    private final NavigableMap<Integer, Entry<Path>> pathsToPairedFiles;

    public Reader(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data,
                  NavigableMap<Integer, Entry<Path>> pathsToPairedFiles) {
        this.data = data;
        this.pathsToPairedFiles = pathsToPairedFiles.descendingMap();
    }

    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to, List<ByteBuffer> tombstones) throws IOException {
        return new MergeIterator(from, to, data, pathsToPairedFiles, tombstones);
    }

    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        Entry<ByteBuffer> result = findKeyInStorage(key);
        return result == null ? findKeyInFile(key) : result;
    }

    private Entry<ByteBuffer> findKeyInStorage(ByteBuffer key) {
        return data.get(key);
    }

    private Entry<ByteBuffer> findKeyInFile(ByteBuffer key) throws IOException {
        Entry<ByteBuffer> result = null;
        for (Map.Entry<Integer, Entry<Path>> pathToPairedFiles: pathsToPairedFiles.entrySet()) {
            Path pathToDataFile = pathToPairedFiles.getValue().key();
            Path pathToIndexesFile = pathToPairedFiles.getValue().value();
            try (FileIterator fileIterator = new FileIterator(pathToDataFile, pathToIndexesFile, key, null)) {
                if (!fileIterator.hasNext()) {
                    continue;
                }
                result = fileIterator.next();
                if (Utils.isTombstone(result)) {
                    return null;
                }
                if (result != null && result.key().equals(key)) {
                    return result;
                }
            }
        }
        return result;
    }

}
