package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;

import static ru.mail.polis.artyomscheredin.Utils.findOffset;
import static ru.mail.polis.artyomscheredin.Utils.readEntry;

public class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    ByteBuffer dataBuffer;
    ByteBuffer indexBuffer;
    int lowerBound;
    int upperBound;
    int pointer = 0;

    public FileIterator(Utils.Pair<Path> paths, ByteBuffer from, ByteBuffer to) throws IOException {
        try (FileChannel dataChannel = FileChannel.open(paths.dataPath());
             FileChannel indexChannel = FileChannel.open(paths.indexPath())) {
            indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            dataBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
        }

        lowerBound = findOffset(indexBuffer, dataBuffer, from, Utils.Strategy.ACCURATE_KEY_NON_REQUIRED);
        upperBound = findOffset(indexBuffer, dataBuffer, to, Utils.Strategy.ACCURATE_KEY_NON_REQUIRED);
        pointer = indexBuffer.getInt();
    }


    @Override
    public boolean hasNext() {
        return pointer != upperBound;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        BaseEntry<ByteBuffer> temp = readEntry(dataBuffer, pointer);
        pointer = indexBuffer.getInt();
        return temp;
    }
}
