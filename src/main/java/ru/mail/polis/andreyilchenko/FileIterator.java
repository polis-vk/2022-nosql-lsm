package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private static final int NULL_OFFSET = -1;
    private final Path dataPath;
    private final Path offsetsPath;
    private final ByteBuffer rightDataBoundary;
    private final long maxOffsetsFilePointer;
    private BaseEntry<ByteBuffer> prevElem;
    private long offsetPointer;
    private boolean hasNotNext;

    public FileIterator(Path dataPath, Path offsetsPath, long startOffset,
                        long offsetsSize, ByteBuffer to) throws IOException {
        this.dataPath = dataPath;
        this.offsetsPath = offsetsPath;
        this.offsetPointer = startOffset;
        this.maxOffsetsFilePointer = offsetsSize;
        this.rightDataBoundary = to;
        if (startOffset >= offsetsSize) {
            hasNotNext = true;
            return;
        }
        try (
                RandomAccessFile dataReader = new RandomAccessFile(dataPath.toFile(), "r");
                FileChannel dataChannel = dataReader.getChannel();
                RandomAccessFile offsetsReader = new RandomAccessFile(offsetsPath.toFile(), "r")
        ) {
            boolean isNull = false;
            offsetsReader.seek(offsetPointer);
            moveOffsetsPointer();
            int keyStartOffset = offsetsReader.readInt();
            int valueStartOffset = offsetsReader.readInt();
            int valueEndOffset = offsetsReader.readInt();
            if (valueStartOffset == NULL_OFFSET) {
                valueStartOffset = valueEndOffset;
                isNull = true;
            }
            ByteBuffer probableKey = readByteBuffer(valueStartOffset, keyStartOffset, dataChannel);
            ByteBuffer value = readByteBuffer(valueEndOffset, valueStartOffset, dataChannel);
            prevElem = new BaseEntry<>(probableKey, isNull ? null : value);
        }
    }

    @Override
    public boolean hasNext() {
        boolean boundaryFlag = true;
        if (rightDataBoundary != null && !hasNotNext) {
            boundaryFlag = prevElem.key().compareTo(rightDataBoundary) < 0;
        }
        return !hasNotNext && boundaryFlag && maxOffsetsFilePointer >= offsetPointer;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        try (
                RandomAccessFile dataReader = new RandomAccessFile(dataPath.toFile(), "r");
                FileChannel dataChannel = dataReader.getChannel();
                RandomAccessFile offsetsReader = new RandomAccessFile(offsetsPath.toFile(), "r")
        ) {
            if (maxOffsetsFilePointer <= offsetPointer + 4) {
                moveOffsetsPointer();
                return prevElem;
            }
            offsetsReader.seek(offsetPointer);
            moveOffsetsPointer();
            boolean isValueNull = false;
            int keyStartOffset = offsetsReader.readInt();
            int valueStartOffset = offsetsReader.readInt();
            int valueEndOffset = offsetsReader.readInt();
            if (valueStartOffset == NULL_OFFSET) {
                isValueNull = true;
                valueStartOffset = valueEndOffset;
            }
            ByteBuffer probableKey = readByteBuffer(valueStartOffset, keyStartOffset, dataChannel);
            ByteBuffer value = readByteBuffer(valueEndOffset, valueStartOffset, dataChannel);
            BaseEntry<ByteBuffer> returnElem = prevElem;
            prevElem = new BaseEntry<>(probableKey, isValueNull ? null : value);
            return returnElem;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void moveOffsetsPointer() {
        offsetPointer += 8;
    }

    private ByteBuffer readByteBuffer(int start, int end, FileChannel dataChannel) throws IOException {
        ByteBuffer probableKey = ByteBuffer.allocate(start - end);
        dataChannel.read(probableKey, end);
        probableKey.flip();
        return probableKey;
    }
}
