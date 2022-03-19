package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DaoReader {
    private final MappedByteBuffer mapper;
    private final MappedByteBuffer indexMapper;

    private int position = -1;
    private int lastPosition = -1;

    public DaoReader(Path fileName, Path indexName) throws NoSuchFileException {
        MappedByteBuffer tempMapper;
        MappedByteBuffer tempIndexMapper;
        try (FileChannel reader = FileChannel.open(fileName, StandardOpenOption.READ);
             FileChannel indexReader = FileChannel.open(indexName, StandardOpenOption.READ)) {
            tempMapper = reader.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(fileName));
            tempIndexMapper = indexReader.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexName));
        } catch (IOException e) {
            if(e.getClass().equals(NoSuchFileException.class)) {
                throw (NoSuchFileException) e;
            }
            throw new UncheckedIOException(e);
        }
        this.mapper = tempMapper;
        this.indexMapper = tempIndexMapper;
    }

    public BaseEntry<ByteBuffer> binarySearch(ByteBuffer key) {
        int lowerBond = 0;
        int higherBond = indexMapper.getInt() - 1;
        lastPosition = indexMapper.position((higherBond + 1) * Integer.BYTES).getInt();
        int middle = higherBond / 2;

        while (lowerBond <= higherBond) {
            BaseEntry<ByteBuffer> result = getEntry(middle);
            if (result.value() == null) {
                return new BaseEntry<>(key, null);
            }
            if (key.compareTo(result.key()) > 0) {
                lowerBond = middle + 1;
            } else if (key.compareTo(result.key()) < 0) {
                higherBond = middle - 1;
            } else if (key.compareTo(result.key()) == 0) {
                return result;
            }
            middle = (lowerBond + higherBond) / 2;
        }
        return null;
    }

    public BaseEntry<ByteBuffer> nonPreciseBinarySearch(ByteBuffer key) {
        int lowerBond = 0;
        int higherBond = indexMapper.getInt() - 1;
        lastPosition = indexMapper.position((higherBond + 1) * Integer.BYTES).getInt();
        int middle = higherBond / 2;
        BaseEntry<ByteBuffer> result = null;
        while (lowerBond <= higherBond) {
            result = getEntry(middle);
            if (result.value() == null) {
                return result;
            } else if (key.compareTo(result.key()) > 0) {
                lowerBond = middle + 1;
            } else if (key.compareTo(result.key()) < 0) {
                higherBond = middle - 1;
            } else if (key.compareTo(result.key()) == 0) {
                return result;
            }
            middle = (lowerBond + higherBond) / 2;
        }
        if (result == null) {
            return null;
        }
        if (key.compareTo(result.key()) < 0) {
            return result;
        }
        return getNextEntry();
    }

    private BaseEntry<ByteBuffer> getEntry(int middle) {
        indexMapper.position((middle + 1) * Integer.BYTES);
        int curPosition = indexMapper.getInt();
        mapper.position(curPosition);
        int keyLen = mapper.getInt();
        int valLen = mapper.getInt();
        ByteBuffer key = mapper.slice(curPosition + Integer.BYTES * 2, keyLen);
        if (valLen == -1) {
            position = curPosition + Integer.BYTES * 2 + keyLen + valLen;
            return new BaseEntry<>(key, null);
        }
        ByteBuffer value = mapper.slice(curPosition + Integer.BYTES * 2 + keyLen, valLen);
        position = curPosition + Integer.BYTES * 2 + keyLen + valLen;
        return new BaseEntry<>(key, value);
    }

    public BaseEntry<ByteBuffer> getNextEntry() {
        if (position == -1) {
            throw new UnsupportedOperationException();
        }
        if (position > lastPosition) {
            return null;
        }
        mapper.position(position);
        int keyLen = mapper.getInt();
        int valLen = mapper.getInt();
        ByteBuffer key = mapper.slice(position + Integer.BYTES * 2, keyLen);
        if (valLen == -1) {
            position = position + Integer.BYTES * 2 + keyLen + valLen;
            return new BaseEntry<>(key, null);
        }
        ByteBuffer value = mapper.slice(position + Integer.BYTES * 2 + keyLen, valLen);
        position = position + Integer.BYTES * 2 + keyLen + valLen;
        return new BaseEntry<>(key, value);
    }

    public BaseEntry<ByteBuffer> getFirstEntry() {
        int size = indexMapper.getInt();
        lastPosition = indexMapper.position(size * Integer.BYTES).getInt();
        if (size != 0) {
            position = 0;
            return getNextEntry();
        }
        return null;
    }
}
