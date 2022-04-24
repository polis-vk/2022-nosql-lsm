package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class DaoReader {
    private final MappedByteBuffer mapper;
    private final MappedByteBuffer indexMapper;

    private int position = -1;
    private int lastPosition = -1;

    public DaoReader(MappedByteBuffer mapper, MappedByteBuffer indexMapper) {
        this.mapper = mapper;
        this.indexMapper = indexMapper;
    }

    public BaseEntry<ByteBuffer> binarySearch(ByteBuffer key) {
        int lowerBond = 0;
        int higherBond = indexMapper.getInt(0) - 1;
        lastPosition = indexMapper.getInt((higherBond + 1) * Integer.BYTES);
        int middle = higherBond / 2;

        while (lowerBond <= higherBond) {
            BaseEntry<ByteBuffer> result = getEntry(middle);
            int comparison = key.compareTo(result.key());
            if (comparison > 0) {
                lowerBond = middle + 1;
            } else if (comparison < 0) {
                higherBond = middle - 1;
            } else {
                return result;
            }
            middle = (lowerBond + higherBond) / 2;
        }
        return null;
    }

    public BaseEntry<ByteBuffer> nonPreciseBinarySearch(ByteBuffer key) {
        int lowerBond = 0;
        int higherBond = indexMapper.getInt(0) - 1;
        lastPosition = indexMapper.getInt((higherBond + 1) * Integer.BYTES);
        int middle = higherBond / 2;
        BaseEntry<ByteBuffer> result = null;
        while (lowerBond <= higherBond) {
            result = getEntry(middle);
            int comparison = key.compareTo(result.key());
            if (comparison > 0) {
                lowerBond = middle + 1;
            } else if (comparison < 0) {
                higherBond = middle - 1;
            } else {
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
        int curPosition = indexMapper.getInt((middle + 1) * Integer.BYTES);
        int keyLen = mapper.getInt(curPosition);
        int valLen = mapper.getInt(curPosition + Integer.BYTES);
        int newIndex = curPosition + Integer.BYTES * 2;
        ByteBuffer key = mapper.slice(newIndex, keyLen);
        newIndex += keyLen;
        if (valLen == -1) {
            position = newIndex;
            return new BaseEntry<>(key, null);
        }
        ByteBuffer value = mapper.slice(newIndex, valLen);
        position = newIndex + valLen;
        return new BaseEntry<>(key, value);
    }

    public BaseEntry<ByteBuffer> getNextEntry() {
        if (position == -1) {
            throw new UnsupportedOperationException();
        }
        if (position > lastPosition) {
            return null;
        }
        int keyLen = mapper.getInt(position);
        int valLen = mapper.getInt(position + Integer.BYTES);
        int newIndex = position + Integer.BYTES * 2;
        ByteBuffer key = mapper.slice(newIndex, keyLen);
        newIndex += keyLen;
        if (valLen == -1) {
            position = newIndex;
            return new BaseEntry<>(key, null);
        }
        ByteBuffer value = mapper.slice(newIndex, valLen);
        position = newIndex + valLen;
        return new BaseEntry<>(key, value);
    }

    public BaseEntry<ByteBuffer> getFirstEntry() {
        int size = indexMapper.getInt(0);
        lastPosition = indexMapper.getInt(size * Integer.BYTES);
        if (size != 0) {
            position = 0;
            return getNextEntry();
        }
        return null;
    }
}
