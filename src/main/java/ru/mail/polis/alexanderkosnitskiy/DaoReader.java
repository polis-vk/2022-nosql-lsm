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
        mapper.position(0);
        indexMapper.position(0);
        int lowerBond = 0;
        int higherBond = indexMapper.getInt() - 1;
        lastPosition = indexMapper.position((higherBond + 1) * Integer.BYTES).getInt();
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
        mapper.position(0);
        indexMapper.position(0);
        int lowerBond = 0;
        int higherBond = indexMapper.getInt() - 1;
        lastPosition = indexMapper.position((higherBond + 1) * Integer.BYTES).getInt();
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
        indexMapper.position((middle + 1) * Integer.BYTES);
        int curPosition = indexMapper.getInt();
        mapper.position(curPosition);
        int keyLen = mapper.getInt();
        int valLen = mapper.getInt();
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
        mapper.position(position);
        int keyLen = mapper.getInt();
        int valLen = mapper.getInt();
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
        mapper.position(0);
        indexMapper.position(0);
        int size = indexMapper.getInt();
        lastPosition = indexMapper.position(size * Integer.BYTES).getInt();
        if (size != 0) {
            position = 0;
            return getNextEntry();
        }
        return null;
    }
}
