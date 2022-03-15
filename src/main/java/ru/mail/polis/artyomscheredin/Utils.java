package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;

public class Utils {

    public record Pair<E>(E dataPath, E indexPath) {
    }

    enum Strategy {ACCURATE_KEY_REQUIRED, ACCURATE_KEY_NON_REQUIRED}

    public static int findOffset(ByteBuffer indexBuffer, ByteBuffer dataBuffer, ByteBuffer key, Strategy isAccurateKeyRequired) {
        int offset = 0;
        int low = 0;
        int high = indexBuffer.remaining() / Integer.BYTES - 1;
        while (low <= high) {
            int mid = low + ((high - low) / 2);
            offset = indexBuffer.getInt(mid * Integer.BYTES);
            int keySize = dataBuffer.getInt(offset);

            ByteBuffer curKey = dataBuffer.slice(offset + Integer.BYTES,  keySize);
            if (curKey.compareTo(key) < 0) {
                low = mid + 1;
            } else if (curKey.compareTo(key) > 0) {
                high = mid - 1;
            } else if (curKey.compareTo(key) == 0) {
                return offset;
            }
        }
        indexBuffer.rewind();
        dataBuffer.rewind();
        return (isAccurateKeyRequired == Strategy.ACCURATE_KEY_REQUIRED) ? -1 : offset;
    }

    public static BaseEntry<ByteBuffer> readEntry(ByteBuffer dataBuffer, int offset) {
        int keySize = dataBuffer.getInt(offset);
        offset += Integer.BYTES;
        ByteBuffer curKey = dataBuffer.slice(offset, keySize);
        offset += keySize;
        int valueSize = dataBuffer.getInt(offset);
        offset += Integer.BYTES;
        ByteBuffer curValue = dataBuffer.slice(offset, valueSize);
        return new BaseEntry<ByteBuffer>(curKey, curValue);
    }
}
