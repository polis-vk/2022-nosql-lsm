package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;

public class Utils {

    public record Pair<E>(E dataPath, E indexPath) {
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
