package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public class Utils {

    public static class mappedBufferedPair {
        private ByteBuffer dataBuffer;
        private ByteBuffer indexBuffer;

        public mappedBufferedPair(ByteBuffer dataBuffer, ByteBuffer indexBuffer) {
            this.dataBuffer = dataBuffer;
            this.indexBuffer = indexBuffer;
        }

        public void setDataBuffer(ByteBuffer dataBuffer) {
            this.dataBuffer = dataBuffer;
        }

        public void setIndexBuffer(ByteBuffer indexBuffer) {
            this.indexBuffer = indexBuffer;
        }

        public ByteBuffer getDataBuffer() {
            return dataBuffer;
        }

        public ByteBuffer getIndexBuffer() {
            return indexBuffer;
        }
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
