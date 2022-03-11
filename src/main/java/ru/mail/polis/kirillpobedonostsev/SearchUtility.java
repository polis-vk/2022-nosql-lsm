package ru.mail.polis.kirillpobedonostsev;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class SearchUtility {
    private SearchUtility() {
    }

    public static int floorOffset(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage, ByteBuffer to) {
        int low = 0;
        int high = readIndexPage.capacity() / Integer.BYTES - 1;
        boolean isLower = false;
        int offset = 0;
        int mid;
        while (low <= high) {
            mid = (high + low) / 2;
            readIndexPage.position(mid * Integer.BYTES);
            offset = readIndexPage.getInt();
            readDataPage.position(offset);
            int keySize = readDataPage.getInt();
            if (keySize != to.remaining()) {
                continue;
            }
            ByteBuffer readKey = readDataPage.slice(readDataPage.position(), keySize);
            int compareResult = readKey.compareTo(to);
            if (compareResult >= 0) {
                high = mid - 1;
                isLower = false;
            } else {
                low = mid + 1;
                isLower = true;
            }
        }
        if (isLower) {
            readIndexPage.position(high * Integer.BYTES);
            offset = readIndexPage.getInt();
        }
        return offset;
    }

    public static int ceilOffset(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage, ByteBuffer from) {
        int low = 0;
        int high = readIndexPage.capacity() / Integer.BYTES - 1;
        boolean isLower = false;
        int offset = 0;
        int mid = 0;
        while (low <= high) {
            mid = (high + low) / 2;
            readIndexPage.position(mid * Integer.BYTES);
            offset = readIndexPage.getInt();
            readDataPage.position(offset);
            int keySize = readDataPage.getInt();
            if (keySize != from.remaining()) {
                continue;
            }
            ByteBuffer readKey = readDataPage.slice(readDataPage.position(), keySize);
            int compareResult = readKey.compareTo(from);
            if (compareResult > 0) {
                high = mid - 1;
                isLower = false;
            } else if (compareResult < 0) {
                low = mid + 1;
                isLower = true;
            } else {
                return offset;
            }
        }
        if (!isLower) {
            readIndexPage.position(mid * Integer.BYTES);
            offset = readIndexPage.getInt();
        }
        return offset;
    }

    public static ByteBuffer searchInFile(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage,
                                          ByteBuffer key) {
        int low = 0;
        int high = readIndexPage.capacity() / Integer.BYTES - 1;

        while (low <= high) {
            int mid = (high + low) / 2;
            readIndexPage.position(mid * Integer.BYTES);
            int offset = readIndexPage.getInt();
            readDataPage.position(offset);
            int keySize = readDataPage.getInt();
            if (keySize != key.remaining()) {
                continue;
            }
            ByteBuffer readKey = readDataPage.slice(readDataPage.position(), keySize);
            int compareResult = readKey.compareTo(key);
            if (compareResult > 0) {
                high = mid - 1;
            } else if (compareResult < 0) {
                low = mid + 1;
            } else {
                readDataPage.position(readDataPage.position() + keySize);
                int valueSize = readDataPage.getInt();
                ByteBuffer value = readDataPage.slice(readDataPage.position(), valueSize);
                readDataPage.position(readKey.position() + valueSize);
                return value;
            }
        }
        return null;
    }
}
