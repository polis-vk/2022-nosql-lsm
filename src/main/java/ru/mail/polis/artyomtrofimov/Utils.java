package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public class Utils {

    private Utils() {
    }

    public static Entry<String> findCeilEntry(RandomAccessFile raf, String key, Path indexPath) throws IOException {
        Entry<String> nextEntry = null;
        try (RandomAccessFile index = new RandomAccessFile(indexPath.toString(), "r")) {
            long lastPos = 0;
            raf.seek(0);
            int size = raf.readInt();
            long left = -1;
            long right = size;
            long mid;
            String minKey = null;
            String minValue = null;
            while (left < right - 1) {
                mid = left + (right - left) / 2;
                index.seek(mid * Long.BYTES);
                raf.seek(index.readLong());
                byte tombstone = raf.readByte();
                String currentKey = raf.readUTF();
                String currentValue = tombstone < 0 ? null : raf.readUTF();
                int keyComparing = currentKey.compareTo(key);
                if (keyComparing == 0) {
                    lastPos = raf.getFilePointer();
                    minKey = currentKey;
                    minValue = currentValue;
                    break;
                } else if (keyComparing > 0) {
                    lastPos = raf.getFilePointer();
                    minKey = currentKey;
                    minValue = currentValue;
                    right = mid;
                } else {
                    left = mid;
                }
            }
            if (minKey != null) {
                nextEntry = new BaseEntry<>(minKey, minValue);
            }
            raf.seek(lastPos);
        } catch (FileNotFoundException | EOFException e) {
            return null;
        }
        return nextEntry;
    }
}
