package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class Utils {

    private Utils() {
    }

    public static Entry<String> findCeilEntry(RandomAccessFile raf, String key, Path indexPath) throws IOException {
        Entry<String> nextEntry = null;
        try (RandomAccessFile index = new RandomAccessFile(indexPath.toString(), "r")) {
            long lastPos = -1;
            raf.seek(0);
            int size = raf.readInt();
            long left = -1;
            long right = size;
            long mid;
            while (left < right - 1) {
                mid = left + (right - left) / 2;
                index.seek(mid * Long.BYTES);
                long entryPos = index.readLong();
                raf.seek(entryPos);
                raf.readByte(); //read tombstone
                String currentKey = raf.readUTF();
                int keyComparing = currentKey.compareTo(key);
                if (keyComparing == 0) {
                    lastPos = entryPos;
                    break;
                } else if (keyComparing > 0) {
                    lastPos = entryPos;
                    right = mid;
                } else {
                    left = mid;
                }
            }
            if (lastPos != -1) {
                raf.seek(lastPos);
                nextEntry = readEntry(raf);
            }

        }
        return nextEntry;
    }

    public static Entry<String> readEntry(RandomAccessFile raf) throws IOException {
        byte tombstone = raf.readByte();
        String key = raf.readUTF();
        String value = null;
        if (tombstone == 1) {
            value = raf.readUTF();
        } else if (tombstone == 2) {
            int valueSize = raf.readInt();
            byte[] valueBytes = new byte[valueSize];
            raf.read(valueBytes);
            value = new String(valueBytes, StandardCharsets.UTF_8);
        }
        return new BaseEntry<>(key, value);
    }

    public static void writeEntry(RandomAccessFile output, Entry<String> entry) throws IOException {
        String val = entry.value();
        if (val == null) {
            output.writeByte(-1);
            output.writeUTF(entry.key());
        } else {
            if (val.length() < 65536) {
                output.writeByte(1);
                output.writeUTF(entry.key());
                output.writeUTF(val);
            } else {
                output.writeByte(2);
                output.writeUTF(entry.key());
                byte[] b = val.getBytes(StandardCharsets.UTF_8);
                output.writeInt(b.length);
                output.write(b);
            }
        }
    }
}
