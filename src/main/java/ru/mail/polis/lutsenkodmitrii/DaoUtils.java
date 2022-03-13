package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class DaoUtils {

    private DaoUtils(){
    }

    public static BaseEntry<String> readEntry(BufferedReader bufferedReader) throws IOException {
        bufferedReader.skip(Integer.BYTES);
        int keyLength = readUnsignedInt(bufferedReader);
        if (keyLength == -1) {
            return null;
        }
        char[] keyChars = new char[keyLength];
        bufferedReader.read(keyChars);
        return new BaseEntry<>(new String(keyChars), bufferedReader.readLine());
    }

    public static String readKey(BufferedReader bufferedReader) throws IOException {
        char[] keyChars;
        int keyLength;
        keyLength = readUnsignedInt(bufferedReader);
        keyChars = new char[keyLength];
        bufferedReader.read(keyChars);
        return new String(keyChars);
    }

    public static char[] intToCharArray(int k) {
        char[] writeBuffer = new char[Integer.BYTES];
        writeBuffer[0] = (char) (k >>> 24);
        writeBuffer[1] = (char) (k >>> 16);
        writeBuffer[2] = (char) (k >>> 8);
        writeBuffer[3] = (char) (k >>> 0);
        return writeBuffer;
    }

    public static int readUnsignedInt(BufferedReader bufferedReader) throws IOException {
        int ch1 = bufferedReader.read();
        int ch2 = bufferedReader.read();
        int ch3 = bufferedReader.read();
        int ch4 = bufferedReader.read();
        if (ch1 == -1 || ch2 == -1 || ch3 == -1 || ch4 == -1) {
            return -1;
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public static BaseEntry<String> ceilKey(Path path, BufferedReader bufferedReader, String key) throws IOException {
        int keyLength;
        int prevEntryLength;
        char[] keyChars;
        String currentKey;
        long left = 0;
        long right = Files.size(path) - Integer.BYTES;
        long mid;
        while (left <= right) {
            mid = (left + right) / 2;
            bufferedReader.mark((int) right);
            bufferedReader.skip(mid - left);
            int readBytes = bufferedReader.readLine().length();
            prevEntryLength = readUnsignedInt(bufferedReader);
            if (mid + prevEntryLength >= right) {
                bufferedReader.reset();
                bufferedReader.skip(Integer.BYTES);
                right = mid - prevEntryLength + readBytes + 1;
            }
            keyLength = readUnsignedInt(bufferedReader);
            keyChars = new char[keyLength];
            bufferedReader.read(keyChars);
            currentKey = new String(keyChars);
            String currentValue = bufferedReader.readLine();
            int compareResult = key.compareTo(currentKey);
            if (compareResult == 0) {
                return new BaseEntry<>(currentKey, currentValue);
            }
            if (compareResult > 0) {
                bufferedReader.mark(0);
                bufferedReader.skip(Integer.BYTES);
                keyLength = readUnsignedInt(bufferedReader);
                if (keyLength == -1) {
                    return null;
                }
                keyChars = new char[keyLength];
                bufferedReader.read(keyChars);
                String nextKey = new String(keyChars);
                if (key.compareTo(nextKey) <= 0) {
                    return new BaseEntry<>(nextKey, bufferedReader.readLine());
                }
                left = mid - prevEntryLength + readBytes;
                bufferedReader.reset();
            } else {
                right = mid + readBytes;
                bufferedReader.reset();
            }
        }
        return null;
    }

    public static BaseEntry<String> searchInFile(Path path, String key) throws IOException {
        try (BufferedReader bufferedReader = Files.newBufferedReader(path, UTF_8)) {
            String fileMinKey = readKey(bufferedReader);
            String fileMaxKey = readKey(bufferedReader);
            if (key.compareTo(fileMinKey) < 0 || key.compareTo(fileMaxKey) > 0) {
                return null;
            }
            int keyLength;
            int prevEntryLength;
            char[] keyChars;
            String currentKey;
            long left = 0;
            long right = Files.size(path) - Integer.BYTES;
            long mid;
            while (left <= right) {
                mid = (left + right) / 2;
                bufferedReader.mark((int) right);
                bufferedReader.skip(mid - left);
                int readBytes = bufferedReader.readLine().length();
                prevEntryLength = readUnsignedInt(bufferedReader);
                if (mid + prevEntryLength >= right) {
                    bufferedReader.reset();
                    bufferedReader.skip(Integer.BYTES);
                    right = mid - prevEntryLength + readBytes + 1;
                }
                keyLength = readUnsignedInt(bufferedReader);
                keyChars = new char[keyLength];
                bufferedReader.read(keyChars);
                currentKey = new String(keyChars);
                int compareResult = key.compareTo(currentKey);
                if (compareResult > 0) {
                    left = mid - prevEntryLength + readBytes;
                } else if (compareResult < 0) {
                    right = mid + readBytes;
                    bufferedReader.reset();
                } else {
                    return new BaseEntry<>(currentKey, bufferedReader.readLine());
                }
            }
            return null;
        } catch (NoSuchFileException e) {
            return null;
        }
    }
}
