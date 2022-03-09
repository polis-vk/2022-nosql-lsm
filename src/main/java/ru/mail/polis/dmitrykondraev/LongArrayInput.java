package ru.mail.polis.dmitrykondraev;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

class LongArrayInput implements Closeable {
    private final DataInputStream in;

    private LongArrayInput(InputStream in) {
        this.in = new DataInputStream(new BufferedInputStream(in));
    }

    public static LongArrayInput of(Path file) throws IOException {
        return new LongArrayInput(Files.newInputStream(file));
    }

    // read array from stream in format:
    // ┌─────────┬─────────────────┐
    // │size: int│array: long[size]│
    // └─────────┴─────────────────┘
    public long[] readLongArray() throws IOException {
        int size = in.readInt();
        long[] result = new long[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = in.readLong();
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
