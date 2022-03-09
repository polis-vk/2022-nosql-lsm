package ru.mail.polis.dmitrykondraev;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

class LongArrayOutput implements Closeable {

    private final DataOutputStream out;

    public LongArrayOutput(OutputStream out) {
        this.out = new DataOutputStream(new BufferedOutputStream(out));
    }

    public static LongArrayOutput of(Path file) throws IOException {
        return new LongArrayOutput(Files.newOutputStream(file));
    }

    // write array to stream in format:
    // ┌─────────┬─────────────────┐
    // │size: int│array: long[size]│
    // └─────────┴─────────────────┘
    public void write(long[] array) throws IOException {
        out.writeInt(array.length);
        for (long offset : array) {
            out.writeLong(offset);
        }
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
