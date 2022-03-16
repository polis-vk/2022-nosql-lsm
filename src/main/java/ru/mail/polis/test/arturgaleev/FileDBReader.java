package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class FileDBReader implements AutoCloseable {

    private final RandomAccessFile reader;
    private int size;
    private final FileChannel channel;
    private MappedByteBuffer pageData;
    private MappedByteBuffer pageLinks;
    private MappedByteBuffer page;


    public FileDBReader(Path path) throws IOException {
        reader = new RandomAccessFile(path.toFile(), "r");
        channel = reader.getChannel();

    }

    private void unmap(MappedByteBuffer nmap) throws IOException {
        if (nmap == null) {
            return;
        }
    }

    public void open() throws IOException {
        long fileSize = channel.size();
        page = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        page.load();
        this.size = page.getInt(0);

        pageData = page.slice(Integer.BYTES * (1 + this.size), page.limit() - Integer.BYTES * (1 + this.size));
        pageLinks = page.slice(Integer.BYTES, Integer.BYTES * this.size);
    }

    protected BaseEntry<ByteBuffer> readEntry() throws IOException {
        int keyLength = pageData.getInt();
        int valueLength = pageData.getInt();
        return new BaseEntry<>(pageData.slice(pageData.position(), keyLength), pageData.slice(pageData.position(pageData.position() + keyLength).position(), valueLength));
    }

    public BaseEntry<ByteBuffer> getByPos(int pos) throws IOException {
        pageData.position(pageLinks.slice(pos * Integer.BYTES, Integer.BYTES).getInt());
        return readEntry();
    }

    public BaseEntry<ByteBuffer> getByKey(ByteBuffer key) throws IOException {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = low + ((high - low) / 2);
            int result = getByPos(mid).key().compareTo(key);
            if (result < 0) {
                low = mid + 1;
            } else if (result > 0) {
                high = mid - 1;
            } else {
                return getByPos(mid);
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {


        channel.close();
        reader.close();
    }

    public String toString(ByteBuffer in) {
        ByteBuffer data = in.asReadOnlyBuffer();
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
