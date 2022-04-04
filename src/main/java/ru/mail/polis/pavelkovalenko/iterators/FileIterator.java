package ru.mail.polis.pavelkovalenko.iterators;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.MappedPairedFiles;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class FileIterator implements Iterator<Entry<ByteBuffer>> {

    private final MappedPairedFiles mappedFilePair;
    private final ByteBuffer from;
    private final ByteBuffer to;
    private final Entry<ByteBuffer> toEntry;
    private Entry<ByteBuffer> current;
    private final Serializer serializer;
    private int curIndexesPos = 0;

    public FileIterator(MappedPairedFiles mappedFilePair, Serializer serializer, ByteBuffer from, ByteBuffer to) throws IOException {
        this.mappedFilePair = mappedFilePair;
        this.from = from;
        this.to = to;
        toEntry = new BaseEntry<>(to, to);
        this.serializer = serializer;

        binarySearchInFile();
    }

    @Override
    public boolean hasNext() {
        try {
            boolean hasNext = dataExists() && canContinue();
            current = null;
            return hasNext;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Entry<ByteBuffer> next() {
        try {
            Entry<ByteBuffer> peek = peek();
            current = null;
            curIndexesPos += Utils.INDEX_OFFSET;
            return peek;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Entry<ByteBuffer> peek() throws IOException {
        if (current == null && !isEOFReached()) {
            current = serializer.readEntry(mappedFilePair, curIndexesPos);
        }
        return current;
    }

    private boolean dataExists() throws IOException {
        return peek() != null;
    }

    private boolean canContinue() throws IOException {
        return (peek() != null && peek().key().compareTo(from) >= 0)
                && ((to == null && peek() != null)
                    || (peek() != null && Utils.entryComparator.compare(peek(), toEntry) < 0));
    }

    private boolean isEOFReached() {
        return curIndexesPos >= mappedFilePair.indexesFile().limit();
    }

    private void binarySearchInFile() throws IOException {
        if (!dataExists() || isFromOutOfBound()) {
            return;
        }

        Entry<ByteBuffer> ceilEntry = getLast();
        int a = 0;
        int b = getIndexesFileLength() / Utils.INDEX_OFFSET;
        int c;

        while (b - a >= 1) {
            c = (b + a) / 2;
            Entry<ByteBuffer> curEntry = serializer.readEntry(mappedFilePair, c * Utils.INDEX_OFFSET);
            if (curEntry.key().compareTo(from) >= 0 && Utils.entryComparator.compare(curEntry, ceilEntry) <= 0) {
                ceilEntry = curEntry;
                curIndexesPos = c * Utils.INDEX_OFFSET;
            }

            int compare = curEntry.key().compareTo(from);
            if (compare < 0) {
                if (b - a <= 1) {
                    break;
                }
                a = c;
            } else if (compare == 0) {
                break;
            } else {
                if (b - a <= 1) {
                    break;
                }
                b = c;
            }
        }
    }

    private Entry<ByteBuffer> getLast() {
        return serializer.readEntry(mappedFilePair, getIndexesFileLength() - Utils.INDEX_OFFSET);
    }

    private boolean isFromOutOfBound() {
        return from.compareTo(getLast().key()) > 0;
    }

    private int getIndexesFileLength() {
        return mappedFilePair.indexesFile().limit();
    }

}
