package ru.mail.polis.pavelkovalenko.iterators;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.comparators.EntryComparator;
import ru.mail.polis.pavelkovalenko.dto.MappedPairedFiles;
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
    private int curIndexesPos;

    public FileIterator(MappedPairedFiles mappedFilePair, Serializer serializer, ByteBuffer from, ByteBuffer to)
            throws IOException {
        this.mappedFilePair = mappedFilePair;
        this.from = from;
        this.to = to;
        toEntry = new BaseEntry<>(to, to);
        this.serializer = serializer;

        if (dataExists() && !isFromOutOfBound()) {
            binarySearchInFile();
        }
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
                || (peek() != null && EntryComparator.INSTANSE.compare(peek(), toEntry) < 0));
    }

    private boolean isEOFReached() {
        return curIndexesPos >= mappedFilePair.indexesFile().limit();
    }

    private void binarySearchInFile() {
        ByteBuffer ceilKey = getLast().key();
        int a = -1;
        int b = getIndexesFileLength() / Utils.INDEX_OFFSET;
        int c;

        while (b - a > 1) {
            c = (b + a) / 2;
            ByteBuffer curKey = serializer.readKey(mappedFilePair, c * Utils.INDEX_OFFSET);
            int curKeyCompareToFrom = curKey.compareTo(from);
            if (curKeyCompareToFrom >= 0 && curKey.compareTo(ceilKey) <= 0) {
                ceilKey = curKey;
                curIndexesPos = c * Utils.INDEX_OFFSET;
            }

            if (curKeyCompareToFrom < 0) {
                a = c;
            } else if (curKeyCompareToFrom == 0) {
                break;
            } else {
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
