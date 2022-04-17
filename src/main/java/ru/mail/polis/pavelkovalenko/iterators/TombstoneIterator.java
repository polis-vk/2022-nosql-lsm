package ru.mail.polis.pavelkovalenko.iterators;

import javax.security.auth.login.LoginException;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Queue;

public class TombstoneIterator implements Iterator<Entry<ByteBuffer>> {

    private final Queue<PeekIterator<Entry<ByteBuffer>>> iterators;

    public TombstoneIterator(Queue<PeekIterator<Entry<ByteBuffer>>> iterators) {
        this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
        iterators.removeIf(this::hasNotNext);
        skipTombstones();
        return !iterators.isEmpty();
    }

    @Override
    public Entry<ByteBuffer> next() {
        throw new UnsupportedOperationException("Unable to call 'next' from TombstoneIterator");
    }

    private boolean hasNotNext(PeekIterator<Entry<ByteBuffer>> iterator) {
        return !iterator.hasNext();
    }

    private void skipTombstones() {
        while (!iterators.isEmpty() && hasTombstoneForFirstElement()) {
            if (iterators.size() == 1) {
                skipLastOneStanding();
                return;
            }

            PeekIterator<Entry<ByteBuffer>> first = iterators.peek();
            while (first != null && first.hasNext() && Utils.isTombstone(first.peek())) {
                Utils.fallEntry(iterators, first.peek());
                first = iterators.peek();
            }
        }
    }

    private boolean hasTombstoneForFirstElement() {
        PeekIterator<Entry<ByteBuffer>> first = iterators.remove();
        iterators.add(first);
        return !iterators.isEmpty() && Utils.isTombstone(iterators.peek().peek());
    }

    private void skipLastOneStanding() {
        if (iterators.isEmpty()) {
            return;
        }

        PeekIterator<Entry<ByteBuffer>> first = iterators.peek();
        while (first.hasNext() && Utils.isTombstone(first.peek())) {
            first.next();
        }

        if (!first.hasNext()) {
            iterators.remove();
        }
    }
}
