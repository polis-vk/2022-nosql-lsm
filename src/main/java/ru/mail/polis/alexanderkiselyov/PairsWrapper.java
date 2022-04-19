package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PairsWrapper {
    private final NavigableMap<Byte[], BaseEntry<Byte[]>> pairs0;
    private final NavigableMap<Byte[], BaseEntry<Byte[]>> pairs1;
    private final AtomicLong size0;
    private final AtomicLong size1;
    private final AtomicLong maxThresholdBytesSize;
    private final AtomicInteger pairNum;

    public PairsWrapper(long maxThresholdBytesSize) {
        this.maxThresholdBytesSize = new AtomicLong(maxThresholdBytesSize);
        pairs0 = new ConcurrentSkipListMap<>(Arrays::compare);
        pairs1 = new ConcurrentSkipListMap<>(Arrays::compare);
        size0 = new AtomicLong(0);
        size1 = new AtomicLong(0);
        pairNum = new AtomicInteger(0);
    }

    public NavigableMap<Byte[], BaseEntry<Byte[]>> getPairs() {
        return pairNum.get() == 0 ? pairs0 : pairs1;
    }

    public void updateSize(long delta) {
        if (pairNum.get() == 0) {
            size0.addAndGet(delta);
        } else {
            size1.addAndGet(delta);
        }
    }

    public AtomicLong getCurrentPairSize() {
        return pairNum.get() == 0 ? size0 : size1;
    }

    public AtomicLong getMaxPairSize() {
        return maxThresholdBytesSize;
    }

    public AtomicInteger getCurrentPairNum() {
        return pairNum;
    }

    public void changePairs(long delta) {
        if ((pairNum.get() == 0 && (maxThresholdBytesSize.get() - size1.get()) <= delta)
        || (pairNum.get() == 1 && (maxThresholdBytesSize.get() - size0.get()) <= delta)) {
            throw new RuntimeException("Unable to upsert new values, all maps are full.");
        }
        pairNum.set(1 - pairNum.get());
    }

    public void clearPair(AtomicInteger pairNum) {
        if (pairNum.get() == 0) {
            pairs0.clear();
            size0.set(0);
        } else {
            pairs1.clear();
            size1.set(0);
        }
    }
}
