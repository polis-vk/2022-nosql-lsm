package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class WrapperReader {

    private final List<DataReader> dataReaders = new ArrayList<>();

    public WrapperReader(TreeSet<String> namesFiles) throws IOException {
        for (Iterator<String> it = namesFiles.descendingIterator(); it.hasNext(); ) {
            String nameFile = it.next();
            dataReaders.add(new DataReader(Path.of(nameFile + ".dat"), Path.of(nameFile + ".ind")));
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        for (DataReader dataReader : dataReaders) {
            if (dataReader.isExist()) {
                Entry<MemorySegment> entry = dataReader.get(key);
                if (entry != null) {
                    return entry;
                }
            }
        }
        return null;
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to,
                                              Iterator<Entry<MemorySegment>> inMemory) {
        List<DataReader.DataReaderIterator> dataReaderIteratorList = new ArrayList<>();
        for (DataReader dataReader : dataReaders) {
            DataReader.WrapperMemorySegment wrapperMemorySegmentFrom;
            DataReader.WrapperMemorySegment wrapperMemorySegmentTo;
            long start;
            if (from != null) {
                wrapperMemorySegmentFrom = dataReader.binarySearch(from);
                if (wrapperMemorySegmentFrom.memorySegment == null) {
                    start = wrapperMemorySegmentFrom.offset;
                } else {
                    start = wrapperMemorySegmentFrom.offset - wrapperMemorySegmentFrom.lengthMemorySegment - Long.BYTES;
                }
            } else {
                start = 0;
            }
            long finish;
            if (to != null) {
                wrapperMemorySegmentTo = dataReader.binarySearch(to);
                if (wrapperMemorySegmentTo.memorySegment == null) {
                    finish = wrapperMemorySegmentTo.offset;
                } else {
                    finish = wrapperMemorySegmentTo.offset - wrapperMemorySegmentTo.lengthMemorySegment - Long.BYTES;
                }
            } else {
                finish = dataReader.dataMemorySegment.byteSize();
            }
            dataReaderIteratorList.add(new DataReader.DataReaderIterator(start, finish, dataReader.dataMemorySegment));
        }
        List<Entry<MemorySegment>> listOfRange = new ArrayList<>();
        int countLive = dataReaderIteratorList.size();

        while (countLive > 0) {
            MemorySegment minMemorySegment = null;
            Entry<MemorySegment> minEntry = null;
            DataReader.DataReaderIterator minIterator = null;
            for (DataReader.DataReaderIterator iterator : dataReaderIteratorList) {
                if (iterator.getNow() == null) {
                    iterator.next();
                }
                if (iterator.hasNext() && (minMemorySegment == null || InMemoryDao.comparator(minMemorySegment, iterator.getNow().key()) > 0)) {
                    minIterator = iterator;
                    minMemorySegment = iterator.getNow().key();
                    minEntry = iterator.getNow();
                }
                if (!iterator.hasNext()) {
                    countLive--;
                }
            }
            if (minIterator != null ) {
                listOfRange.add(minEntry);
                minIterator.next();
            }
        }

        PeekIterator peekIteratorInMemory = new PeekIterator(inMemory);
        PeekIterator peekIteratorFile = new PeekIterator(listOfRange.iterator());
        List<Entry<MemorySegment>> result = new ArrayList<>();
        if (peekIteratorInMemory.peek() == null && peekIteratorInMemory.hasNext()) {
            peekIteratorInMemory.next();
        }
        if (peekIteratorFile.peek() == null && peekIteratorFile.hasNext()) {
            peekIteratorFile.next();
        }

        while (peekIteratorInMemory.peek() != null || peekIteratorFile.peek() != null) {
            if (peekIteratorInMemory.peek() != null && peekIteratorFile.peek() != null) {
                long resultCompare = InMemoryDao.comparator(peekIteratorInMemory.peek().key(),
                        peekIteratorFile.peek().key());
                if (resultCompare == 0) {
                    result.add(peekIteratorInMemory.peek());
                    peekIteratorInMemory.next();
                    peekIteratorFile.next();
                } else if (resultCompare < 0) {
                    result.add(peekIteratorInMemory.peek());
                    peekIteratorInMemory.next();
                } else {
                    result.add(peekIteratorFile.peek());
                    peekIteratorFile.next();
                }
            } else if (peekIteratorInMemory.peek() != null) {
                result.add(peekIteratorInMemory.peek());
                peekIteratorInMemory.next();
            } else {
                result.add(peekIteratorFile.peek());
                peekIteratorFile.next();
            }
        }
        return result.iterator();
    }

}
