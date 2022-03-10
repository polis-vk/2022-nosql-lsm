package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class ReadFromNonVolatileMemory {

    private final List<DataReader> dataReaders = new ArrayList<>();

    public ReadFromNonVolatileMemory(TreeSet<String> namesFiles) throws IOException {
        for (Iterator<String> it = namesFiles.descendingIterator(); it.hasNext(); ) {
            String nameFile = it.next();
            dataReaders.add(new DataReader(Path.of(nameFile + ".dat"), Path.of(nameFile + ".ind")));
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        for(DataReader dataReader : dataReaders) {
            if (dataReader.isExist()) {
            Entry<MemorySegment> entry = dataReader.get(key);
            if (entry != null) {
                return entry;
            }
            }
        }
        return null;
    }

}
