package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.UnexpectedException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final static int IN_MEMORY_SIZE = 100000;
    private final static int CHUNK_SIZE = 100;
    private final static String FILE_EXTENSION = ".seg";
    private final Config config;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> memory = new ConcurrentSkipListMap<>();
    private int currentFolder;

    public PersistenceDao(Config config) {
        String[] str = new File(config.basePath().toString()).list();
        if (str == null) {
            currentFolder = 0;
        } else {
            currentFolder = str.length;
        }
        this.config = config;
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = memory.get(key);
        if (result != null || currentFolder == 0) {
            return result;
        }
        return findInFiles(key);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return memory.values().iterator();
        }
        if (from == null) {
            return memory.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return memory.tailMap(from, true).values().iterator();
        }
        return memory.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (memory.size() >= IN_MEMORY_SIZE) {
            try {
                flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        memory.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        store();
        memory.clear();
        currentFolder++;
    }

    private void store() throws IOException {
        String path = config.basePath() + File.separator + currentFolder + File.separator;
        new File(path).mkdirs();

        if(memory.size() < CHUNK_SIZE) {
            try (DaoWriter out = new DaoWriter(path + 0 + FILE_EXTENSION)) {
                out.writeMap(memory);
                return;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        Iterator<Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>>> iterator = memory.entrySet().iterator();
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> tempMap;
        Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry;
        int counter = 0;

        while (iterator.hasNext()) {
            tempMap = new ConcurrentSkipListMap<>();
            while (iterator.hasNext() && tempMap.size() != CHUNK_SIZE) {
                entry = iterator.next();
                tempMap.put(entry.getKey(), entry.getValue());
            }
            try (DaoWriter out = new DaoWriter(path + counter + FILE_EXTENSION)) {
                out.writeMap(tempMap);
                tempMap.clear();
                counter++;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

    }

    private int counter = 0;
    private BaseEntry<ByteBuffer> findInFiles(ByteBuffer key) throws IOException {
        for (int i = currentFolder - 1; i >= 0; i--) {
            BaseEntry<ByteBuffer> result = binarySearchInFolder(key, i);
            if(result != null) {
                counter++;
                System.out.println("Записано: " + counter);
                return result;
            }
        }
        return null;
    }

    private BaseEntry<ByteBuffer> binarySearchInFolder(ByteBuffer key, int folder) throws IOException {
        String path = config.basePath() + File.separator + folder + File.separator;

        String[] files = (new File(path)).list();
        if(files == null) {
            return null;
        }
        Arrays.sort(files, Comparator.comparingInt(this::extractInt));

        int lowerBond = 0;
        int higherBond = files.length - 1;
        int middle = files.length / 2;

        while(lowerBond <= higherBond) {
            int result = checkFile(path, files[middle], key);
            if (result == 1) {
                lowerBond = middle + 1;
            }
            else if (result == -1){
                higherBond = middle - 1;
            }
            else if (result == 0) {
                return retrieveFromFile(path, files[middle], key);
            }
            else if (result == 404) {
                return null;
            }
            else {
                throw new UnexpectedException("Something is very wrong with comparator");
            }
            middle = (lowerBond + higherBond)/2;
        }

        return null;
    }

    private BaseEntry<ByteBuffer> retrieveFromFile(String path, String file, ByteBuffer key) throws IOException {
        try (DaoReader finder = new DaoReader(path + file)) {
            return finder.retrieveElement(key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int checkFile(String path, String file, ByteBuffer key) throws IOException {
        try (DaoReader checker = new DaoReader(path + file)) {
            return checker.checkForKey(key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return -2;
    }

    private int extractInt(String s) {
        String num = s.substring(0, s.length() - FILE_EXTENSION.length());
        return num.isEmpty() ? 0 : Integer.parseInt(num);
    }

}
