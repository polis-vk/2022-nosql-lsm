package ru.mail.polis.dmitreemaximenko;

import java.io.IOException;
import java.nio.file.Path;

// this class only exists because of the file size limit (250 lines)
public class Flusher implements Runnable {
    private final MemorySegmentDao memorySegmentDao;

    public Flusher(MemorySegmentDao memorySegmentDao) {
        this.memorySegmentDao = memorySegmentDao;
    }

    @Override
    public void run() {
        try {
            while (true) {
                memorySegmentDao.flushLock.readLock().lock();
                if (memorySegmentDao.flushFinish) {
                    break;
                }
                memorySegmentDao.flushLock.readLock().unlock();
                MemTable tableToFlush = memorySegmentDao.flushingTables.take();

                memorySegmentDao.dbTablesLock.writeLock().lock();
                memorySegmentDao.flushingTable = tableToFlush;
                memorySegmentDao.dbTablesLock.writeLock().unlock();

                if (!memorySegmentDao.flushingTable.isEmpty()) {
                    memorySegmentDao.writeValuesToFile(memorySegmentDao.flushingTable,
                            memorySegmentDao.fileManager.getFlushTmpFile());

                    int newLogIndex = memorySegmentDao.fileManager
                            .addLog(memorySegmentDao.fileManager.getFlushTmpFile());
                    memorySegmentDao.dbTablesLock.writeLock().lock();
                    memorySegmentDao.ssTables.add(0,
                            new SSTable(memorySegmentDao.fileManager.getLogName(newLogIndex),
                                    memorySegmentDao.scope));
                    memorySegmentDao.dbTablesLock.writeLock().unlock();
                }
            }

            while (!memorySegmentDao.flushingTables.isEmpty()) {
                MemTable tableToFlush = memorySegmentDao.flushingTables.poll();

                memorySegmentDao.dbTablesLock.writeLock().lock();
                memorySegmentDao.flushingTable = tableToFlush;
                Path filename = memorySegmentDao.fileManager.getNextLogName();
                memorySegmentDao.dbTablesLock.writeLock().unlock();

                if (!memorySegmentDao.flushingTable.isEmpty()) {
                    memorySegmentDao.writeValuesToFile(memorySegmentDao.flushingTable, filename);
                }
            }
        } catch (IOException | InterruptedException exception) {
            exception.printStackTrace();
            // can't throw because of code style check
            // throw new IllegalThreadStateException();
        }
    }
}
