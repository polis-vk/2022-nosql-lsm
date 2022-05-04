package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;

import static ru.mail.polis.deniszhidkov.DaoUtils.DATA_FILE_NAME;
import static ru.mail.polis.deniszhidkov.DaoUtils.OFFSETS_FILE_NAME;

public class Compacter {

    private final DaoUtils utils;

    public Compacter(DaoUtils utils) {
        this.utils = utils;
    }

    public void writeData(CopyOnWriteArrayList<DaoReader> readers) throws IOException {
        Queue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        utils.addInStorageIteratorsByRange(iteratorsQueue, readers, null, null, 0);
        Iterator<BaseEntry<String>> allData = new MergeIterator(iteratorsQueue);
        int allDataSize = 0;
        while (allData.hasNext()) {
            allDataSize++;
            allData.next();
        }
        utils.addInStorageIteratorsByRange(iteratorsQueue, readers, null, null, 0);
        allData = new MergeIterator(iteratorsQueue);

        Path pathToTmpData = utils.resolvePath(DATA_FILE_NAME, -2);
        Path pathToTmpOffsets = utils.resolvePath(OFFSETS_FILE_NAME, -2);
        DaoWriter tmpWriter = new DaoWriter(pathToTmpData, pathToTmpOffsets);
        tmpWriter.writeDAOWithoutTombstones(allData, allDataSize);

        moveToCompacted(pathToTmpData, pathToTmpOffsets, readers);
    }

    private void moveToCompacted(Path pathToTmpData,
                                 Path pathToTmpOffsets,
                                 CopyOnWriteArrayList<DaoReader> readers) throws IOException {
        Files.move(pathToTmpData, utils.resolvePath(DATA_FILE_NAME, -1), StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToTmpOffsets, utils.resolvePath(OFFSETS_FILE_NAME, -1), StandardCopyOption.ATOMIC_MOVE);
        utils.closeReaders(readers);
    }

    public void finishCompact() throws IOException {
        Path pathToCompactedData = utils.resolvePath(DATA_FILE_NAME, -1);
        Path pathToCompactedOffsets = utils.resolvePath(OFFSETS_FILE_NAME, -1);
        boolean isDataCompacted = Files.exists(pathToCompactedData);
        boolean isOffsetsCompacted = Files.exists(pathToCompactedOffsets);
        /* Если нет ни одного compacted файла, значит либо данные уже compacted, либо упали, не записав всех данных. */
        if (!isDataCompacted && !isOffsetsCompacted) {
            return;
        }

        if (checkAndMoveIfOnlyOffsetsCompacted(isDataCompacted, pathToCompactedOffsets)) {
            return;
        }

        checkAndMoveIfOffsetsTmp(pathToCompactedOffsets);

        moveToNormal(pathToCompactedData, pathToCompactedOffsets);
    }

    /* Данный метод отработает, если только offsets файл compacted и data файл не compacted. В соответствии с методом
     * moveToCompacted() значит, что мы упали между переводом data из compacted в нормальное состояние и тем же
     * переводом для offsets. */
    private boolean checkAndMoveIfOnlyOffsetsCompacted(boolean isDataCompacted, Path pathToFile) throws IOException {
        if (!isDataCompacted) {
            Files.move(pathToFile, utils.resolvePath(OFFSETS_FILE_NAME, 0), StandardCopyOption.ATOMIC_MOVE);
            return true;
        }
        return false;
    }

    /* Данный метод отработает, если data файл compacted и offsets файл не compacted. В соответствии с методом
     * writeData() это означает, что упали, не успев перевести файл offsets из tmp в compacted. При этом запись
     * полностью прошла, поскольку data файл compacted. */
    private void checkAndMoveIfOffsetsTmp(Path pathToFile) throws IOException {
        Path pathToTmpOffsetsFile = utils.resolvePath(OFFSETS_FILE_NAME, -2);
        if (Files.exists(pathToTmpOffsetsFile)) {
            Files.move(pathToTmpOffsetsFile, pathToFile, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    /* Данный метод выполнится и в том случае, если мы зайдём в данный метод во время compaction
     * (а не только в случае попытки восстановления состояния), поскольку оба файла будут compacted. */
    private void moveToNormal(Path pathToCompactedData, Path pathToCompactedOffsets) throws IOException {
        utils.removeOldFiles();
        Path pathToNewStorage = utils.resolvePath(DATA_FILE_NAME, 0);
        Path pathToNewOffsets = utils.resolvePath(OFFSETS_FILE_NAME, 0);
        Files.move(pathToCompactedData, pathToNewStorage, StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToCompactedOffsets, pathToNewOffsets, StandardCopyOption.ATOMIC_MOVE);
    }
}
