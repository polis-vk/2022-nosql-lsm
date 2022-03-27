/*
 * Copyright 2021 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.junit.jupiter.api.Test;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compaction tests for {@link Dao} implementations.
 *
 * @author Vadim Tsesko
 */
class CompactionTest extends BaseTest {
    @DaoTest(stage = 4)
    void overwrite(Dao<String, Entry<String>> dao) throws IOException {
        // Reference value
        int valueSize = 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        List<Entry<String>> entries = bigValues(keyCount, valueSize);

        // Overwrite keys several times each time closing DAO
        for (int round = 0; round < overwrites; round++) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        // Big size
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        long bigSize = sizePersistentData(dao);

        // Compact
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entries);

        // Check store size
        long smallSize = sizePersistentData(dao);

        // Heuristic
        assertTrue(smallSize * (overwrites - 1) < bigSize);
        assertTrue(smallSize * (overwrites + 1) > bigSize);
    }

    @DaoTest(stage = 4)
    void multiple(Dao<String, Entry<String>> dao) throws IOException {
        // Reference value
        int valueSize = 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        List<Entry<String>> entries = bigValues(keyCount, valueSize);
        List<Long> sizes = new ArrayList<>();

        // Overwrite keys multiple times with intermediate compactions
        for (int round = 0; round < overwrites; round++) {
            // Overwrite
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }

            // Compact
            dao.compact();
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
            sizes.add(sizePersistentData(dao));
        }

        LongSummaryStatistics stats = sizes.stream().mapToLong(k -> k).summaryStatistics();
        // Heuristic
        assertTrue(stats.getMax() - stats.getMin() < 1024);
    }

}
