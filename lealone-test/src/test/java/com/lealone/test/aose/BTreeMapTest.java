/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.lealone.db.value.ValueLong;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.aose.btree.BTreeMap;

// -Xms800M -Xmx800M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
public class BTreeMapTest extends AoseTestBase {
    @Test
    public void run() {
        // for (int i = 0; i < 10; i++) {
        init();

        testSyncOperations();
        testAsyncOperations();
        testSplit();
        testRemove();
        testSave();
        testAppend();
        // }
    }

    void testSyncOperations() {
        Object v = null;
        map.clear();

        v = map.put(10, "a");
        assertNull(v);
        v = map.get(10);
        assertEquals("a", v);
        assertTrue(map.containsKey(10));

        v = map.putIfAbsent(10, "a1");
        assertNotNull(v);
        assertEquals("a", v);

        v = map.putIfAbsent(20, "b");
        assertNull(v);

        v = map.get(20);
        assertEquals("b", v);

        map.clear();
        assertEquals(0, map.size());

        int size = 200;
        for (int i = 1; i <= size; i++) {
            map.put(i, "value" + i);
        }

        assertEquals(size, map.size());

        v = map.firstKey();
        assertEquals(1, v);
        v = map.lastKey();
        assertEquals(size, v);

        v = map.higherKey(101); // >"101"的最小key
        assertEquals(102, v);
        v = map.ceilingKey(101); // >="101"的最小key
        assertEquals(101, v);

        v = map.lowerKey(101); // <"101"的最大key
        assertEquals(100, v);
        v = map.floorKey(101); // <="101"的最大key
        assertEquals(101, v);

        v = map.replace(100, "value100a", "value100");
        assertFalse((boolean) v);
        v = map.replace(100, "value100", "value100a");
        assertTrue((boolean) v);
        v = map.get(100);
        assertEquals("value100a", v);
        v = map.replace(100, "value100a", "value100");
        assertTrue((boolean) v);

        StorageMapCursor<?, ?> cursor = map.cursor();
        int count = 0;
        while (cursor.next()) {
            count++;
        }
        assertEquals(size, count);

        cursor = map.cursor(151);
        count = 0;
        while (cursor.next()) {
            count++;
        }
        assertEquals(50, count);

        v = map.remove(150);
        assertNotNull(v);
        assertEquals(199, map.size());

        // map.printPage();
        // map.remove();

        map.close();

        assertTrue(map.isClosed());

        try {
            map.put(10, "a");
            fail();
        } catch (IllegalStateException e) {
            // e.printStackTrace();
        }
        // 重新打开，看看size这个参数是否保存正确
        openMap();
        assertEquals(199, map.size());
        map.close();
    }

    void testAsyncOperations() {
        openMap();
        map.clear();
        int count = 7;
        CountDownLatch latch = new CountDownLatch(count);
        CountDownLatch latch2 = new CountDownLatch(1);

        int key = 10;
        final String value = "value-10";
        map.put(key, value, ar -> {
            latch.countDown();
            latch2.countDown();
        });
        try {
            latch2.await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        map.get(key, ar -> {
            latch.countDown();
            assertEquals(value, ar.getResult());
        });

        map.putIfAbsent(20, "value-20", ar -> {
            latch.countDown();
        });

        map.putIfAbsent(10, "value-30", ar -> {
            latch.countDown();
        });

        map.replace(10, "value-20", "value-100", ar -> {
            latch.countDown();
        });

        map.replace(10, "value-10", "value-100", ar -> {
            latch.countDown();
        });

        map.remove(20, ar -> {
            latch.countDown();
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(1, map.size());
    }

    void testSplit() {
        openMap();
        map.clear();
        int count = 10000;
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 1; i <= count; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value, ar -> {
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(count, map.size());

        for (int i = 1; i <= 40; i += 2) {
            map.put(i, "value" + i);
        }

        map.save();

        for (int i = 1; i <= 40; i += 2) {
            map.remove(i);
        }

        // map.printPage();
    }

    // remove相对比较复杂，单独拿来重点测
    void testRemove() {
        openMap();
        map.clear();
        map.put(1, "a");
        map.put(2, "b");
        map.remove(1);
        map.remove(2);

        int count = 20;
        for (int i = 1; i <= count; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value);
        }
        map.printPage();

        for (int i = count; i >= 9; i--) {
            Integer key = i;
            map.remove(key);
        }
        map.printPage();

        CountDownLatch latch = new CountDownLatch(1);
        map.remove(8, ar -> {
            latch.countDown();
        });
        try {
            latch.await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        CountDownLatch latch2 = new CountDownLatch(count - 8 + 1);
        for (int i = 8; i <= count; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value, ar -> {
                latch2.countDown();
            });
        }
        try {
            latch2.await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        assertEquals(count, map.size());

        map.printPage();
    }

    void testSave() {
        openMap();
        map.clear();
        map.save();
        map.put(1, "a");
        map.put(2, "b");
        map.save();

        int count = 20;
        for (int i = 1; i <= count; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value);
        }
        map.save();

        map.put(1, "a1");
        map.put(20, "b1");
        map.save();
        map.printPage();
    }

    void testAppend() {
        BTreeMap<ValueLong, String> map = storage.openBTreeMap("BTreeMapTestAppend");
        map.clear();
        assertEquals(0, map.getMaxKey());

        int count = 20;
        for (int i = 1; i <= count; i++) {
            String value = "value-" + i;
            ValueLong key = map.append(value);
            assertEquals(i, key.getLong());
        }
        assertEquals(count, map.getMaxKey());
    }
}
