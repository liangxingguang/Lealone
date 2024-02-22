/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aote;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.lealone.db.index.standard.VersionedValue;
import com.lealone.db.index.standard.VersionedValueType;
import com.lealone.db.result.SortOrder;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueInt;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionMap;
import com.lealone.transaction.aote.TransactionalValue;

public class TransactionalValueTest extends AoteTestBase {

    @Test
    public void run() {
        testExclusiveCommit();
        testExclusiveRollback();
        testUncommittedCommit();
        testRemove();
    }

    void testExclusiveCommit() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map = t.openMap("testExclusiveCommit", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        map.put("2", "b3");
        t.commit();

        t = te.beginTransaction();
        map = t.openMap("testExclusiveCommit", storage);
        map.put("2", "b4");
        map.put("2", "b5");
        t.commit();
        TransactionalValue tv = (TransactionalValue) map.getTransactionalValue("2");
        assertEquals("b5", tv.getValue());
    }

    void testExclusiveRollback() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map = t.openMap("testExclusiveRollback", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        map.put("2", "b3");
        t.rollback();
        TransactionalValue tv = (TransactionalValue) map.getTransactionalValue("2");
        assertNull(tv);

        t = te.beginTransaction();
        map = t.openMap("testExclusiveRollback", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        t.addSavepoint("sp1");
        map.put("2", "b3");
        t.rollbackToSavepoint("sp1");
        t.commit();
        tv = (TransactionalValue) map.getTransactionalValue("2");
        assertEquals("b2", tv.getValue());
    }

    void testUncommittedCommit() {
        String mapName = "testUncommittedCommit";
        int columns = 4;
        int[] sortTypes = new int[columns];
        for (int i = 0; i < columns; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        VersionedValueType vvType = new VersionedValueType(null, null, sortTypes, columns);

        Transaction t = te.beginTransaction();
        TransactionMap<String, VersionedValue> map = t.openMap(mapName, null, vvType, storage);
        map.clear();

        String key = "1";

        ValueArray valueArray = createValueArray(0, 0, 0, 0);
        VersionedValue vv = new VersionedValue(1, valueArray.getList());
        map.put(key, vv);
        t.commit();

        Transaction t1 = te.beginTransaction();
        TransactionMap<String, VersionedValue> map1 = t1.openMap(mapName, storage);

        Transaction t2 = te.beginTransaction();
        TransactionMap<String, VersionedValue> map2 = t2.openMap(mapName, storage);

        Transaction t3 = te.beginTransaction();
        TransactionMap<String, VersionedValue> map3 = t3.openMap(mapName, storage);

        vv = createVersionedValue(map1, key, 0, 10);
        map1.tryUpdate(key, vv, new int[] { 0 });
        vv = createVersionedValue(map1, key, 0, 11);
        map1.tryUpdate(key, vv, new int[] { 0 });

        vv = createVersionedValue(map2, key, 1, 20);
        map2.tryUpdate(key, vv, new int[] { 1 });
        vv = createVersionedValue(map2, key, 1, 21);
        map2.tryUpdate(key, vv, new int[] { 1 });

        vv = createVersionedValue(map3, key, 2, 30);
        map3.tryUpdate(key, vv, new int[] { 2 });
        vv = createVersionedValue(map3, key, 2, 31);
        map3.tryUpdate(key, vv, new int[] { 2 });

        TransactionalValue tv = (TransactionalValue) map3.getTransactionalValue(key);
        System.out.println(tv);
        System.out.println("========");

        // t2.commit();
        // // t2.rollback();
        // t3.commit();
        // t1.commit();

        CountDownLatch latch = new CountDownLatch(3);
        new Thread(() -> {
            t2.commit();
            latch.countDown();
        }).start();
        new Thread(() -> {
            t3.commit();
            latch.countDown();
        }).start();
        new Thread(() -> {
            t1.commit();
            latch.countDown();
        }).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("========");
        tv = (TransactionalValue) map3.getTransactionalValue(key);
        System.out.println(tv);
    }

    private ValueArray createValueArray(int... values) {
        ValueInt[] a = new ValueInt[values.length];
        for (int i = 0; i < a.length; i++)
            a[i] = ValueInt.get(values[i]);
        return ValueArray.get(a);
    }

    private VersionedValue createVersionedValue(TransactionMap<String, VersionedValue> map, String key,
            int columnIndex, int value) {
        VersionedValue vv = map.get(key);
        Value[] values = vv.columns.clone();
        values[columnIndex] = ValueInt.get(value);
        vv = new VersionedValue(1, values);
        return vv;
    }

    void testRemove() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map = t.openMap("testRemove", storage);
        map.clear();
        map.put("2", "b1");
        t.commit();

        Transaction t1 = te.beginTransaction(Transaction.IL_REPEATABLE_READ);
        TransactionMap<String, String> map1 = t1.openMap("testRemove", storage);

        Transaction t2 = te.beginTransaction();
        TransactionMap<String, String> map2 = t2.openMap("testRemove", storage);
        map2.remove("2");
        t2.commit();

        String v = map1.get("2");
        System.out.println(v);
        t1.commit();
    }
}
