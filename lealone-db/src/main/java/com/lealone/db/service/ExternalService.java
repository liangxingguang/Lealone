/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import java.util.HashMap;
import java.util.Map;

import com.lealone.db.Database;
import com.lealone.db.RunMode;
import com.lealone.db.index.Cursor;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.value.ValueString;
import com.lealone.storage.StorageSetting;

public class ExternalService {

    public static String getName() {
        return "external_service";
    }

    public static Table findTable(Database db) {
        return db.findSchema(null, "INFORMATION_SCHEMA").findTableOrView(null, getName());
    }

    private Table table;

    public void init(Database db) {
        table = findTable(db);
        if (table != null)
            return;
        db.getSystemSession()
                .executeUpdateLocal("CREATE TABLE IF NOT EXISTS INFORMATION_SCHEMA.external_service" //
                        + " (name varchar, url varchar, comment varchar," //
                        + " PRIMARY KEY(name))" //
                        + " PARAMETERS(" + StorageSetting.RUN_MODE.name() + "='" //
                        + RunMode.CLIENT_SERVER.name() + "')");
        table = findTable(db);
    }

    public Map<String, Map<String, String>> getRecords(ServerSession session) {
        Cursor cursor = table.getIndexes().get(1).find(session, null, null);
        Map<String, Map<String, String>> records = new HashMap<>();
        while (cursor.next()) {
            Row row = cursor.get();
            String name = row.getValue(0).getString();
            String url = row.getValue(1).getString();
            String comment = row.getValue(2).getString();
            Map<String, String> map = new HashMap<>();
            map.put("name", name);
            map.put("url", url);
            map.put("comment", comment);
            records.put(name, map);
        }
        return records;
    }

    public void addRecord(ServerSession session, String name, String url, String comment) {
        Row row = table.getTemplateRow();
        row.setValue(0, ValueString.get(name));
        row.setValue(1, ValueString.get(url));
        row.setValue(2, ValueString.get(comment));
        table.addRow(session, row);
    }

    public void deleteRecord(ServerSession session, String name) {
        Row row = table.getTemplateRow();
        row.setValue(0, ValueString.get(name));
        Cursor cursor = table.getIndexes().get(1).find(session, row, row);
        while (cursor.next()) {
            row = cursor.get();
            table.removeRow(session, row);
        }
    }
}
