/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.table;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.MapUtils;
import com.lealone.common.util.StatementBuilder;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Constants;
import com.lealone.db.DataHandler;
import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.RunMode;
import com.lealone.db.SysProperties;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.async.Future;
import com.lealone.db.constraint.Constraint;
import com.lealone.db.constraint.ConstraintReferential;
import com.lealone.db.index.Index;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexRebuilder;
import com.lealone.db.index.IndexType;
import com.lealone.db.index.hash.NonUniqueHashIndex;
import com.lealone.db.index.hash.UniqueHashIndex;
import com.lealone.db.index.standard.StandardDelegateIndex;
import com.lealone.db.index.standard.StandardPrimaryIndex;
import com.lealone.db.index.standard.StandardSecondaryIndex;
import com.lealone.db.index.standard.VersionedValue;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.result.Row;
import com.lealone.db.result.SortOrder;
import com.lealone.db.schema.SchemaObject;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column.EnumColumn;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.storage.StorageEngine;
import com.lealone.storage.StorageSetting;

/**
 * @author H2 Group
 * @author zhh
 */
public class StandardTable extends Table {

    private final StandardPrimaryIndex primaryIndex;
    private final ArrayList<Index> indexes = Utils.newSmallArrayList();
    private final ArrayList<Index> indexesExcludeDelegate = Utils.newSmallArrayList();
    private final StorageEngine storageEngine;
    private final Map<String, String> parameters;
    private final boolean globalTemporary;
    private final TableAnalyzer tableAnalyzer;

    private long lastModificationId;
    private Column rowIdColumn;
    private int[] largeObjectColumns;
    private DataHandler dataHandler;
    private final boolean useTableLobStorage;

    public StandardTable(CreateTableData data, StorageEngine storageEngine) {
        super(data.schema, data.id, data.tableName, data.persistIndexes, data.persistData);
        this.storageEngine = storageEngine;
        if (data.storageEngineParams != null) {
            parameters = data.storageEngineParams;
        } else {
            parameters = new CaseInsensitiveMap<>();
        }
        globalTemporary = data.globalTemporary;

        if (!data.persistData && data.isMemoryTable())
            parameters.put(StorageSetting.IN_MEMORY.name(), "1");
        if (!parameters.containsKey(StorageSetting.RUN_MODE.name()))
            parameters.put(StorageSetting.RUN_MODE.name(), getRunMode().name());

        isHidden = data.isHidden;
        int nextAnalyze = database.getSettings().analyzeAuto;
        tableAnalyzer = nextAnalyze > 0 ? new TableAnalyzer(this, nextAnalyze) : null;

        useTableLobStorage = MapUtils.getBoolean(parameters, StorageSetting.USE_TABLE_LOB_STORAGE.name(),
                true);

        setTemporary(data.temporary);
        setColumns(data.columns.toArray(new Column[0]));
        primaryIndex = new StandardPrimaryIndex(data.session, this);
        indexes.add(primaryIndex);
        indexesExcludeDelegate.add(primaryIndex);
    }

    public String getMapName() {
        return primaryIndex.getMapName();
    }

    public StorageEngine getStorageEngine() {
        return storageEngine;
    }

    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public String getParameter(String name) {
        String v = parameters.get(name);
        if (v == null)
            v = database.getParameters().get(name);
        return v;
    }

    public RunMode getRunMode() {
        String str = MapUtils.getString(parameters, StorageSetting.RUN_MODE.name(), null);
        if (str != null)
            return RunMode.valueOf(str.trim().toUpperCase());
        else
            return database.getRunMode();
    }

    @Override
    public String getCreateSQL() {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (isTemporary()) {
            if (isGlobalTemporary()) {
                buff.append("GLOBAL ");
            } else {
                buff.append("LOCAL ");
            }
            buff.append("TEMPORARY ");
        } else if (isPersistIndexes()) {
            buff.append("CACHED ");
        } else {
            buff.append("MEMORY ");
        }
        buff.append("TABLE ");
        if (isHidden) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(\n    ");
        for (Column column : columns) {
            buff.appendExceptFirst(",\n    ");
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        String storageEngineName = storageEngine.getName();
        if (storageEngineName != null) {
            String d = getDatabase().getSettings().defaultStorageEngine;
            if (d == null || !storageEngineName.endsWith(d)) {
                buff.append("\nENGINE \"");
                buff.append(storageEngineName);
                buff.append('\"');
            }
        }
        if (parameters != null && !parameters.isEmpty()) {
            buff.append(" PARAMETERS");
            Database.appendMap(buff, parameters);
        }
        if (!isPersistIndexes() && !isPersistData()) {
            buff.append("\nNOT PERSISTENT");
        }
        if (isHidden) {
            buff.append("\nHIDDEN");
        }
        if (getPackageName() != null) {
            buff.append("\nPACKAGE '").append(getPackageName()).append("'");
        }
        if (getCodePath() != null) {
            buff.append("\nGENERATE CODE '").append(getCodePath()).append("'");
        }
        return buff.toString();
    }

    @Override
    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL() + " CASCADE";
    }

    @Override
    public void close(ServerSession session) {
        for (Index index : indexes) {
            index.close(session);
        }
        database.removeDataHandler(getId());
    }

    @Override
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

    /**
     * Create a row from the values.
     *
     * @param data the value list
     * @return the row
     */
    public static Row createRow(Value[] data) {
        return new Row(data, Row.MEMORY_CALCULATE);
    }

    @Override
    public boolean canTruncate() {
        if (getCheckForeignKeyConstraints() && database.getReferentialIntegrity()) {
            ArrayList<Constraint> constraints = getConstraints();
            if (constraints != null) {
                for (int i = 0, size = constraints.size(); i < size; i++) {
                    Constraint c = constraints.get(i);
                    if (!(c.getConstraintType().equals(Constraint.REFERENTIAL))) {
                        continue;
                    }
                    ConstraintReferential ref = (ConstraintReferential) c;
                    if (ref.getRefTable() == this) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public Row getRow(ServerSession session, long key) {
        return primaryIndex.getRow(session, key);
    }

    public Row getRow(ServerSession session, long key, int[] columnIndexes) {
        return primaryIndex.getRow(session, key, columnIndexes);
    }

    @Override
    public Row getRow(Row oldRow) {
        return primaryIndex.getRow(oldRow.getPage(), oldRow.getTValue(), oldRow.getKey(),
                oldRow.getTValue().getValue());
    }

    @Override
    public boolean isRowChanged(Row row) {
        VersionedValue v = (VersionedValue) row.getTValue().getValue();
        return v.columns != row.getValueList();
    }

    @Override
    public Index addIndex(ServerSession session, String indexName, int indexId, IndexColumn[] cols,
            IndexType indexType, boolean create, String indexComment, DbObjectLock lock) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        Index index;
        int mainIndexColumn = getMainIndexColumn(indexType, cols);
        if (indexType.isDelegate()) {
            index = createDelegateIndex(indexId, indexName, indexType, mainIndexColumn);
        } else {
            if (database.isStarting()) {
                if (database.getStorage(storageEngine).hasMap(getMapNameForIndex(indexId))) {
                    mainIndexColumn = -1;
                }
            } else if (primaryIndex.getRowCountMax() != 0) {
                mainIndexColumn = -1;
            }
            if (mainIndexColumn != -1) {
                index = createDelegateIndex(indexId, indexName, indexType, mainIndexColumn);
            } else if (indexType.isHash() && cols.length <= 1) {
                if (indexType.isUnique()) {
                    index = new UniqueHashIndex(this, indexId, indexName, indexType, cols);
                } else {
                    index = new NonUniqueHashIndex(this, indexId, indexName, indexType, cols);
                }
            } else {
                index = new StandardSecondaryIndex(session, this, indexId, indexName, indexType, cols);
            }
        }
        // 先加到indexesExcludeDelegate中，新记录可以直接写入，但是不能通过它查询
        if (!(index instanceof StandardDelegateIndex))
            indexesExcludeDelegate.add(index);
        index.setTemporary(isTemporary());
        if (index.needRebuild()) {
            new IndexRebuilder(session, this, index).rebuild();
        }
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                schema.add(session, index, lock);
            }
        }
        indexes.add(index); // 索引rebuild完成后再加入indexes，此时可以通过索引查询了
        setModified();
        return index;
    }

    private StandardDelegateIndex createDelegateIndex(int indexId, String indexName, IndexType indexType,
            int mainIndexColumn) {
        primaryIndex.setMainIndexColumn(mainIndexColumn);
        return new StandardDelegateIndex(primaryIndex, this, indexId, indexName, indexType);
    }

    private int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (primaryIndex.getMainIndexColumn() != -1) {
            return -1;
        }
        if (!indexType.isPrimaryKey() || cols.length != 1) {
            return -1;
        }
        IndexColumn first = cols[0];
        if (first.sortType != SortOrder.ASCENDING) {
            return -1;
        }
        switch (first.column.getType()) {
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
            break;
        default:
            return -1;
        }
        return first.column.getColumnId();
    }

    // 向多个索引异步执行add/update/remove记录时，如果其中之一出错了，其他的就算成功了也不能当成最终的回调结果，而是取第一个异常
    private AsyncHandler<AsyncResult<Integer>> createHandler(ServerSession session,
            AsyncCallback<Integer> ac, AtomicInteger count, AtomicBoolean isFailed) {
        return ar -> {
            if (ar.isFailed() && isFailed.compareAndSet(false, true)) {
                ac.setAsyncResult(ar);
            }
            if (count.decrementAndGet() == 0 && !isFailed.get()) {
                ac.setAsyncResult(ar);
                analyzeIfRequired(session);
            }
        };
    }

    private void analyzeIfRequired(ServerSession session) {
        if (tableAnalyzer != null)
            tableAnalyzer.analyzeIfRequired(session);
    }

    @Override
    public void analyze(ServerSession session, int sample) {
        if (tableAnalyzer != null)
            tableAnalyzer.analyze(session, sample);
    }

    private AsyncCallback<Integer> createAsyncCallbackForAddRow(ServerSession session, Row row) {
        AsyncCallback<Integer> ac = session.createCallback();
        if (containsLargeObject()) {
            // 增加row全部成功后再连接大对象
            AsyncCallback<Integer> acLob = session.createCallback();
            acLob.onComplete(ar -> {
                if (ar.isSucceeded()) {
                    primaryIndex.onAddSucceeded(session, row);
                }
                ac.setAsyncResult(ar);
            });
            return acLob;
        }
        return ac;
    }

    @Override
    public Future<Integer> addRow(ServerSession session, Row row) {
        row.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        AsyncCallback<Integer> ac = createAsyncCallbackForAddRow(session, row);
        int size = indexesExcludeDelegate.size();
        AtomicInteger count = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean();

        if (primaryIndex.containsMainIndexColumn()) {
            // 第一个是PrimaryIndex
            for (int i = 0; i < size && !isFailed.get(); i++) {
                Index index = indexesExcludeDelegate.get(i);
                index.add(session, row).onComplete(createHandler(session, ac, count, isFailed));
            }
        } else {
            // 如果表没有主键，需要等primaryIndex写成功得到一个row id后才能写其他索引
            primaryIndex.add(session, row).onComplete(ar -> {
                if (ar.isSucceeded()) {
                    if (count.decrementAndGet() == 0) {
                        ac.setAsyncResult(ar);
                        analyzeIfRequired(session);
                        return;
                    }
                    for (int i = 1; i < size && !isFailed.get(); i++) {
                        Index index = indexesExcludeDelegate.get(i);
                        index.add(session, row).onComplete(createHandler(session, ac, count, isFailed));
                    }
                } else {
                    ac.setAsyncResult(ar.getCause());
                }
            });
        }
        return ac;
    }

    @Override
    public Future<Integer> updateRow(ServerSession session, Row oldRow, Row newRow, int[] updateColumns,
            boolean isLockedBySelf) {
        newRow.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        AsyncCallback<Integer> ac = session.createCallback();
        int size = indexesExcludeDelegate.size();
        AtomicInteger count = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean();

        // 第一个是PrimaryIndex
        for (int i = 0; i < size && !isFailed.get(); i++) {
            Index index = indexesExcludeDelegate.get(i);
            index.update(session, oldRow, newRow, updateColumns, isLockedBySelf)
                    .onComplete(createHandler(session, ac, count, isFailed));
        }
        return ac;
    }

    @Override
    public Future<Integer> removeRow(ServerSession session, Row row, boolean isLockedBySelf) {
        lastModificationId = database.getNextModificationDataId();
        AsyncCallback<Integer> ac = session.createCallback();
        int size = indexesExcludeDelegate.size();
        AtomicInteger count = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean();

        for (int i = size - 1; i >= 0 && !isFailed.get(); i--) {
            Index index = indexesExcludeDelegate.get(i);
            index.remove(session, row, isLockedBySelf)
                    .onComplete(createHandler(session, ac, count, isFailed));
        }
        return ac;
    }

    @Override
    public int tryLockRow(ServerSession session, Row row, int[] lockColumns) {
        // 只锁主索引即可
        return primaryIndex.tryLock(session, row, lockColumns);
    }

    @Override
    public void truncate(ServerSession session) {
        lastModificationId = database.getNextModificationDataId();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        if (tableAnalyzer != null)
            tableAnalyzer.reset();
        if (containsLargeObject())
            dataHandler.getLobStorage().removeAllForTable(getId());
    }

    @Override
    public void repair(ServerSession session) {
        lastModificationId = database.getNextModificationDataId();
        primaryIndex.repair(session);
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public TableType getTableType() {
        return TableType.STANDARD_TABLE;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        return primaryIndex;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId;
    }

    @Override
    public boolean containsLargeObject() {
        return largeObjectColumns != null;
    }

    @Override
    public boolean containsIndex() {
        return indexesExcludeDelegate.size() > 1;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return primaryIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return primaryIndex.getRowCountApproximation();
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
        if (containsLargeObject()) {
            if (dataHandler.isTableLobStorage()) {
                getDatabase().getTransactionEngine().removeGcTask(dataHandler.getLobStorage());
                dataHandler.getLobStorage().close();
            } else {
                dataHandler.getLobStorage().removeAllForTable(getId());
            }
        }
        super.removeChildrenAndResources(session, lock);
        // go backwards because database.removeIndex will
        // call table.removeIndex
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            if (index.getName() != null) {
                schema.remove(session, index, lock);
            }
            // needed for session temporary indexes
            indexes.remove(index);
            indexesExcludeDelegate.remove(index);
        }
        if (SysProperties.CHECK) {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObjectType.INDEX)) {
                Index index = (Index) obj;
                if (index.getTable() == this) {
                    DbException.throwInternalError("index not dropped: " + index.getName());
                }
            }
        }
        primaryIndex.remove(session);
        close(session);
    }

    @Override
    public void removeIndex(Index index) {
        super.removeIndex(index);
        indexesExcludeDelegate.remove(index);
    }

    @Override
    public long getDiskSpaceUsed() {
        long sum = 0;
        for (Index i : indexes) {
            sum += i.getDiskSpaceUsed();
        }
        return sum;
    }

    @Override
    public Column getRowIdColumn() {
        if (rowIdColumn == null) {
            rowIdColumn = new Column(Column.ROWID, Value.LONG);
            rowIdColumn.setTable(this, -1);
        }
        return rowIdColumn;
    }

    @Override
    public String toString() {
        return getSQL();
    }

    // 只要组合数据库id和表或索引的id就能得到一个全局唯一的map名了
    public String getMapNameForTable(int id) {
        return getMapName("t", database.getId(), id);
    }

    public String getMapNameForIndex(int id) {
        return getMapName("i", database.getId(), id);
    }

    private static String getMapName(Object... args) {
        StringBuilder name = new StringBuilder();
        for (Object arg : args) {
            if (name.length() > 0)
                name.append(Constants.NAME_SEPARATOR);
            name.append(arg.toString());
        }
        return name.toString();
    }

    private Column[] oldColumns;

    @Override
    public Column[] getOldColumns() {
        return oldColumns;
    }

    @Override
    public void setNewColumns(Column[] columns) {
        this.oldColumns = this.columns;
        setColumns(columns);
    }

    @Override
    protected void setColumns(Column[] columns) {
        super.setColumns(columns);
        largeObjectColumns = null;
        ArrayList<Column> list = new ArrayList<>(1);
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType())) {
                list.add(col);
            }
        }
        if (!list.isEmpty()) {
            int size = list.size();
            largeObjectColumns = new int[size];
            for (int i = 0; i < size; i++)
                largeObjectColumns[i] = list.get(i).getColumnId();

            if (useTableLobStorage) {
                dataHandler = new TableDataHandler(this, getMapNameForTable(getId()));
                database.addDataHandler(getId(), dataHandler);
            } else {
                dataHandler = database;
            }
        } else {
            dataHandler = database;
        }
    }

    public int[] getLargeObjectColumns() {
        return largeObjectColumns;
    }

    @Override
    public DataHandler getDataHandler() {
        return dataHandler;
    }

    public EnumColumn[] getEnumColumns() {
        EnumColumn[] enumColumns = null;
        for (int i = 0, len = columns.length; i < len; i++) {
            if (columns[i].isEnumType()) {
                if (enumColumns == null)
                    enumColumns = new EnumColumn[len];
                enumColumns[i] = (EnumColumn) columns[i];
            }
        }
        return enumColumns;
    }
}
