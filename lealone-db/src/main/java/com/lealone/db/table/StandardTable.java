/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.table;

import java.util.ArrayList;
import java.util.List;
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
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.constraint.Constraint;
import com.lealone.db.constraint.ConstraintReferential;
import com.lealone.db.index.Index;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexOperator;
import com.lealone.db.index.IndexOperator.IndexOperation;
import com.lealone.db.index.IndexRebuilder;
import com.lealone.db.index.IndexType;
import com.lealone.db.index.hash.NonUniqueHashIndex;
import com.lealone.db.index.hash.UniqueHashIndex;
import com.lealone.db.index.standard.StandardDelegateIndex;
import com.lealone.db.index.standard.StandardPrimaryIndex;
import com.lealone.db.index.standard.StandardSecondaryIndex;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.result.SortOrder;
import com.lealone.db.row.Row;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.schema.SchemaObject;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column.EnumColumn;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.storage.StorageEngine;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageSetting;
import com.lealone.transaction.TransactionEngine;
import com.lealone.transaction.TransactionEngine.GcTask;

/**
 * @author H2 Group
 * @author zhh
 */
public class StandardTable extends Table {

    // add或remove时会copy一份
    private ArrayList<Index> indexes = Utils.newSmallArrayList();
    // 以下两个都不包含Delegate索引
    private ArrayList<Index> indexesSync = Utils.newSmallArrayList(); // 只包含unique和primary
    private ArrayList<Index> indexesAsync = Utils.newSmallArrayList();

    private final StandardPrimaryIndex primaryIndex;
    private final StorageEngine storageEngine;
    private final Map<String, String> parameters;
    private final boolean globalTemporary;
    private final TableAnalyzer tableAnalyzer;

    private long lastModificationId;
    private Column rowIdColumn;
    private int[] largeObjectColumns;
    private DataHandler dataHandler;
    private final boolean useTableLobStorage;

    private final ArrayList<IndexOperator> indexOperators = Utils.newSmallArrayList();

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
        indexesSync.add(primaryIndex);
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
        return new Row(data);
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
        return primaryIndex.getRow(oldRow, oldRow.getKey());
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
        index.setTemporary(isTemporary());
        // 先加到indexesSync或indexesAsync中，新记录可以直接写入
        if (!indexType.isDelegate()) {
            if (indexType.isUnique()) {
                indexesSync = copyOnAdd(indexesSync, index);
            } else {
                indexesAsync = copyOnAdd(indexesAsync, index);
                addIndexOperator(session, index);
            }
        }
        // 可以边构建边查询
        if (index.needRebuild() && getRowCountApproximation() > 0) {
            new IndexRebuilder(session, this, index).rebuild();
        }
        if (indexType.isDelegate() || index.getCreateSQL() != null) {
            index.setComment(indexComment);
            boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                schema.add(session, index, lock);
            }
        }
        indexes = copyOnAdd(indexes, index); // 包含Delegate索引
        setModified();
        return index;
    }

    private void addIndexOperator(ServerSession session, Index index) {
        InternalScheduler scheduler = (InternalScheduler) session.getScheduler().getSchedulerFactory()
                .getScheduler();
        IndexOperator operator = new IndexOperator(scheduler, this, index);
        indexOperators.add(operator);
        index.setIndexOperator(operator);
    }

    public List<IndexOperator> getIndexOperators() {
        return indexOperators;
    }

    private StandardDelegateIndex createDelegateIndex(int indexId, String indexName, IndexType indexType,
            int mainIndexColumn) {
        indexType.setDelegate(true);
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
    private AsyncResultHandler<Integer> createHandler(ServerSession session,
            AsyncResultHandler<Integer> topHandler, AtomicInteger count, AtomicBoolean isFailed,
            IndexOperation io) {
        return ar -> {
            if (ar.isSucceeded()) {
                if (count.decrementAndGet() == 0 && !isFailed.get()) {
                    if (io != null)
                        IndexOperator.addIndexOperation(session, this, io);
                    topHandler.handle(ar);
                    analyzeIfRequired(session);
                }
            } else if (isFailed.compareAndSet(false, true)) {
                topHandler.handle(ar);
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

    @Override
    public void addRow(ServerSession session, Row row, AsyncResultHandler<Integer> handler) {
        row.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        if (containsLargeObject()) {
            AsyncResultHandler<Integer> topHandler = handler;
            // 增加row全部成功后再连接大对象
            handler = ar -> {
                if (ar.isSucceeded()) {
                    primaryIndex.onAddSucceeded(session, row);
                }
                topHandler.handle(ar);
            };
        }
        ArrayList<Index> oldIndexes = indexesSync;
        int size = oldIndexes.size();
        AtomicInteger count = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean();

        final IndexOperation io;
        if (!indexesAsync.isEmpty()) {
            io = IndexOperator.addRowLazy(row.getKey(), row.getColumns());
            io.setTransaction(session.getTransaction());
        } else {
            io = null;
        }

        AsyncResultHandler<Integer> topHandler = handler;
        if (primaryIndex.containsMainIndexColumn()) {
            // 第一个是PrimaryIndex
            for (int i = 0; i < size && !isFailed.get(); i++) {
                Index index = oldIndexes.get(i);
                index.add(session, row, createHandler(session, topHandler, count, isFailed, io));
            }
        } else {
            // 如果表没有主键，需要等primaryIndex写成功得到一个row id后才能写其他索引
            primaryIndex.add(session, row, ar -> {
                if (ar.isSucceeded()) {
                    if (count.decrementAndGet() == 0) {
                        if (io != null)
                            IndexOperator.addIndexOperation(session, this, io);
                        topHandler.handle(ar);
                        analyzeIfRequired(session);
                        return;
                    }
                    for (int i = 1; i < size && !isFailed.get(); i++) {
                        Index index = oldIndexes.get(i);
                        index.add(session, row, createHandler(session, topHandler, count, isFailed, io));
                    }
                } else {
                    topHandler.handle(ar);
                }
            });
        }

        // 看看有没有刚刚创建的索引，如果有就让它也写入新记录
        ArrayList<Index> newIndexes = indexesSync;
        if (oldIndexes != newIndexes) {
            for (Index index : getNewIndexes(oldIndexes, newIndexes)) {
                index.add(session, row, AsyncResultHandler.emptyHandler());
            }
        }
    }

    @Override
    public void updateRow(ServerSession session, Row oldRow, Row newRow, int[] updateColumns,
            boolean isLockedBySelf, AsyncResultHandler<Integer> handler) {
        newRow.setVersion(getVersion());
        lastModificationId = database.getNextModificationDataId();
        ArrayList<Index> oldIndexes = indexesSync;
        int size = oldIndexes.size();
        AtomicInteger count = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean();
        Value[] oldColumns = oldRow.getColumns(); // 会改变，所以提前保留旧的

        IndexOperation io = null;
        if (!indexesAsync.isEmpty()) {
            io = IndexOperator.updateRowLazy(oldRow.getKey(), newRow.getKey(), oldColumns,
                    newRow.getColumns(), updateColumns);
            io.setTransaction(session.getTransaction());
        }

        // 第一个是PrimaryIndex
        for (int i = 0; i < size && !isFailed.get(); i++) {
            Index index = oldIndexes.get(i);
            index.update(session, oldRow, newRow, oldColumns, updateColumns, isLockedBySelf,
                    createHandler(session, handler, count, isFailed, io));
        }

        // 看看有没有刚刚创建的索引，如果有也更新它
        ArrayList<Index> newIndexes = indexesSync;
        if (oldIndexes != newIndexes) {
            for (Index index : getNewIndexes(oldIndexes, newIndexes)) {
                index.update(session, oldRow, newRow, oldColumns, updateColumns, isLockedBySelf,
                        AsyncResultHandler.emptyHandler());
            }
        }
    }

    @Override
    public void removeRow(ServerSession session, Row row, boolean isLockedBySelf,
            AsyncResultHandler<Integer> handler) {
        lastModificationId = database.getNextModificationDataId();
        ArrayList<Index> oldIndexes = indexesSync;
        int size = oldIndexes.size();
        AtomicInteger count = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean();
        Value[] oldColumns = row.getColumns(); // 会改变，所以提前保留旧的

        IndexOperation io = null;
        if (!indexesAsync.isEmpty()) {
            io = IndexOperator.removeRowLazy(row.getKey(), row.getColumns());
            io.setTransaction(session.getTransaction());
        }

        for (int i = size - 1; i >= 0 && !isFailed.get(); i--) {
            Index index = oldIndexes.get(i);
            index.remove(session, row, oldColumns, isLockedBySelf,
                    createHandler(session, handler, count, isFailed, io));
        }

        // 看看有没有刚刚创建的索引，如果有也删除它的记录
        ArrayList<Index> newIndexes = indexesSync;
        if (oldIndexes != newIndexes) {
            for (Index index : getNewIndexes(oldIndexes, newIndexes)) {
                index.remove(session, row, oldColumns, isLockedBySelf,
                        AsyncResultHandler.emptyHandler());
            }
        }
    }

    @Override
    public int tryLockRow(ServerSession session, Row row) {
        // 只锁主索引即可
        return primaryIndex.tryLock(session, row);
    }

    @Override
    public void truncate(ServerSession session) {
        lastModificationId = database.getNextModificationDataId();
        ArrayList<Index> oldIndexes = indexes;
        for (int i = oldIndexes.size() - 1; i >= 0; i--) {
            Index index = oldIndexes.get(i);
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
    public void recover() {
        ArrayList<StorageMap<?, ?>> indexMaps = null;
        ArrayList<Index> indexes = this.indexes;
        int size = indexes.size();
        if (size > 1) {
            indexMaps = new ArrayList<>(size - 1);
            for (Index index : indexes) {
                if (index instanceof StandardSecondaryIndex) {
                    indexMaps.add(((StandardSecondaryIndex) index).getDataMap().getRawMap());
                }
            }
        }
        TransactionEngine transactionEngine = database.getTransactionEngine();
        transactionEngine.recover(primaryIndex.getDataMap().getRawMap(), indexMaps);
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
    public StandardPrimaryIndex getScanIndex(ServerSession session) {
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
        return indexes.size() > 1;
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
                getDatabase().getTransactionEngine().removeGcTask((GcTask) dataHandler.getLobStorage());
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
            indexes = copyOnRemove(indexes, index);
            indexesSync = copyOnRemove(indexesSync, index);
            indexesAsync = copyOnRemove(indexesAsync, index);
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
        indexesSync = copyOnRemove(indexesSync, index);
        indexesAsync = copyOnRemove(indexesAsync, index);
    }

    @Override
    public long getDiskSpaceUsed() {
        long sum = 0;
        ArrayList<Index> indexes = this.indexes;
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

    private static ArrayList<Index> copyOnAdd(ArrayList<Index> oldIndexes, Index newIndex) {
        ArrayList<Index> newIndexes = new ArrayList<>(oldIndexes.size() + 1);
        newIndexes.addAll(oldIndexes);
        newIndexes.add(newIndex);
        return newIndexes;
    }

    private static ArrayList<Index> copyOnRemove(ArrayList<Index> oldIndexes, Index oldIndex) {
        if (oldIndexes.isEmpty())
            return oldIndexes;
        ArrayList<Index> newIndexes = new ArrayList<>(oldIndexes.size() - 1);
        newIndexes.addAll(oldIndexes);
        newIndexes.remove(oldIndex);
        return newIndexes;
    }

    private static ArrayList<Index> getNewIndexes(ArrayList<Index> oldIndexes,
            ArrayList<Index> currentIndexes) {
        ArrayList<Index> newIndexes = new ArrayList<>(currentIndexes);
        newIndexes.removeAll(oldIndexes);
        return newIndexes;
    }
}
