/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipOutputStream;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.Trace;
import com.lealone.common.trace.TraceModuleType;
import com.lealone.common.trace.TraceSystem;
import com.lealone.common.util.BitField;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.ShutdownHookUtils;
import com.lealone.common.util.StatementBuilder;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.TempFileDeleter;
import com.lealone.common.util.Utils;
import com.lealone.db.api.DatabaseEventListener;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.auth.Role;
import com.lealone.db.auth.User;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.Index;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.result.Row;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerLock;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.SchemaObject;
import com.lealone.db.schema.Sequence;
import com.lealone.db.schema.TriggerObject;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionStatus;
import com.lealone.db.stat.QueryStatisticsData;
import com.lealone.db.table.Column;
import com.lealone.db.table.CreateTableData;
import com.lealone.db.table.InfoMetaTable;
import com.lealone.db.table.PerfMetaTable;
import com.lealone.db.table.Table;
import com.lealone.db.table.TableAlterHistory;
import com.lealone.db.table.TableView;
import com.lealone.db.util.SourceCompiler;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.net.NetNode;
import com.lealone.sql.SQLEngine;
import com.lealone.sql.SQLParser;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageBase;
import com.lealone.storage.StorageBuilder;
import com.lealone.storage.StorageEngine;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.fs.FileUtils;
import com.lealone.storage.lob.LobStorage;
import com.lealone.transaction.TransactionEngine;

/**
 * There is one database object per open database.
 *
 * @author H2 Group
 * @author zhh
 */
public class Database extends DbObjectBase implements DataHandler {

    /**
     * The default name of the system user. This name is only used as long as
     * there is no administrator user registered.
     */
    private static final String SYSTEM_USER_NAME = "DBA";

    private static enum State {
        CONSTRUCTOR_CALLED, // 刚调用完构造函数阶段，也是默认阶段
        OPENED,
        STARTING,
        STARTED,
        CLOSING,
        CLOSED,
        POWER_OFF
    }

    private volatile State state = State.CONSTRUCTOR_CALLED;

    private final TransactionalDbObjects[] dbObjectsArray //
            = new TransactionalDbObjects[DbObjectType.TYPES.length];

    // 与users、roles和rights相关的操作都用这个对象进行同步
    private final DbObjectLock authLock = new DbObjectLock(DbObjectType.USER);
    private final DbObjectLock schemasLock = new DbObjectLock(DbObjectType.SCHEMA);
    private final DbObjectLock commentsLock = new DbObjectLock(DbObjectType.COMMENT);
    private final DbObjectLock databasesLock = new DbObjectLock(DbObjectType.DATABASE);
    private final DbObjectLock pluginsLock = new DbObjectLock(DbObjectType.PLUGIN);

    public DbObjectLock tryExclusiveAuthLock(ServerSession session) {
        return tryExclusiveLock(session, authLock);
    }

    public DbObjectLock tryExclusiveSchemaLock(ServerSession session) {
        return tryExclusiveLock(session, schemasLock);
    }

    public DbObjectLock tryExclusiveCommentLock(ServerSession session) {
        return tryExclusiveLock(session, commentsLock);
    }

    public DbObjectLock tryExclusiveDatabaseLock(ServerSession session) {
        return tryExclusiveLock(session, databasesLock);
    }

    public DbObjectLock tryExclusivePluginLock(ServerSession session) {
        return tryExclusiveLock(session, pluginsLock);
    }

    private DbObjectLock tryExclusiveLock(ServerSession session, DbObjectLock lock) {
        return lock.tryExclusiveLock(session) ? lock : null;
    }

    private final SchedulerLock schedulerLock = new SchedulerLock();

    public SchedulerLock getSchedulerLock() {
        return schedulerLock;
    }

    private final Set<ServerSession> userSessions = Collections.synchronizedSet(new HashSet<>());
    private LinkedList<ServerSession> waitingSessions;
    private ServerSession exclusiveSession;
    private ServerSession systemSession;
    private User systemUser;
    private Role publicRole;
    private Schema mainSchema;
    private Schema infoSchema;
    private Schema perfSchema;
    private volatile boolean infoSchemaMetaTablesInitialized;

    private int nextSessionId;

    private final BitField objectIds = new BitField();

    private final AtomicLong modificationDataId = new AtomicLong();
    private final AtomicLong modificationMetaId = new AtomicLong();

    private Table meta;
    private String metaStorageEngineName;
    private Index metaIdIndex;

    private TraceSystem traceSystem;
    private Trace trace;

    private boolean readOnly;

    private int closeDelay;
    private long lastSessionRemovedAt = -1;
    private Thread closeOnExitHook;

    private final TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
    private Mode mode = Mode.getDefaultMode();
    private CompareMode compareMode;

    private SourceCompiler compiler;
    private DatabaseEventListener eventListener;
    private QueryStatisticsData queryStatisticsData;

    private final boolean persistent;
    private final Map<String, String> parameters;
    private volatile DbSettings dbSettings;

    // 每个数据库只有一个SQL引擎和一个事务引擎
    private final SQLEngine sqlEngine;
    private final TransactionEngine transactionEngine;

    private final ConcurrentHashMap<String, Storage> storages = new ConcurrentHashMap<>();
    private final String storagePath; // 不使用原始的名称，而是用id替换数据库名
    private LobStorage lobStorage;

    private RunMode runMode = RunMode.CLIENT_SERVER;
    private ConnectionInfo lastConnectionInfo;

    private final TableAlterHistory tableAlterHistory = new TableAlterHistory();
    private final ConcurrentHashMap<Integer, DataHandler> dataHandlers = new ConcurrentHashMap<>();

    private String[] hostIds;
    private HashSet<NetNode> nodes;
    private String targetNodes;

    public Database(int id, String name, Map<String, String> parameters) {
        super(id, name);
        storagePath = getStoragePath();
        if (parameters != null) {
            dbSettings = DbSettings.getInstance(parameters);
            this.parameters = parameters;
        } else {
            dbSettings = DbSettings.getDefaultSettings();
            this.parameters = new CaseInsensitiveMap<>();
        }
        persistent = dbSettings.persistent;
        closeDelay = dbSettings.dbCloseDelay; // 默认是-1不关闭
        compareMode = CompareMode.getInstance(null, 0, false);
        if (dbSettings.mode != null) {
            mode = Mode.getInstance(dbSettings.mode);
        }

        sqlEngine = PluggableEngine.getEngine(SQLEngine.class, dbSettings.defaultSQLEngine);
        transactionEngine = PluggableEngine.getEngine(TransactionEngine.class,
                dbSettings.defaultTransactionEngine);

        for (DbObjectType type : DbObjectType.TYPES) {
            if (!type.isSchemaObject) {
                dbObjectsArray[type.value] = new TransactionalDbObjects();
            }
        }
        setDatabase(this);
    }

    // ----------- 以下是 DbObjectBase API 实现 -----------

    @Override
    public DbObjectType getType() {
        return DbObjectType.DATABASE;
    }

    @Override
    public List<? extends DbObject> getChildren() {
        return getAllSchemaObjects();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(true);
    }

    public String getCreateSQL(boolean ifNotExists) {
        return getCreateSQL(quoteIdentifier(name), parameters, runMode, ifNotExists);
    }

    @Override
    public boolean isTemporary() {
        return !persistent || super.isTemporary();
    }

    // ----------- 以下是 DataHandler API 实现 -----------

    @Override
    public String getDatabasePath() {
        return persistent ? getStoragePath() : null;
    }

    @Override
    public FileStorage openFile(String name, String openMode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        return FileStorage.open(this, name, openMode, dbSettings.cipher, dbSettings.filePasswordHash);
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return tempFileDeleter;
    }

    @Override
    public void checkPowerOff() {
        if (state == State.POWER_OFF)
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
    }

    public boolean isPowerOff() {
        return state == State.POWER_OFF;
    }

    @Override
    public void checkWritingAllowed() {
        if (readOnly) {
            throw DbException.get(ErrorCode.DATABASE_IS_READ_ONLY);
        }
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return persistent ? dbSettings.maxLengthInplaceLob : Integer.MAX_VALUE;
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return dbSettings.lobCompressionAlgorithm;
    }

    @Override
    public LobStorage getLobStorage() {
        return lobStorage;
    }

    public void setLobStorage(LobStorage lobStorage) {
        if (lobStorage != null) {
            this.lobStorage = lobStorage;
        }
    }

    // ----------- END -----------

    public String getShortName() {
        return getName();
    }

    public DbSettings getSettings() {
        return dbSettings;
    }

    public boolean setDbSetting(ServerSession session, DbSetting key, String value) {
        Map<String, String> newSettings = new CaseInsensitiveMap<>(1);
        newSettings.put(key.getName(), value);
        return updateDbSettings(session, newSettings);
    }

    public boolean updateDbSettings(ServerSession session, Map<String, String> newSettings) {
        boolean changed = false;
        Map<String, String> oldSettings = new CaseInsensitiveMap<>(dbSettings.getSettings());
        for (Map.Entry<String, String> e : newSettings.entrySet()) {
            String key = e.getKey();
            String newValue = e.getValue();
            String oldValue = oldSettings.get(key);
            if (oldValue == null || !oldValue.equals(newValue)) {
                changed = true;
                oldSettings.put(key, newValue);
            }
        }
        if (changed) {
            dbSettings = DbSettings.getInstance(oldSettings);
            parameters.putAll(newSettings);
            LealoneDatabase.getInstance().updateMeta(session, this);
        }
        return changed;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public SQLEngine getSQLEngine() {
        return sqlEngine;
    }

    public TransactionEngine getTransactionEngine() {
        return transactionEngine;
    }

    public SQLParser createParser(Session session) {
        return sqlEngine.createParser(session);
    }

    public String quoteIdentifier(String identifier) {
        return sqlEngine.quoteIdentifier(identifier);
    }

    public String getDefaultStorageEngineName() {
        return dbSettings.defaultStorageEngine;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setRunMode(RunMode runMode) {
        if (runMode != null) {
            this.runMode = runMode;
        }
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public synchronized Database copy() {
        return copy(true);
    }

    public synchronized Database copy(boolean init) {
        Database db = new Database(id, name, parameters);
        // 因为每个存储只能打开一次，所以要复用原有存储
        db.storages.putAll(storages);
        db.runMode = runMode;
        db.hostIds = hostIds;
        db.nodes = nodes;
        db.targetNodes = targetNodes;
        db.lastConnectionInfo = lastConnectionInfo;
        if (init) {
            db.init();
            LealoneDatabase.getInstance().getDatabasesMap().put(name, db);
            for (ServerSession s : userSessions) {
                db.userSessions.add(s);
                s.setDatabase(db);
            }
        }
        return db;
    }

    public boolean isInitialized() {
        return state == State.OPENED;
    }

    public synchronized void init() {
        if (state == State.OPENED)
            return;

        String listener = dbSettings.eventListener;
        if (listener != null) {
            listener = StringUtils.trim(listener, true, true, "'");
            setEventListenerClass(listener);
        }

        initTraceSystem();
        openDatabase();
        addShutdownHook();

        // 用户也可以在LealoneDatabase的public模式中建表修改表结构
        tableAlterHistory.init(getInternalConnection());
        // 提前初始化表的版本号，避免在执行insert/update时用同步的方式加载
        for (Table t : mainSchema.getAllTablesAndViews())
            t.initVersion();

        if (eventListener != null) {
            eventListener.opened();
        }
        state = State.OPENED;
    }

    private void initTraceSystem() {
        if (persistent) {
            traceSystem = new TraceSystem(getStoragePath() + Constants.SUFFIX_TRACE_FILE);
            traceSystem.setLevelFile(dbSettings.traceLevelFile);
        } else {
            // 内存数据库不需要写跟踪文件，但是可以输出到控制台
            traceSystem = new TraceSystem();
        }
        traceSystem.setLevelSystemOut(dbSettings.traceLevelSystemOut);
        trace = traceSystem.getTrace(TraceModuleType.DATABASE);
        trace.info("opening {0} (build {1}) (persistent: {2})", name, Constants.BUILD_ID, persistent);
    }

    private void addShutdownHook() {
        if (dbSettings.dbCloseOnExit) {
            try {
                closeOnExitHook = ShutdownHookUtils.addShutdownHook(getName(), () -> close(true));
            } catch (IllegalStateException | SecurityException e) {
                // shutdown in progress - just don't register the handler
                // (maybe an application wants to write something into a
                // database at shutdown time)
            }
        }
    }

    private void openDatabase() {
        try {
            // 初始化traceSystem后才能做下面这些
            systemUser = new User(this, 0, SYSTEM_USER_NAME, true);
            systemUser.setAdmin(true);

            publicRole = new Role(this, 0, Constants.PUBLIC_ROLE_NAME, true);
            addDatabaseObject(null, publicRole, null);

            mainSchema = new Schema(this, 0, Constants.SCHEMA_MAIN, systemUser, true);
            infoSchema = new Schema(this, -1, "INFORMATION_SCHEMA", systemUser, true);
            perfSchema = new Schema(this, -2, "PERFORMANCE_SCHEMA", systemUser, true);
            addDatabaseObject(null, mainSchema, null);
            addDatabaseObject(null, infoSchema, null);
            addDatabaseObject(null, perfSchema, null);

            systemSession = new ServerSession(this, systemUser, ++nextSessionId);
            setSessionScheduler(systemSession, null);

            // 在一个新事务中打开sys(meta)表
            systemSession.setAutoCommit(false);
            systemSession.getTransaction();
            openMetaTable();
            systemSession.commit();
            systemSession.setAutoCommit(true);
            trace.info("opened {0}", name);
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                e.fillInStackTrace();
            }
            if (traceSystem != null) {
                trace.error(e, "opening {0}", name);
                traceSystem.close();
            }
            closeSystemSession();
            throw DbException.convert(e);
        }
    }

    private void openMetaTable() {
        // sys(meta)表的id固定是0，其他数据库对象的id从1开始
        int sysTableId = 0;
        CreateTableData data = new CreateTableData();
        ArrayList<Column> cols = data.columns;
        Column columnId = new Column("ID", Value.INT);
        columnId.setNullable(false);
        cols.add(columnId);
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("SQL", Value.STRING));
        data.tableName = "SYS";
        data.id = sysTableId;
        data.persistData = persistent;
        data.persistIndexes = persistent;
        data.create = true;
        data.isHidden = true;
        data.session = systemSession;
        data.storageEngineName = metaStorageEngineName = getDefaultStorageEngineName();
        data.storageEngineParams = new CaseInsensitiveMap<>(1);
        data.storageEngineParams.put(StorageSetting.RUN_MODE.name(), RunMode.CLIENT_SERVER.name());
        meta = infoSchema.createTable(data);
        objectIds.set(sysTableId); // 此时正处于初始化阶段，只有一个线程在访问，所以不需要同步

        // 创建Delegate索引， 委派给原有的primary index(也就是ScanIndex)
        // Delegate索引不需要生成create语句保存到sys(meta)表的，这里只是把它加到schema和table的相应字段中
        // 这里也没有直接使用sys表的ScanIndex，因为id字段是主键
        IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
        IndexType indexType = IndexType.createDelegate();
        metaIdIndex = meta.addIndex(systemSession, "SYS_ID", sysTableId, pkCols, indexType, true, null,
                null);

        // 把sys(meta)表所有的create语句取出来，然后执行它们，在内存中构建出完整的数据库对象
        ArrayList<MetaRecord> records = new ArrayList<>();
        Cursor cursor = metaIdIndex.find(systemSession, null, null);
        while (cursor.next()) {
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            records.add(rec);
        }

        state = State.STARTING;

        // 会按DbObjectType的创建顺序排序
        Collections.sort(records);
        for (MetaRecord rec : records) {
            rec.execute(this, systemSession, eventListener);
        }

        recompileInvalidViews();
        state = State.STARTED;
    }

    private void recompileInvalidViews() {
        boolean recompileSuccessful;
        do {
            recompileSuccessful = false;
            for (Table obj : getAllTablesAndViews(false)) {
                if (obj instanceof TableView) {
                    TableView view = (TableView) obj;
                    if (view.isInvalid()) { // 这里是无效的要recompile
                        view.recompile(systemSession, true);
                        if (!view.isInvalid()) {
                            recompileSuccessful = true;
                        }
                    }
                }
            }
        } while (recompileSuccessful);
        // when opening a database, views are initialized before indexes,
        // so they may not have the optimal plan yet
        // this is not a problem, it is just nice to see the newest plan
        for (Table obj : getAllTablesAndViews(false)) {
            if (obj instanceof TableView) {
                TableView view = (TableView) obj;
                if (!view.isInvalid()) { // 这里是有效的要recompile
                    view.recompile(systemSession, true);
                }
            }
        }
    }

    /**
     * Check if two values are equal with the current comparison mode.
     *
     * @param a the first value
     * @param b the second value
     * @return true if both objects are equal
     */
    public boolean areEqual(Value a, Value b) {
        // can not use equals because ValueDecimal 0.0 is not equal to 0.00.
        return a.compareTo(b, compareMode) == 0;
    }

    /**
     * Compare two values with the current comparison mode. The values may not be of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and 1 otherwise
     */
    public int compare(Value a, Value b) {
        return a.compareTo(b, compareMode);
    }

    /**
     * Compare two values with the current comparison mode. The values must be of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and 1 otherwise
     */
    public int compareTypeSafe(Value a, Value b) {
        return a.compareTypeSafe(b, compareMode);
    }

    public long getModificationDataId() {
        return modificationDataId.get();
    }

    public long getNextModificationDataId() {
        return modificationDataId.incrementAndGet();
    }

    public long getModificationMetaId() {
        return modificationMetaId.get();
    }

    public long getNextModificationMetaId() {
        // if the meta data has been modified, the data is modified as well
        // (because MetaTable returns modificationDataId)
        modificationDataId.incrementAndGet();
        return modificationMetaId.getAndIncrement();
    }

    /**
     * Get the trace object for the given module type.
     *
     * @param traceModuleType the module type
     * @return the trace object
     */
    public Trace getTrace(TraceModuleType traceModuleType) {
        return traceSystem.getTrace(traceModuleType);
    }

    /**
     * Check if the file password hash is correct.
     *
     * @param testCipher the cipher algorithm
     * @param testHash the hash code
     * @return true if the cipher algorithm and the password match
     */
    public boolean validateFilePasswordHash(String testCipher, byte[] testHash) {
        if (!StringUtils.equals(testCipher, dbSettings.cipher)) {
            return false;
        }
        return Utils.compareSecure(testHash, dbSettings.filePasswordHash);
    }

    private void initInfoSchemaMetaTables() {
        if (infoSchemaMetaTablesInitialized) {
            return;
        }
        synchronized (infoSchema) {
            if (!infoSchemaMetaTablesInitialized) {
                for (int type = 0, count = InfoMetaTable.getMetaTableTypeCount(); type < count; type++) {
                    InfoMetaTable m = new InfoMetaTable(infoSchema, -1 - type, type);
                    infoSchema.add(null, m, null);
                }
                for (int type = 0, count = PerfMetaTable.getMetaTableTypeCount(); type < count; type++) {
                    PerfMetaTable m = new PerfMetaTable(perfSchema, -1 - type, type);
                    perfSchema.add(null, m, null);
                }
                infoSchemaMetaTablesInitialized = true;
            }
        }
    }

    /**
     * Allocate a new object id.
     *
     * @return the id
     */
    public int allocateObjectId() {
        synchronized (objectIds) {
            int i = objectIds.nextClearBit(0);
            objectIds.set(i);
            return i;
        }
    }

    public void clearObjectId(int id) {
        synchronized (objectIds) {
            objectIds.clear(id);
        }
    }

    public boolean isObjectIdEnabled(int id) {
        synchronized (objectIds) {
            return objectIds.get(id);
        }
    }

    public Cursor getMetaCursor(ServerSession session) {
        return metaIdIndex.find(session, null, null);
    }

    public Row findMeta(ServerSession session, int id) {
        return metaIdIndex.getRow(session, id);
    }

    public Row tryLockDbObject(ServerSession session, DbObject obj, int errorCode) {
        int id = obj.getId();
        Row row = findMeta(session, id);
        if (row == null)
            throw DbException.get(errorCode, obj.getName());
        if (meta.tryLockRow(session, row, null) > 0)
            return row;
        else
            return null;
    }

    public void tryAddMeta(ServerSession session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && isMetaReady() && !obj.isTemporary()) {
            checkWritingAllowed();
            Row r = MetaRecord.getRow(meta, obj);
            meta.addRow(session, r);
        }
    }

    public void tryRemoveMeta(ServerSession session, SchemaObject obj, DbObjectLock lock) {
        if (isMetaReady()) {
            Table t = getDependentTable(obj, null);
            if (t != null && t != obj) {
                throw DbException.get(ErrorCode.CANNOT_DROP_2, obj.getSQL(), t.getSQL());
            }
        }
        tryRemoveMeta(session, obj.getId());
        Comment comment = findComment(session, obj);
        if (comment != null) {
            removeDatabaseObject(session, comment, lock);
        }
    }

    /**
     * Remove the given object from the meta data.
     *
     * @param session the session
     * @param id the id of the object to remove
     */
    private void tryRemoveMeta(ServerSession session, int id) {
        if (id > 0 && isMetaReady()) {
            checkWritingAllowed();
            Row row = findMeta(session, id);
            if (row != null)
                meta.removeRow(session, row);
        }
    }

    /**
     * Update an object in the system table.
     *
     * @param session the session
     * @param obj the database object
     */
    public void updateMeta(ServerSession session, DbObject obj) {
        updateMeta(session, obj, null);
    }

    public void updateMeta(ServerSession session, DbObject obj, Row oldRow) {
        int id = obj.getId();
        if (id > 0 && isMetaReady()) {
            checkWritingAllowed();
            boolean isLockedBySelf;
            if (oldRow != null) {
                isLockedBySelf = true;
            } else {
                isLockedBySelf = false;
                oldRow = findMeta(session, id);
            }
            if (oldRow != null) {
                Row newRow = MetaRecord.getRow(meta, obj);
                newRow.setKey(oldRow.getKey());
                Column sqlColumn = meta.getColumn(2);
                meta.updateRow(session, oldRow, newRow, new int[] { sqlColumn.getColumnId() },
                        isLockedBySelf);
                getNextModificationMetaId();
            }
        }
    }

    public void updateMetaAndFirstLevelChildren(ServerSession session, DbObject obj) {
        checkWritingAllowed();
        List<? extends DbObject> list = obj.getChildren();
        Comment comment = findComment(session, obj);
        if (comment != null) {
            DbException.throwInternalError();
        }
        updateMeta(session, obj);
        // remember that this scans only one level deep!
        if (list != null) {
            for (DbObject o : list) {
                if (o.getCreateSQL() != null) {
                    updateMeta(session, o);
                }
            }
        }
    }

    /**
     * Add an object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public void addDatabaseObject(ServerSession session, DbObject obj, DbObjectLock lock) {
        TransactionalDbObjects dbObjects = dbObjectsArray[obj.getType().value];

        if (SysProperties.CHECK && dbObjects.containsKey(session, obj.getName())) {
            DbException.throwInternalError("object already exists");
        }

        if (obj.getId() <= 0 || session == null || isStarting()) {
            dbObjects.add(obj);
            return;
        }

        tryAddMeta(session, obj);
        dbObjects.copyOnAdd(session, obj);

        if (lock != null) {
            lock.addHandler(ar -> {
                if (ar.isSucceeded() && ar.getResult()) {
                    dbObjects.commit();
                } else {
                    clearObjectId(obj.getId());
                    dbObjects.rollback();
                }
            });
        }
    }

    /**
     * Remove the object from the database.
     *
     * @param session the session
     * @param obj the object to remove
     */
    public void removeDatabaseObject(ServerSession session, DbObject obj, DbObjectLock lock) {
        checkWritingAllowed();
        String objName = obj.getName();
        DbObjectType type = obj.getType();
        TransactionalDbObjects dbObjects = dbObjectsArray[type.value];
        if (SysProperties.CHECK && !dbObjects.containsKey(session, objName)) {
            DbException.throwInternalError("not found: " + objName);
        }
        if (session == null) {
            dbObjects.remove(objName);
            removeInternal(obj);
            return;
        }
        Comment comment = findComment(session, obj);
        if (comment != null) {
            removeDatabaseObject(session, comment, lock);
        }
        int id = obj.getId();
        obj.removeChildrenAndResources(session, lock);
        tryRemoveMeta(session, id);
        dbObjects.copyOnRemove(session, objName);

        if (lock != null) {
            lock.addHandler(ar -> {
                if (ar.isSucceeded() && ar.getResult()) {
                    dbObjects.commit();
                    removeInternal(obj);
                } else {
                    dbObjects.rollback();
                }
            });
        }
    }

    private void removeInternal(DbObject obj) {
        clearObjectId(obj.getId());
        obj.invalidate();
    }

    /**
     * Rename a database object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public void renameDatabaseObject(ServerSession session, DbObject obj, String newName,
            DbObjectLock lock) {
        checkWritingAllowed();
        DbObjectType type = obj.getType();
        TransactionalDbObjects dbObjects = dbObjectsArray[type.value];
        String oldName = obj.getName();
        if (SysProperties.CHECK) {
            if (!dbObjects.containsKey(session, oldName)) {
                DbException.throwInternalError("not found: " + oldName);
            }
            if (oldName.equals(newName) || dbObjects.containsKey(session, newName)) {
                DbException.throwInternalError("object already exists: " + newName);
            }
        }

        obj.checkRename();
        dbObjects.copyOnRemove(session, oldName);
        obj.rename(newName);
        dbObjects.add(obj);

        lock.addHandler(ar -> {
            if (ar.isSucceeded() && ar.getResult()) {
                dbObjects.commit();
            } else {
                obj.rename(oldName);
                dbObjects.rollback();
            }
        });

        updateMetaAndFirstLevelChildren(session, obj);
    }

    /**
     * Get the comment for the given database object if one exists, or null if not.
     *
     * @param object the database object
     * @return the comment or null
     */
    public Comment findComment(ServerSession session, DbObject object) {
        if (object.getType() == DbObjectType.COMMENT) {
            return null;
        }
        String key = Comment.getKey(object);
        return find(DbObjectType.COMMENT, session, key);
    }

    @SuppressWarnings("unchecked")
    protected <T> HashMap<String, T> getDbObjects(DbObjectType type) {
        return (HashMap<String, T>) dbObjectsArray[type.value].getDbObjects();
    }

    @SuppressWarnings("unchecked")
    protected <T> T find(DbObjectType type, ServerSession session, String name) {
        return (T) dbObjectsArray[type.value].find(session, name);
    }

    /**
     * Get the role if it exists, or null if not.
     *
     * @param roleName the name of the role
     * @return the role or null
     */
    public Role findRole(ServerSession session, String roleName) {
        return find(DbObjectType.ROLE, session, roleName);
    }

    /**
     * Get the schema if it exists, or null if not.
     *
     * @param schemaName the name of the schema
     * @return the schema or null
     */
    public Schema findSchema(ServerSession session, String schemaName) {
        Schema schema = find(DbObjectType.SCHEMA, session, schemaName);
        if (isSystemSchema(schema)) {
            initInfoSchemaMetaTables();
        }
        return schema;
    }

    public boolean isSystemSchema(Schema schema) {
        return schema == infoSchema || schema == perfSchema;
    }

    /**
     * Get the user if it exists, or null if not.
     *
     * @param name the name of the user
     * @return the user or null
     */
    public User findUser(ServerSession session, String name) {
        return find(DbObjectType.USER, session, name);
    }

    /**
     * Get user with the given name. This method throws an exception if the user does not exist.
     *
     * @param name the user name
     * @return the user
     * @throws DbException if the user does not exist
     */
    public User getUser(ServerSession session, String name) {
        User user = findUser(session, name);
        if (user == null) {
            throw DbException.get(ErrorCode.USER_NOT_FOUND_1, name);
        }
        return user;
    }

    public Role getRole(ServerSession session, String name) {
        Role role = findRole(session, name);
        if (role == null) {
            throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, name);
        }
        return role;
    }

    private void setSessionScheduler(ServerSession session, Scheduler scheduler) {
        if (scheduler == null) {
            scheduler = SchedulerThread.currentScheduler();
            if (scheduler == null)
                DbException.throwInternalError();
        }
        session.setScheduler(scheduler);
        ConnectionInfo ci = session.getConnectionInfo();
        if (ci == null || ci.isEmbedded())
            scheduler.addSession(session);
    }

    /**
     * Create a session for the given user.
     *
     * @param user the user
     * @return the session
     * @throws DbException if the database is in exclusive mode
     */
    public ServerSession createSession(User user) {
        return createSession(user, null, null);
    }

    public synchronized ServerSession createSession(User user, ConnectionInfo ci) {
        return createSession(user, ci, null);
    }

    public ServerSession createSession(User user, Scheduler scheduler) {
        return createSession(user, null, scheduler);
    }

    // 创建session是低频且不耗时的操作，所以直接用synchronized即可，不必搞成异步增加复杂性
    public synchronized ServerSession createSession(User user, ConnectionInfo ci, Scheduler scheduler) {
        if (exclusiveSession != null) {
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        // systemUser不存在，执行CreateSchema会出错
        if (user == systemUser) {
            for (User u : getAllUsers()) {
                if (u.isAdmin()) {
                    user = u;
                    break;
                }
            }
        }
        ServerSession session = new ServerSession(this, user, ++nextSessionId);
        session.setConnectionInfo(ci);
        userSessions.add(session);
        session.getTrace().setType(TraceModuleType.DATABASE).info("connected session #{0} to {1}",
                session.getId(), name);
        lastSessionRemovedAt = -1;
        setSessionScheduler(session, scheduler);
        return session;
    }

    /**
     * Remove a session. This method is called after the user has disconnected.
     *
     * @param session the session
     */
    public synchronized void removeSession(ServerSession session) {
        if (session != null) {
            if (exclusiveSession == session) {
                setExclusiveSession(null, false);
            }
            userSessions.remove(session);
            if (session != systemSession && session.getTrace().isInfoEnabled()) {
                session.getTrace().setType(TraceModuleType.DATABASE).info("disconnected session #{0}",
                        session.getId());
            }
        }

        if (userSessions.isEmpty() && session != systemSession) {
            if (closeDelay == 0) {
                close(false);
            } else if (closeDelay < 0) {
                return;
            } else {
                lastSessionRemovedAt = System.currentTimeMillis();
            }
        }
    }

    private synchronized void closeAllSessionsException(ServerSession except) {
        ServerSession[] all = new ServerSession[userSessions.size()];
        userSessions.toArray(all);
        for (ServerSession s : all) {
            if (s != except) {
                try {
                    // must roll back, otherwise the session is removed and
                    // the transaction log that contains its uncommitted operations as well
                    s.rollback();
                    s.close();
                } catch (DbException e) {
                    trace.error(e, "disconnecting session #{0}", s.getId());
                }
            }
        }
    }

    public synchronized boolean closeIfNeeded() {
        if (closeDelay < 0 || !userSessions.isEmpty())
            return false;
        if (closeDelay == 0 || lastSessionRemovedAt > 0
                && System.currentTimeMillis() - lastSessionRemovedAt > closeDelay) {
            close(false);
            return true;
        }
        return false;
    }

    /**
     * Close the database.
     *
     * @param fromShutdownHook true if this method is called from the shutdown hook
     */
    private synchronized void close(boolean fromShutdownHook) {
        if (state == State.CLOSING || state == State.CLOSED) {
            return;
        }
        if (userSessions.size() > 0) {
            if (!fromShutdownHook) {
                return;
            }
            state = State.CLOSING;
            trace.info("closing {0} from shutdown hook", name);
            closeAllSessionsException(null);
        } else {
            state = State.CLOSING;
        }
        trace.info("closing {0}", name);
        if (eventListener != null) {
            // allow the event listener to connect to the database
            state = State.OPENED;
            DatabaseEventListener e = eventListener;
            // set it to null, to make sure it's called only once
            eventListener = null;
            e.closingDatabase();
            if (userSessions.size() > 0) {
                // if a connection was opened, we can't close the database
                return;
            }
            state = State.CLOSING;
        }
        // remove all session variables
        if (persistent) {
            if (lobStorage != null) {
                boolean containsLargeObject = false;
                for (Schema schema : getAllSchemas()) {
                    for (Table table : schema.getAllTablesAndViews()) {
                        if (table.containsLargeObject() && !table.getDataHandler().isTableLobStorage()) {
                            containsLargeObject = true;
                            break;
                        }
                    }
                }
                // 避免在没有lob字段时在关闭数据库的最后反而去生成lob相关的文件
                if (containsLargeObject) {
                    try {
                        lobStorage.removeAllForTable(LobStorage.TABLE_ID_SESSION_VARIABLE);
                    } catch (DbException e) {
                        trace.error(e, "close");
                    }
                }
            }
        }
        try {
            if (systemSession != null) {
                for (Table table : getAllTablesAndViews(false)) {
                    if (table.isGlobalTemporary()) {
                        table.removeChildrenAndResources(systemSession, null);
                    } else {
                        table.close(systemSession);
                    }
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObjectType.SEQUENCE)) {
                    Sequence sequence = (Sequence) obj;
                    sequence.close();
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObjectType.TRIGGER)) {
                    TriggerObject trigger = (TriggerObject) obj;
                    try {
                        trigger.close();
                    } catch (SQLException e) {
                        trace.error(e, "close");
                    }
                }
                if (meta != null)
                    meta.close(systemSession);
                systemSession.commit();
            }
        } catch (DbException e) {
            trace.error(e, "close");
        }
        tempFileDeleter.deleteAll();
        closeSystemSession();
        trace.info("closed");
        traceSystem.close();
        if (closeOnExitHook != null && !fromShutdownHook) {
            try {
                ShutdownHookUtils.removeShutdownHook(closeOnExitHook);
            } catch (Exception e) {
                // ignore
            }
            closeOnExitHook = null;
        }
        LealoneDatabase.getInstance().closeDatabase(name);

        for (Storage s : getStorages()) {
            s.close();
        }
        state = State.CLOSED;
    }

    private synchronized void closeSystemSession() {
        if (systemSession != null) {
            try {
                systemSession.close();
            } catch (DbException e) {
                trace.error(e, "close system session");
            }
            systemSession = null;
        }
    }

    /**
     * Immediately close the database.
     */
    public synchronized void shutdownImmediately() {
        try {
            userSessions.clear();
            LealoneDatabase.getInstance().closeDatabase(name);
            for (Storage s : getStorages()) {
                s.closeImmediately();
            }
            if (traceSystem != null) {
                traceSystem.close();
            }
        } catch (DbException e) {
            DbException.traceThrowable(e);
        } finally {
            state = State.POWER_OFF;
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public ArrayList<Comment> getAllComments() {
        HashMap<String, Comment> map = getDbObjects(DbObjectType.COMMENT);
        return new ArrayList<>(map.values());
    }

    public ArrayList<Right> getAllRights() {
        HashMap<String, Right> map = getDbObjects(DbObjectType.RIGHT);
        return new ArrayList<>(map.values());
    }

    public ArrayList<Role> getAllRoles() {
        HashMap<String, Role> map = getDbObjects(DbObjectType.ROLE);
        return new ArrayList<>(map.values());
    }

    /**
     * Get all schema objects.
     *
     * @return all objects of all types
     */
    public ArrayList<SchemaObject> getAllSchemaObjects() {
        initInfoSchemaMetaTables();
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : getAllSchemas()) {
            list.addAll(schema.getAll());
        }
        return list;
    }

    /**
     * Get all schema objects of the given type.
     *
     * @param type the object type
     * @return all objects of that type
     */
    public ArrayList<SchemaObject> getAllSchemaObjects(DbObjectType type) {
        if (type == DbObjectType.TABLE_OR_VIEW) {
            initInfoSchemaMetaTables();
        }
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : getAllSchemas()) {
            list.addAll(schema.getAll(type));
        }
        return list;
    }

    /**
     * Get all tables and views.
     *
     * @param includeMeta whether to force including the meta data tables (if
     *            true, metadata tables are always included; if false, metadata
     *            tables are only included if they are already initialized)
     * @return all objects of that type
     */
    public ArrayList<Table> getAllTablesAndViews(boolean includeMeta) {
        if (includeMeta) {
            initInfoSchemaMetaTables();
        }
        ArrayList<Table> list = new ArrayList<>();
        for (Schema schema : getAllSchemas(includeMeta)) {
            list.addAll(schema.getAllTablesAndViews());
        }
        return list;
    }

    public ArrayList<Schema> getAllSchemas() {
        return getAllSchemas(true);
    }

    private ArrayList<Schema> getAllSchemas(boolean includeMeta) {
        if (includeMeta) {
            initInfoSchemaMetaTables();
        }
        HashMap<String, Schema> map = getDbObjects(DbObjectType.SCHEMA);
        return new ArrayList<>(map.values());
    }

    public ArrayList<User> getAllUsers() {
        HashMap<String, User> map = getDbObjects(DbObjectType.USER);
        return new ArrayList<>(map.values());
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    /**
     * Get all sessions that are currently connected to the database.
     *
     * @param includingSystemSession if the system session should also be included
     * @return the list of sessions
     */
    public ServerSession[] getSessions(boolean includingSystemSession) {
        ArrayList<ServerSession> list;
        // need to synchronized on userSession, otherwise the list
        // may contain null elements
        synchronized (userSessions) {
            list = new ArrayList<>(userSessions);
        }
        // copy, to ensure the reference is stable
        ServerSession sys = systemSession;
        if (includingSystemSession && sys != null) {
            list.add(sys);
        }
        ServerSession[] array = new ServerSession[list.size()];
        list.toArray(array);
        return array;
    }

    /**
     * Get the schema. If the schema does not exist, an exception is thrown.
     *
     * @param schemaName the name of the schema
     * @return the schema
     * @throws DbException no schema with that name exists
     */
    public Schema getSchema(ServerSession session, String schemaName) {
        Schema schema = findSchema(session, schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    /**
     * Get the first table that depends on this object.
     *
     * @param obj the object to find
     * @param except the table to exclude (or null)
     * @return the first dependent table, or null
     */
    public Table getDependentTable(SchemaObject obj, Table except) {
        switch (obj.getType()) {
        case COMMENT:
        case CONSTRAINT:
        case INDEX:
        case RIGHT:
        case TRIGGER:
        case USER:
            return null;
        default:
        }
        HashSet<DbObject> set = new HashSet<>();
        for (Table t : getAllTablesAndViews(false)) {
            if (except == t) {
                continue;
            }
            set.clear();
            t.addDependencies(set);
            if (set.contains(obj)) {
                return t;
            }
        }
        return null;
    }

    /**
     * Start collecting statistics.
     */
    public void statisticsStart() {
    }

    public HashMap<String, Integer> statisticsEnd() {
        return null;
    }

    public TraceSystem getTraceSystem() {
        return traceSystem;
    }

    public int getCacheSize() {
        return dbSettings.cacheSize;
    }

    public int getPageSize() {
        return dbSettings.pageSize;
    }

    public byte[] getFileEncryptionKey() {
        return dbSettings.fileEncryptionKey;
    }

    public Role getPublicRole() {
        return publicRole;
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    public DatabaseEventListener getEventListener() {
        return eventListener;
    }

    public void setEventListenerClass(String className) {
        if (className == null || className.length() == 0) {
            eventListener = null;
        } else {
            try {
                eventListener = Utils.newInstance(className);
                eventListener.init(name);
            } catch (Throwable e) {
                throw DbException.get(ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2, e, className,
                        e.toString());
            }
        }
    }

    /**
     * Set the progress of a long running operation.
     * This method calls the {@link DatabaseEventListener} if one is registered.
     *
     * @param state the {@link DatabaseEventListener} state
     * @param name the object name
     * @param x the current position
     * @param max the highest value
     */
    public void setProgress(int state, String name, int x, int max) {
        if (eventListener != null) {
            try {
                eventListener.setProgress(state, name, x, max);
            } catch (Exception e2) {
                // ignore this (user made) exception
            }
        }
    }

    /**
     * This method is called after an exception occurred, to inform the database
     * event listener (if one is set).
     *
     * @param e the exception
     * @param sql the SQL statement
     */
    public void exceptionThrown(SQLException e, String sql) {
        if (eventListener != null) {
            try {
                eventListener.exceptionThrown(e, sql);
            } catch (Exception e2) {
                // ignore this (user made) exception
            }
        }
    }

    public int getAllowLiterals() {
        if (isStarting()) {
            return Constants.ALLOW_LITERALS_ALL;
        }
        return dbSettings.allowLiterals;
    }

    public int getMaxMemoryRows() {
        return dbSettings.maxMemoryRows;
    }

    public int getMaxMemoryUndo() {
        return dbSettings.maxMemoryUndo;
    }

    public int getMaxOperationMemory() {
        return dbSettings.maxOperationMemory;
    }

    public synchronized void setCloseDelay(int value) {
        this.closeDelay = value;
    }

    public ServerSession getSystemSession() {
        return systemSession;
    }

    public boolean getIgnoreCase() {
        if (isStarting()) {
            // tables created at startup must not be converted to ignorecase
            return false;
        }
        return dbSettings.ignoreCase;
    }

    public boolean getOptimizeReuseResults() {
        return dbSettings.optimizeReuseResults;
    }

    public boolean getReferentialIntegrity() {
        return dbSettings.referentialIntegrity;
    }

    public synchronized int getSessionCount() {
        return userSessions.size();
    }

    public void setQueryStatistics(boolean b) {
        synchronized (this) {
            if (!b) {
                queryStatisticsData = null;
            }
        }
    }

    public boolean getQueryStatistics() {
        return dbSettings.queryStatistics;
    }

    public void setQueryStatisticsMaxEntries(int n) {
        if (queryStatisticsData != null) {
            synchronized (this) {
                if (queryStatisticsData != null) {
                    queryStatisticsData.setMaxQueryEntries(n);
                }
            }
        }
    }

    public QueryStatisticsData getQueryStatisticsData() {
        if (!dbSettings.queryStatistics) {
            return null;
        }
        if (queryStatisticsData == null) {
            synchronized (this) {
                if (queryStatisticsData == null) {
                    queryStatisticsData = new QueryStatisticsData(dbSettings.queryStatisticsMaxEntries);
                }
            }
        }
        return queryStatisticsData;
    }

    /**
     * Check if the database is currently opening. This is true until all stored
     * SQL statements have been executed.
     *
     * @return true if the database is still starting
     */
    public boolean isStarting() {
        return state == State.STARTING;
    }

    private boolean isMetaReady() {
        return state != State.CONSTRUCTOR_CALLED && state != State.STARTING;
    }

    /**
     * Check if the database is in the process of closing.
     *
     * @return true if the database is closing
     */
    public boolean isClosing() {
        return state == State.CLOSING;
    }

    public boolean isOpened() {
        return state == State.OPENED;
    }

    /**
     * Check if multi version concurrency is enabled for this database.
     *
     * @return true if it is enabled
     */
    public boolean isMultiVersion() {
        return transactionEngine.supportsMVCC();
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public Mode getMode() {
        return mode;
    }

    public ServerSession getExclusiveSession() {
        return exclusiveSession;
    }

    /**
     * Set the session that can exclusively access the database.
     *
     * @param session the session
     * @param closeOthers whether other sessions are closed
     */
    public synchronized void setExclusiveSession(ServerSession session, boolean closeOthers) {
        this.exclusiveSession = session;
        if (closeOthers) {
            closeAllSessionsException(session);
        }
        if (session == null && waitingSessions != null) {
            for (ServerSession s : waitingSessions) {
                s.setStatus(SessionStatus.TRANSACTION_NOT_COMMIT);
                s.getScheduler().wakeUp();
            }
            waitingSessions = null;
        }
    }

    public synchronized boolean addWaitingSession(ServerSession session) {
        if (exclusiveSession == null)
            return false;
        if (waitingSessions == null)
            waitingSessions = new LinkedList<>();
        waitingSessions.add(session);
        return true;
    }

    /**
     * Get the first user defined table.
     *
     * @return the table or null if no table is defined
     */
    public Table getFirstUserTable() {
        for (Table table : getAllTablesAndViews(false)) {
            if (TableAlterHistory.getName().equalsIgnoreCase(table.getName()))
                continue;
            if (table.getCreateSQL() != null) {
                if (table.isHidden()) {
                    // LOB tables
                    continue;
                }
                return table;
            }
        }
        return null;
    }

    /**
     * Flush all changes and open a new transaction log.
     */
    public void checkpoint() {
        if (persistent) {
            transactionEngine.checkpoint();
        }
        getTempFileDeleter().deleteUnused();
    }

    /**
     * Switch the database to read-only mode.
     *
     * @param readOnly the new value
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public SourceCompiler getCompiler() {
        if (compiler == null) {
            compiler = new SourceCompiler();
        }
        return compiler;
    }

    public Connection getInternalConnection() {
        return getInternalConnection(systemSession);
    }

    public Connection getInternalConnection(Scheduler scheduler) {
        ServerSession session = createSession(getSystemUser(), scheduler);
        return getInternalConnection(session);
    }

    private Connection getInternalConnection(ServerSession session) {
        return ServerSession.createConnection(session, systemUser.getName(),
                Constants.CONN_URL_INTERNAL);
    }

    public int getDefaultTableType() {
        return dbSettings.defaultTableType;
    }

    /**
     * Create a new hash map. Depending on the configuration, the key is case
     * sensitive or case insensitive.
     *
     * @param <V> the value type
     * @return the hash map
     */
    public <V> HashMap<String, V> newStringMap() {
        return dbSettings.databaseToUpper ? new HashMap<String, V>() : new CaseInsensitiveMap<V>();
    }

    /**
     * Compare two identifiers (table names, column names,...) and verify they
     * are equal. Case sensitivity depends on the configuration.
     *
     * @param a the first identifier
     * @param b the second identifier
     * @return true if they match
     */
    public boolean equalsIdentifiers(String a, String b) {
        if (a == b || a.equals(b)) {
            return true;
        }
        if (!dbSettings.databaseToUpper && a.equalsIgnoreCase(b)) {
            return true;
        }
        return false;
    }

    public void backupTo(String fileName, Long lastDate) {
        checkpoint();
        String baseDir = getStoragePath().replace('\\', '/');
        baseDir = baseDir.substring(0, baseDir.lastIndexOf('/'));
        try (ZipOutputStream out = StorageBase.createZipOutputStream(fileName)) {
            // 如果有database lob storage，这一步也把它备份了
            for (Storage s : getStorages()) {
                s.backupTo(baseDir, out, lastDate);
            }
            // 备份table lob storage
            for (DataHandler dh : dataHandlers.values()) {
                dh.getLobStorage().backupTo(baseDir, out, lastDate);
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    public Storage getMetaStorage() {
        return storages.get(metaStorageEngineName);
    }

    public List<Storage> getStorages() {
        return new ArrayList<>(storages.values());
    }

    public Storage getStorage(String storageName) {
        return storages.get(storageName);
    }

    public synchronized Storage getStorage(StorageEngine storageEngine) {
        Storage storage = storages.get(storageEngine.getName());
        if (storage != null)
            return storage;
        String storagePath = persistent ? getStoragePath() : null;
        storage = getStorageBuilder(storageEngine, storagePath).openStorage();
        storages.put(storageEngine.getName(), storage);
        if (persistent && lobStorage == null) {
            setLobStorage(storageEngine.getLobStorage(this, storage));
            transactionEngine.addGcTask(lobStorage);
        }
        return storage;
    }

    public String getStoragePath() {
        if (storagePath != null)
            return storagePath;
        String baseDir = SysProperties.getBaseDir();
        baseDir = FileUtils.getDirWithSeparator(baseDir);

        String path;
        if (baseDir == null)
            path = "." + File.separator;
        else
            path = baseDir;

        path = path + "db" + Constants.NAME_SEPARATOR + id;
        try {
            path = new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return path;
    }

    public StorageBuilder getStorageBuilder(StorageEngine storageEngine, String storagePath) {
        StorageBuilder storageBuilder = storageEngine.getStorageBuilder();
        if (!persistent) {
            storageBuilder.inMemory();
        } else {
            byte[] key = getFileEncryptionKey();
            storageBuilder.cacheSize(getCacheSize());
            storageBuilder.pageSize(getPageSize());
            storageBuilder.storagePath(storagePath);
            if (isReadOnly()) {
                storageBuilder.readOnly();
            }
            if (key != null) {
                char[] password = new char[key.length / 2];
                for (int i = 0; i < password.length; i++) {
                    password[i] = (char) (((key[i + i] & 255) << 16) | ((key[i + i + 1]) & 255));
                }
                storageBuilder.encryptionKey(password);
            }
            if (getSettings().compressData) {
                storageBuilder.compress();
                // use a larger page split size to improve the compression ratio
                int pageSize = getPageSize();
                int compressPageSize = 64 * 1024;
                if (pageSize > compressPageSize)
                    compressPageSize = pageSize;
                storageBuilder.pageSize(compressPageSize);
            }
        }
        return storageBuilder;
    }

    @Override
    public String getSQL() {
        return quoteIdentifier(name);
    }

    private static String getCreateSQL(String quotedDbName, Map<String, String> parameters,
            RunMode runMode, boolean ifNotExists) {
        StatementBuilder sql = new StatementBuilder("CREATE DATABASE ");
        if (ifNotExists)
            sql.append("IF NOT EXISTS ");
        sql.append(quotedDbName);
        if (runMode != null) {
            sql.append(" RUN MODE ").append(runMode.toString());
        }
        if (parameters != null && !parameters.isEmpty()) {
            sql.append(" PARAMETERS");
            appendMap(sql, parameters);
        }
        return sql.toString();
    }

    public static void appendMap(StatementBuilder sql, Map<String, String> map) {
        sql.resetCount();
        sql.append("(");
        for (Entry<String, String> e : map.entrySet()) {
            if (e.getValue() == null)
                continue;
            sql.appendExceptFirst(",");
            sql.append(e.getKey()).append('=').append("'").append(e.getValue()).append("'");
        }
        sql.append(')');
    }

    public String[] getHostIds() {
        if (hostIds == null) {
            synchronized (this) {
                if (hostIds == null) {
                    if (parameters != null && parameters.containsKey("hostIds")) {
                        targetNodes = parameters.get("hostIds").trim();
                        hostIds = StringUtils.arraySplit(targetNodes, ',');
                    }
                    if (hostIds == null) {
                        hostIds = new String[0];
                        nodes = null;
                    } else {
                        nodes = new HashSet<>(hostIds.length);
                        for (String id : hostIds) {
                            nodes.add(NetNode.createTCP(id));
                        }
                    }
                    if (nodes != null && nodes.isEmpty()) {
                        nodes = null;
                    }
                    if (targetNodes != null && targetNodes.isEmpty())
                        targetNodes = null;
                }
            }
        }
        return hostIds;
    }

    public void setHostIds(String[] hostIds) {
        this.hostIds = null;
        if (hostIds != null && hostIds.length > 0)
            parameters.put("hostIds", StringUtils.arrayCombine(hostIds, ','));
        else
            parameters.put("hostIds", "");
        getHostIds();
    }

    public boolean isTargetNode(NetNode node) {
        if (hostIds == null) {
            getHostIds();
        }
        return nodes == null || nodes.contains(node);
    }

    public String getTargetNodes() {
        if (hostIds == null) {
            getHostIds();
        }
        return targetNodes;
    }

    public void createRootUserIfNotExists() {
        // 如果已经存在一个Admin权限的用户，那就不再创建root用户了
        // 最常见的是对默认的root用户重命名后会出现这种情况
        for (User user : getAllUsers()) {
            if (user.isAdmin())
                return;
        }
        // 新建session，避免使用system session
        try (ServerSession session = createSession(systemUser)) {
            // executeUpdate()会自动提交，所以不需要再调用一次commit
            session.executeUpdateLocal("CREATE USER IF NOT EXISTS root PASSWORD '' ADMIN");
        }
    }

    synchronized User createAdminUser(String userName, byte[] userPasswordHash) {
        // 新建session，避免使用system session
        try (ServerSession session = createSession(systemUser)) {
            DbObjectLock lock = tryExclusiveAuthLock(session);
            User user = new User(this, allocateObjectId(), userName, false);
            user.setAdmin(true);
            user.setUserPasswordHash(userPasswordHash);
            addDatabaseObject(session, user, lock);
            session.commit();
            return user;
        }
    }

    public User getSystemUser() {
        return systemUser;
    }

    public synchronized void drop() {
        state = State.CLOSED;
        LealoneDatabase.getInstance().dropDatabase(getName());
        if (lobStorage != null) {
            getTransactionEngine().removeGcTask(lobStorage);
        }
        for (Storage storage : getStorages()) {
            storage.drop();
        }
    }

    public void setLastConnectionInfo(ConnectionInfo ci) {
        lastConnectionInfo = ci;
    }

    public TableAlterHistory getTableAlterHistory() {
        return tableAlterHistory;
    }

    public DataHandler getDataHandler(int tableId) {
        DataHandler dh = dataHandlers.get(tableId);
        return dh == null ? this : dh;
    }

    public void addDataHandler(int tableId, DataHandler dataHandler) {
        dataHandlers.put(tableId, dataHandler);
    }

    public void removeDataHandler(int tableId) {
        dataHandlers.remove(tableId);
    }

    public long getDiskSpaceUsed() {
        long sum = 0;
        for (Storage s : getStorages()) {
            sum += s.getDiskSpaceUsed();
        }
        return sum;
    }

    public TransactionalDbObjects[] getTransactionalDbObjectsArray() {
        return dbObjectsArray;
    }

    public void markClosed() {
        setCloseDelay(0);
        for (ServerSession s : getSessions(false)) {
            // 先标记为关闭状态，然后由调度器优雅关闭
            s.markClosed();
        }
    }

    public PluginObject findPluginObject(ServerSession session, String name) {
        return find(DbObjectType.PLUGIN, session, name);
    }

    public ArrayList<PluginObject> getAllPluginObjects() {
        HashMap<String, PluginObject> map = getDbObjects(DbObjectType.PLUGIN);
        return new ArrayList<>(map.values());
    }

    private long lastGcMetaId;

    public long getLastGcMetaId() {
        return lastGcMetaId;
    }

    public void setLastGcMetaId(long lastGcMetaId) {
        this.lastGcMetaId = lastGcMetaId;
    }
}
