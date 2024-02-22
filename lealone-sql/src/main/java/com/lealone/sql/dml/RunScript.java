/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.ScriptReader;
import com.lealone.db.Constants;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.StatementBase;

/**
 * This class represents the statement
 * RUNSCRIPT
 */
public class RunScript extends ScriptBase {

    /**
     * The byte order mark.
     * 0xfeff because this is the Unicode char
     * represented by the UTF-8 byte order mark (EF BB BF).
     */
    private static final char UTF8_BOM = '\uFEFF';

    private Charset charset = Constants.UTF8;

    public RunScript(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.RUNSCRIPT;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        int count = 0;
        try {
            openInput();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset));
            // if necessary, strip the BOM from the front of the file
            reader.mark(1);
            if (reader.read() != UTF8_BOM) {
                reader.reset();
            }
            ScriptReader r = new ScriptReader(reader);
            while (true) {
                String sql = r.readStatement();
                if (sql == null) {
                    break;
                }
                execute(sql);
                count++;
                if ((count & 127) == 0) {
                    checkCanceled();
                }
            }
            reader.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            closeIO();
        }
        return count;
    }

    private void execute(String sql) {
        try {
            StatementBase command = (StatementBase) session.prepareStatement(sql);
            if (command.isQuery()) {
                command.query(0);
            } else {
                command.update();
            }
            if (session.isAutoCommit()) {
                session.commit();
            }
        } catch (DbException e) {
            throw e.addSQL(sql);
        }
    }
}
