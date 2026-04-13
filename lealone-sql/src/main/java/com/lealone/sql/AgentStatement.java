/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

import com.lealone.agent.CodeAgent;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.db.Database;
import com.lealone.db.SysProperties;
import com.lealone.db.result.LocalResult;
import com.lealone.db.result.Result;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.util.SourceCompiler;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueString;
import com.lealone.sql.expression.ExpressionColumn;

public class AgentStatement extends StatementBase {

    private final String userPrompt;

    public AgentStatement(ServerSession session, String userPrompt) {
        super(session);
        this.userPrompt = userPrompt;
    }

    @Override
    public int getType() {
        return SQLStatement.UNKNOWN;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public Result query(int maxRows) {
        Database db = session.getDatabase();
        Map<String, Map<String, String>> extServices = db.getExternalService().getRecords(session);
        StringBuilder userPrompt = new StringBuilder(this.userPrompt);
        String str = """
                \n\n以上是用户的需求，请从后面的服务列表中找出可能用到的服务，
                如果存在可用的服务就直接基于这些服务写java代码实现需求，代码放在public static String run()中实现，
                返回的结果只包含逗号分隔的name列表和一个冒号然后是类名再加一个冒号最后加上纯java代码实现，不要包含java标记也不需要注释；
                如果没有合适的服务就返回0和一个冒号然后你自由回答。

                以下是可用的服务,按name:comment的方式排列:
                                                """;
        userPrompt.append(str);
        for (Map<String, String> map : extServices.values()) {
            userPrompt.append(map.get("name")).append(":").append(map.get("comment")).append("\n");
        }
        CodeAgent agent = db.getCodeAgent();
        String content = agent.generateJavaCode(userPrompt.toString());
        if (content.startsWith("0:")) {
            content = content.substring(2);
        } else {
            int pos = content.indexOf(':');
            if (pos >= 0) {
                File lib = new File(SysProperties.getBaseDir(), "lib");
                String nameStr = content.substring(0, pos);
                String code = content.substring(pos + 1);
                pos = code.indexOf(':');
                String className = code.substring(0, pos);
                code = code.substring(pos + 1);
                String[] names = StringUtils.arraySplit(nameStr, ',');
                ArrayList<URL> urls = new ArrayList<>();
                for (String name : names) {
                    Map<String, String> map = extServices.get(name);
                    String url = map.get("url");
                    if (url.equalsIgnoreCase("cli"))
                        continue;
                    try {
                        File f = new File(url);
                        if (f.exists()) {
                            urls.add(f.toURI().toURL());
                            continue;
                        }
                    } catch (Exception e) {
                    }
                    File jarFile;
                    pos = url.lastIndexOf('/');
                    if (pos >= 0) {
                        jarFile = new File(lib, url.substring(pos + 1));
                    } else {
                        continue;
                    }
                    try {
                        if (!jarFile.exists()) {
                            URI uri = URI.create(url);
                            downloadJar(uri, lib.getAbsolutePath());
                        }
                        urls.add(jarFile.toURI().toURL());
                    } catch (Exception e) {
                    }
                }
                SourceCompiler compiler = new SourceCompiler();
                compiler.setSource(className, code);
                compiler.setUrls(urls.toArray(new URL[0]));
                try {
                    Method mainMethod = compiler.compile(className).getMethod("run");
                    content = (String) mainMethod.invoke(null);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        }

        ExpressionColumn c = new ExpressionColumn(session.getDatabase(),
                new Column("content", Value.STRING));
        LocalResult result = new LocalResult(session, new IExpression[] { c, }, 1);
        Value[] row = { ValueString.get(content) };
        result.addRow(row);
        result.done();
        return result;
    }

    private static void downloadJar(URI uri, String savePath) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000); // 连接超时 10 秒
        conn.setReadTimeout(10000); // 读取超时 10 秒

        // 获取响应码，判断是否下载成功
        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("下载失败，HTTP 响应码：" + responseCode);
        }
        try (InputStream inputStream = conn.getInputStream();
                FileOutputStream outputStream = new FileOutputStream(savePath);
                BufferedInputStream bufferedIn = new BufferedInputStream(inputStream)) {
            IOUtils.copy(bufferedIn, outputStream);
        } finally {
            // 断开连接
            conn.disconnect();
        }
    }
}
