/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent;

import java.util.Map;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginManager;

public interface CodeAgent extends Plugin {

    public String getPromptPrefix();

    public String generateJavaCode(String userPrompt);

    public static CodeAgent getCodeAgent(Map<String, String> llmParameters) {
        String llmProvider = llmParameters.get("PROVIDER");
        CodeAgent agent = PluginManager.getPlugin(CodeAgent.class, llmProvider);
        if (agent == null)
            throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, llmProvider);
        agent.init(llmParameters);
        return agent;
    }
}
