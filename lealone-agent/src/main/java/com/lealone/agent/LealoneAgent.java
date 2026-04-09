/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent;

import com.lealone.client.LealoneClient;

public class LealoneAgent extends LealoneClient {

    public static void main(String[] args) {
        LealoneAgent agent = new LealoneAgent(args);
        main(agent);
    }

    protected LealoneAgent(String[] args) {
        super(args);
    }

    @Override
    public String getName() {
        return "Lealone Agent";
    }

    @Override
    protected String getPrompt() {
        return "agent> ";
    }

    @Override
    protected String getPromptContinue() {
        return "    -> ";
    }
}
