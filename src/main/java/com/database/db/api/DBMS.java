package com.database.db.api;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBMS {
    static {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.ALL);
        for (Handler handler : rootLogger.getHandlers()) {
            handler.setLevel(Level.ALL);
        }
    }
}
