package com.database.db;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileIOThread extends Thread {

    private static final Logger logger = Logger.getLogger(FileIOThread.class.getName());

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private volatile boolean acceptingTasks = true;

    private static final Runnable POISON_PILL = () -> { };

    public FileIOThread(String tableName) {
        super("FileIOThread-" + tableName);
    }

    public void submit(Runnable task) {
        if (!acceptingTasks) {
            throw new IllegalStateException("Cannot submit task — shutdown in progress.");
        }
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING, "Interrupted while submitting a task to the queue.", e);
        }
    }

    public void shutdown() throws InterruptedException {
        acceptingTasks = false;
        taskQueue.put(POISON_PILL);
        this.join();      // Wait for thread to finish
    }

    @Override
    public void run() {
        while (true) {
            try {
                Runnable task = taskQueue.take();
                if (task == POISON_PILL) break;
                try {
                    task.run();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Task execution failed.", e);
                }
            } catch (InterruptedException e) {
                // Thread was interrupted (likely due to shut down), check flags and loop
            }
        }
        logger.info("FileIOThread shut down. All tasks completed.");
    }
}