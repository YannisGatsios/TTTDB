package com.database.db;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
public class FileIOThread extends Thread {

    private static final Logger logger = Logger.getLogger(FileIOThread.class.getName());

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private volatile boolean running = true;
    private volatile boolean acceptingTasks = true;

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
        running = false;
        this.interrupt(); // In case it's blocked on take()
        this.join();      // Wait for thread to finish
    }

    @Override
    public void run() {
        while (running || !taskQueue.isEmpty()) {
            try {
                Runnable task = taskQueue.take();
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