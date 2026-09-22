package com.database.tttdb.core;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileIOThread extends Thread {

    private static final Logger logger =
            Logger.getLogger(FileIOThread.class.getName());

    private static final Runnable POISON_PILL = () -> { };

    private final BlockingQueue<Runnable> taskQueue =
            new LinkedBlockingQueue<>();

    private final Object lifecycleLock = new Object();

    private boolean acceptingTasks = true;

    public FileIOThread(String databaseName) {
        super("FileIOThread-" + databaseName);
    }

    public void submit(Runnable task) {
        Objects.requireNonNull(task, "task");

        synchronized (lifecycleLock) {
            if (!acceptingTasks) {
                throw new IllegalStateException(
                        "Cannot submit task: shutdown is in progress."
                );
            }

            // LinkedBlockingQueue is unbounded, so add() will not block.
            taskQueue.add(task);
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown(null);
    }

    /**
     * Stops accepting new work, queues an optional cleanup task,
     * then queues the poison pill.
     *
     * Everything already queued will run before cleanup and shutdown.
     */
    public void shutdown(Runnable cleanupTask)
            throws InterruptedException {

        synchronized (lifecycleLock) {
            if (acceptingTasks) {
                acceptingTasks = false;

                if (cleanupTask != null) {
                    taskQueue.add(cleanupTask);
                }

                taskQueue.add(POISON_PILL);
            }
        }

        join();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Runnable task = taskQueue.take();

                if (task == POISON_PILL) {
                    break;
                }

                try {
                    task.run();
                } catch (Exception e) {
                    logger.log(
                            Level.SEVERE,
                            "File I/O task execution failed.",
                            e
                    );
                }
            } catch (InterruptedException e) {
                // Preserve the running behavior. The poison pill determines
                // when the worker actually stops.
                logger.log(
                        Level.FINE,
                        "File I/O thread interrupted.",
                        e
                );
            }
        }

        logger.fine("File I/O thread shut down.");
    }
}