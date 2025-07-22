package com.database.db;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class FileIOThread extends Thread {

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
        }
    }

    public void shutdown() throws InterruptedException {
        acceptingTasks = false;
        running = false;
        this.interrupt();
        this.join();
    }

    @Override
    public void run() {
        while (running || !taskQueue.isEmpty()) {
            try {
                Runnable task = taskQueue.take();
                try {
                    task.run();
                } catch (Exception e) {
                    System.err.println("Task execution failed: " + e);
                    e.printStackTrace();
                }
            } catch (InterruptedException e) {
                // Ignore — check running flag & queue again
            }
        }
        System.out.println("FileIOThread shut down. All tasks completed.");
    }
}
