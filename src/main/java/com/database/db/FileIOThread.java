package com.database.db;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class FileIOThread extends Thread {

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private volatile boolean running = true;

    public void submit(Runnable task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        running = false;
        this.interrupt();
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
                // allow shutdown
            }
        }
        System.out.println("FileIOThread shut down.");
    }
}
