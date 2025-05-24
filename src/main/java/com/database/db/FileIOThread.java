package com.database.db;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class FileIOThread extends Thread {

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private volatile boolean running = true;

    public void submit(Runnable task) {
        try {
            taskQueue.put(task); // Blocks if queue is full (rare in unbounded)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        running = false;
        this.interrupt(); // Unblock if waiting
    }

    @Override
    public void run() {
        while (running || !taskQueue.isEmpty()) {
            try {
                Runnable task = taskQueue.take();
                task.run();
            } catch (InterruptedException e) {
                // allow shutdown to exit loop
            }
        }
        System.out.println("FileIOThread shut down.");
    }
}
