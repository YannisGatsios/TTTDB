package com.database.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileIO {
    private static final Logger logger = Logger.getLogger(FileIO.class.getName());

    private FileIOThread fileIOThread;;

    public FileIO(FileIOThread thread) {
        this.fileIOThread = thread;
    }
    public void writeTree(String filePath, byte[] treeBuffer) {
        fileIOThread.submit(() -> {
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                fos.write(treeBuffer);
                fos.getChannel().truncate(treeBuffer.length);
                logger.fine("Index File successfully written to: " + filePath);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error writing to Index file: " + filePath, e);
            }
        });
    }

    public byte[] readTree(String indexPath) throws InterruptedException, ExecutionException {
        return readTreeAsync(indexPath).get();
    }

    private Future<byte[]> readTreeAsync(String indexPath) {
        FutureTask<byte[]> readTask = new FutureTask<>(() -> {
            try (FileInputStream fis = new FileInputStream(indexPath)) {
                byte[] data = new byte[fis.available()];
                fis.read(data);
                logger.fine("Index File successfully read into byte array from: " + indexPath);
                return data;
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error reading Index file: " + indexPath, e);
                return null;
            }
        });
        fileIOThread.submit(readTask);
        return readTask;
    }

    public void writePage(String path, byte[] pageBuffer, int pagePosition) {
        fileIOThread.submit(() -> {
            if (pageBuffer == null || pageBuffer.length == 0)
                throw new IllegalArgumentException("Page buffer cannot be null or empty.");
            if (path == null || path.isEmpty())
                throw new IllegalArgumentException("Path cannot be null or empty.");
            try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                raf.seek(pagePosition);
                raf.write(pageBuffer);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error writing page to file: " + path + " at position " + pagePosition, e);
                throw new RuntimeException("Failed to write page to file: " + path, e);
            }
        });
    }

    public byte[] readPage(String path, int pagePosition, int pageMaxSize) throws InterruptedException, ExecutionException {
        return readPageAsync(path, pagePosition, pageMaxSize).get();
    }

    private Future<byte[]> readPageAsync(String path, int pagePosition, int pageMaxSize) {
        FutureTask<byte[]> readTask = new FutureTask<>(() -> {
            if (path == null || path.isEmpty())
                throw new IllegalArgumentException("Path cannot be null or empty.");
            if (pageMaxSize <= 0 || pageMaxSize % 4096 != 0)
                throw new IllegalArgumentException("Invalid page size: " + pageMaxSize);

            byte[] buffer = new byte[pageMaxSize];
            try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
                raf.seek(pagePosition);
                int bytesRead = raf.read(buffer);
                if (bytesRead == -1) return null;
                if (bytesRead < pageMaxSize) {
                    byte[] actualBytes = new byte[bytesRead];
                    System.arraycopy(buffer, 0, actualBytes, 0, bytesRead);
                    buffer = actualBytes;
                }
                return buffer;
            } catch (FileNotFoundException e) {
                logger.log(Level.WARNING, "Page file not found: " + path, e);
                throw new RuntimeException("Page file not found: " + path, e);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error reading page from file: " + path, e);
                throw new RuntimeException("Failed to read page from file: " + path, e);
            }
        });
        fileIOThread.submit(readTask);
        return readTask;
    }

    public void truncateFile(String path, int pageSize) throws ExecutionException, InterruptedException {
        FutureTask<Boolean> future = new FutureTask<>(() -> {
            try (RandomAccessFile file = new RandomAccessFile(path, "rw");
                 FileChannel channel = file.getChannel()) {

                long currentSize = channel.size();
                long newSize = currentSize - pageSize;
                if (newSize < 0) throw new IOException("File too small to truncate.");

                channel.truncate(newSize);
                logger.fine(String.format("Truncated last %d bytes from %s. New size: %d bytes.", pageSize, path, newSize));
                return true;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to truncate file: " + path, e);
                throw new RuntimeException("Failed to truncate file", e);
            }
        });
        fileIOThread.submit(future);
        future.get();
    }
}
