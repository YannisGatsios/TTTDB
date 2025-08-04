package com.database.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
            try {
                Files.write(Path.of(filePath), treeBuffer, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
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
            try {
                byte[] data = Files.readAllBytes(Path.of(indexPath));
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
        if (pageBuffer == null || pageBuffer.length == 0)
            throw new IllegalArgumentException("Page buffer cannot be null or empty.");
        if (path == null || path.isEmpty())
            throw new IllegalArgumentException("Path cannot be null or empty.");
        fileIOThread.submit(() -> {
            try (FileChannel channel = FileChannel.open(Path.of(path), 
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                ByteBuffer buffer = ByteBuffer.wrap(pageBuffer);
                channel.position(pagePosition);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
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
        if (path == null || path.isEmpty())
            throw new IllegalArgumentException("Path cannot be null or empty.");
        if (pageMaxSize <= 0 || pageMaxSize % 4096 != 0)
            throw new IllegalArgumentException("Invalid page size: " + pageMaxSize);
        
        FutureTask<byte[]> readTask = new FutureTask<>(() -> {
            ByteBuffer buffer = ByteBuffer.allocate(pageMaxSize);
            try (FileChannel channel = FileChannel.open(Path.of(path), StandardOpenOption.READ)) {
                channel.position(pagePosition);
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1)
                    return null;

                if (bytesRead < pageMaxSize) {
                    byte[] actual = new byte[bytesRead];
                    buffer.flip();
                    buffer.get(actual);
                    return actual;
                } else {
                    return buffer.array(); // Full page read
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error reading page from file: " + path, e);
                throw new RuntimeException("Failed to read page from file: " + path, e);
            }
        });
        fileIOThread.submit(readTask);
        return readTask;
    }

    public void truncateFile(String path, int pageSize) throws ExecutionException, InterruptedException {
        if (pageSize <= 0 || pageSize % 4096 != 0)
            throw new IllegalArgumentException("Page size must be a positive multiple of 4096");

        FutureTask<Boolean> future = new FutureTask<>(() -> {
            try (FileChannel channel = FileChannel.open(Path.of(path), StandardOpenOption.WRITE)) {
                long currentSize = channel.size();
                long newSize = currentSize - pageSize;
                if (newSize < 0)
                    throw new IOException("File too small to truncate.");
                channel.truncate(newSize);
                logger.fine(
                        String.format("Truncated last %d bytes from %s. New size: %d bytes.", pageSize, path, newSize));
                return true;
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to truncate file: " + path, e);
                throw new RuntimeException("Failed to truncate file", e);
            }
        });

        fileIOThread.submit(future);
        future.get();
    }
}
