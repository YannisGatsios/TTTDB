package com.database.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.page.Page;

public class FileIO {
    private static final Logger logger = Logger.getLogger(FileIO.class.getName());

    private FileIOThread fileIOThread;

    public FileIO(FileIOThread thread) {
        this.fileIOThread = thread;
    }

    public void writePage(String path, byte[] pageBuffer, int pagePosition) {
        if (pageBuffer == null || pageBuffer.length == 0)
            throw new IllegalArgumentException("Page buffer cannot be null or empty.");
        if (path == null || path.isEmpty())
            throw new IllegalArgumentException("Path cannot be null or empty.");
        if (pageBuffer.length%4096 != 0)
            throw new IllegalArgumentException("Page size not modulo of 4096.");
        fileIOThread.submit(() -> {
            try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                raf.seek(pagePosition);
                raf.write(pageBuffer);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error writing page to file: " + path + " pos=" + pagePosition, e);
                throw new RuntimeException(e);
            }
        });
    }

    public byte[] readPage(String path, int pagePosition, int pageMaxSize)
            throws InterruptedException, ExecutionException {
        // quick validations
        if (path == null || path.isEmpty())
            throw new IllegalArgumentException("Path cannot be null or empty.");
        if (pageMaxSize <= 0 || pageMaxSize % Page.BLOCK_SIZE != 0)
            throw new IllegalArgumentException("Invalid page size: " + pageMaxSize);

        FutureTask<byte[]> readTask = new FutureTask<>(() -> {
            ByteBuffer buffer = ByteBuffer.allocate(pageMaxSize);
            Path p = Path.of(path);
            try (FileChannel channel = FileChannel.open(p, StandardOpenOption.READ)) {
                channel.position(pagePosition);
                int totalRead = 0;
                int r;
                while (buffer.hasRemaining() && (r = channel.read(buffer)) != -1) {
                    totalRead += r;
                }
                if (totalRead <= 0)
                    return null;
                if (totalRead < pageMaxSize) {
                    byte[] actual = new byte[totalRead];
                    buffer.flip();
                    buffer.get(actual);
                    return actual;
                } else {
                    buffer.flip();
                    byte[] full = buffer.array();
                    return full;
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error reading page from file: " + path, e);
                throw new RuntimeException("Failed to read page from file: " + path, e);
            }
        });
        fileIOThread.submit(readTask);
        byte[] result = readTask.get();
        return result;
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
