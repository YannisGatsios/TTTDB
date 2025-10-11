package com.database.db;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.page.Page;

public class FileIO {
    private static final Logger logger = Logger.getLogger(FileIO.class.getName());

    private FileIOThread fileIOThread;

    public FileIO(FileIOThread fileIOThread){
        this.fileIOThread = fileIOThread;
    }

    public static int getNumOfPages(String path, int sizeOfEntry){
        File file = new File(path);
        long fileSize = file.length();
        int pageSize = Page.pageSizeInBytes(sizeOfEntry);
        return (int) ((fileSize + pageSize - 1) / pageSize);
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

    public void writePages(List<Page> pages) {
        fileIOThread.submit(() -> {
            Map<String, List<Page>> pagesByFile = new HashMap<>();

            // Group pages by file path
            for (Page page : pages) {
                String path = page.getFilePath(); // Table path or index path
                pagesByFile.computeIfAbsent(path, _ -> new ArrayList<>()).add(page);
            }

            // Write pages grouped by file
            for (Map.Entry<String, List<Page>> entry : pagesByFile.entrySet()) {
                String path = entry.getKey();
                List<Page> filePages = entry.getValue();
                try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                    for (Page page : filePages) {
                        raf.seek(page.getPagePos());
                        raf.write(page.toBytes());
                    }
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Error writing pages to file: " + path, e);
                }
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
                    return buffer.array();
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error reading page from file: " + path, e);
                throw new RuntimeException("Failed to read page from file: " + path, e);
            }
        });
        fileIOThread.submit(readTask);
        return readTask.get();
    }

    public void truncateFile(String path, int pageSize) throws ExecutionException, InterruptedException {
        if (pageSize <= 0 || pageSize % 4096 != 0)
            throw new IllegalArgumentException("Page size must be a positive multiple of 4096");
        fileIOThread.submit(() -> {
            try (FileChannel channel = FileChannel.open(Path.of(path), StandardOpenOption.WRITE)) {
                long newSize = channel.size() - pageSize;
                if (newSize < 0) throw new IOException("File too small");
                channel.truncate(newSize);
                logger.fine("Truncated " + pageSize + " bytes from " + path);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to truncate file: " + path, e);
            }
        });
    }
}