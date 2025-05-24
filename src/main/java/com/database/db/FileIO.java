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

public class FileIO {

    private FileIOThread fileIOThread;;

    public FileIO(FileIOThread thread) {
        this.fileIOThread = thread;
    }

    public void writeTree(String filePath, byte[] treeBuffer) {
        fileIOThread.submit(() -> {
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                fos.write(treeBuffer);
                fos.getChannel().truncate(treeBuffer.length);
                System.out.println("Index File successfully written to ./" + filePath);
            } catch (IOException e) {
                System.err.println("Error writing to Index file: " + e.getMessage());
            }
        });
    }

    //B+Tree FileIO
    public byte[] readTree(String indexPath) throws InterruptedException, ExecutionException {
        Future<byte[]> future = this.readTreeAsync(indexPath);
        return future.get();
    }

    private Future<byte[]> readTreeAsync(String indexPath) {
        FutureTask<byte[]> readTask = new FutureTask<>(() -> {
            byte[] data = null;
            try (FileInputStream fis = new FileInputStream(indexPath)) {
                // Get the file length
                long fileLength = fis.available();
                // Create a byte array to hold the file data
                data = new byte[(int) fileLength];
                // Read the file into the byte array
                fis.read(data);
                System.out.println("Index File successfully read into byte array.");
            } catch (IOException e) {
                System.err.println("Error reading Index file: " + e.getMessage());
            }
            return data;
        });
        fileIOThread.submit(readTask);
        return readTask;
    }

    public void writePage(String path, byte[] pageBuffer, int pagePosition) {
        fileIOThread.submit(() -> {
            if (pageBuffer == null || pageBuffer.length == 0)
                throw new IllegalArgumentException("Page buffer cannot be null or empty. When Writing a Page.");
            if (path == null || path.isEmpty())
                throw new IllegalArgumentException("Path cannot be null or empty. When Writing a Page.");
            try {
                RandomAccessFile raf = new RandomAccessFile(path, "rw");
                raf.seek(pagePosition);
                raf.write(pageBuffer, 0, pageBuffer.length);
                raf.close();
            } catch (IOException e) {
                // LOGGING
                System.err.println("Error writing to file: " + path);
                System.err.println("Page position: " + pagePosition);
                e.printStackTrace();

                throw new RuntimeException("Failed to write page to file: " + path, e);
            }
        });
    }

    //Page FileIO
    public byte[] readPage(String path, int pagePosition, int pageMaxSize) throws InterruptedException, ExecutionException {
        Future<byte[]> future = this.readPageAsync(path, pagePosition, pageMaxSize);
        return future.get();
    }

    private Future<byte[]> readPageAsync(String path, int pagePosition, int pageMaxSize) {
        FutureTask<byte[]> readTask = new FutureTask<>(() -> {
            if (path == null || path.isEmpty())
                throw new IllegalArgumentException("Path cannot be null or empty when trying to read Page.");
            if (pageMaxSize <= 0)
                throw new IllegalArgumentException("Page max size must be greater than zero you gave : " + pageMaxSize);
            if (pageMaxSize % 4096 != 0)
                throw new IllegalArgumentException(
                        "Page size must be a modulo of a Blocks Size(4096 BYTES) value you gave is : " + pageMaxSize);

            byte[] buffer = new byte[pageMaxSize];
            try {
                RandomAccessFile raf = new RandomAccessFile(path, "r");
                raf.seek(pagePosition);
                int bytesRead = raf.read(buffer, 0, pageMaxSize);
                if (bytesRead == -1) {
                    // File is empty or end of file is reached
                    raf.close();
                    return null; // Return an empty byte array
                }
                if (bytesRead < pageMaxSize) {
                    byte[] actualBytes = new byte[bytesRead];
                    System.arraycopy(buffer, 0, actualBytes, 0, bytesRead);
                    buffer = actualBytes;
                }
                raf.close();
            } catch (FileNotFoundException e) {
                System.err.println("File not found(Can not read Page): " + path);
                throw new RuntimeException("File not found at specified path: " + path, e);
            } catch (IOException e) {
                System.err.println("Error reading from file: " + path);
                throw new RuntimeException("Failed to read page from file: " + path, e);
            }
            return buffer;
        });
        fileIOThread.submit(readTask);
        return readTask;
    }

    public void deleteLastPage(String path, int pageSize) throws ExecutionException, InterruptedException{
        FutureTask<Boolean> future = new FutureTask<>(() -> {
            try (RandomAccessFile file = new RandomAccessFile(path, "rw");
                    FileChannel fileChannel = file.getChannel()) {

                long fileSize = fileChannel.size();
                long newSize = fileSize - pageSize;
                if (newSize < 0) {
                    throw new IOException("File is smaller than " + pageSize + " bytes; cannot truncate.");
                }

                fileChannel.truncate(newSize);
                System.out.println(
                        "======== Last " + pageSize + " bytes truncated. New file size: " + newSize + " bytes.");
                        return true;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to delete last page", e);
            }
        });
        fileIOThread.submit(future);
        future.get();
    }
}
