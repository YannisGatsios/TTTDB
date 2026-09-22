package com.database.tttdb.core;

import com.database.tttdb.core.page.Page;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class FileIO implements AutoCloseable {

    private static final Logger logger =
            Logger.getLogger(FileIO.class.getName());

    /*
     * Prevent one enormous allocation if thousands of consecutive pages
     * are being committed. Adjust after benchmarking.
     */
    private static final int MAX_BATCH_BYTES = 4 * 1024 * 1024;

    private final Map<Path, FileChannel> channels = new HashMap<>();

    private FileIOThread fileIOThread;

    public FileIO(FileIOThread fileIOThread) {
        this.fileIOThread = fileIOThread;
    }

    public void setFileIOThread(FileIOThread fileIOThread) {
        if (fileIOThread == null) {
            throw new IllegalArgumentException(
                    "FileIOThread cannot be null."
            );
        }
        /*
        * A replacement worker must not inherit channels belonging to the
        * previous worker or database lifecycle.
        */
        if (!channels.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot replace FileIOThread while persistent "
                            + "file channels remain open."
            );
        }
        this.fileIOThread = fileIOThread;
    }

    public static int getNumOfPages(String path, int sizeOfEntry) {
        File file = new File(path);
        long fileSize = file.length();
        int pageSize = Page.pageSizeInBytes(sizeOfEntry);

        return (int) ((fileSize + pageSize - 1) / pageSize);
    }

    /**
     * Queues one page write.
     *
     * pagePosition is a byte offset, not a page number.
     */
    public void writePage(
            String path,
            byte[] pageBuffer,
            long pagePosition
    ) {
        validateWrite(path, pageBuffer, pagePosition);

        /*
         * Copy before submitting. Otherwise the caller could modify the
         * original array while it is waiting in the I/O queue.
         */
        byte[] immutableData =
                Arrays.copyOf(pageBuffer, pageBuffer.length);

        PageWrite write = new PageWrite(
                normalizePath(path),
                pagePosition,
                immutableData
        );

        fileIOThread.submit(() ->
                writeBatches(List.of(write))
        );
    }

    public void writePages(List<Page> pages)
        throws InterruptedException, ExecutionException {
        if (pages == null) {
            throw new IllegalArgumentException(
                    "Pages cannot be null."
            );
        }

        if (pages.isEmpty()) {
            return;
        }

        /*
        * Convert mutable Page objects into immutable write snapshots before
        * placing the work onto the I/O queue.
        */
        List<PageWrite> writes = new ArrayList<>(pages.size());

        for (Page page : pages) {
            if (page == null) {
                throw new IllegalArgumentException(
                        "Pages cannot contain null values."
                );
            }

            byte[] bytes = page.toBytes();
            String path = page.getFilePath();
            long position = page.getPagePos();

            validateWrite(path, bytes, position);

            writes.add(
                    new PageWrite(
                            normalizePath(path),
                            position,
                            Arrays.copyOf(bytes, bytes.length)
                    )
            );
        }

        FutureTask<Void> writeTask = new FutureTask<>(() -> {
            writeBatches(writes);
            return null;
        });

        fileIOThread.submit(writeTask);

        /*
        * Wait for the batch to finish. This allows Cache.commit() to mark
        * pages clean only after the write succeeds.
        */
        writeTask.get();
    }

    /**
     * Groups writes by file and combines adjacent page writes.
     * Runs only on the FileIOThread.
     */
    private void writeBatches(List<PageWrite> writes) {
        Map<Path, List<PageWrite>> writesByFile = new HashMap<>();

        for (PageWrite write : writes) {
            writesByFile
                    .computeIfAbsent(
                            write.path,
                            ignored -> new ArrayList<>()
                    )
                    .add(write);
        }

        for (Map.Entry<Path, List<PageWrite>> entry
                : writesByFile.entrySet()) {

            Path path = entry.getKey();
            List<PageWrite> fileWrites = entry.getValue();

            fileWrites.sort(
                    Comparator.comparingLong(write -> write.position)
            );

            try {
                FileChannel channel = getWriteChannel(path);
                writeAdjacentRuns(channel, path, fileWrites);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed writing pages to " + path,
                        e
                );
            }
        }
    }

    /**
     * Combines adjacent writes:
     *
     * page 0: position 0,    length 4096
     * page 1: position 4096, length 4096
     * page 2: position 8192, length 4096
     *
     * These become one 12 KiB FileChannel write.
     */
    private void writeAdjacentRuns(
            FileChannel channel,
            Path path,
            List<PageWrite> writes
    ) throws IOException {

        int index = 0;

        while (index < writes.size()) {
            PageWrite first = writes.get(index);

            long batchStartPosition = first.position;
            long expectedNextPosition =
                    first.position + first.data.length;

            int batchSize = first.data.length;
            int endExclusive = index + 1;

            while (endExclusive < writes.size()) {
                PageWrite next = writes.get(endExclusive);

                if (next.position < expectedNextPosition) {
                    throw new IllegalStateException(
                            "Overlapping writes detected for " + path
                                    + ": next position=" + next.position
                                    + ", expected at least="
                                    + expectedNextPosition
                    );
                }

                // There is a gap, so start another batch.
                if (next.position != expectedNextPosition) {
                    break;
                }

                long proposedSize =
                        (long) batchSize + next.data.length;

                // Avoid allocating an excessively large ByteBuffer.
                if (proposedSize > MAX_BATCH_BYTES) {
                    break;
                }

                batchSize += next.data.length;
                expectedNextPosition += next.data.length;
                endExclusive++;
            }

            ByteBuffer batchBuffer =
                    ByteBuffer.allocate(batchSize);

            for (int i = index; i < endExclusive; i++) {
                batchBuffer.put(writes.get(i).data);
            }

            batchBuffer.flip();

            writeFully(
                    channel,
                    batchBuffer,
                    batchStartPosition
            );

            index = endExclusive;
        }
    }

    /**
     * A FileChannel write is not guaranteed to consume the entire buffer
     * in one call, so keep writing until no bytes remain.
     */
    private static void writeFully(
            FileChannel channel,
            ByteBuffer buffer,
            long position
    ) throws IOException {

        long currentPosition = position;

        while (buffer.hasRemaining()) {
            int bytesWritten =
                    channel.write(buffer, currentPosition);

            if (bytesWritten < 0) {
                throw new IOException(
                        "Unexpected end while writing file."
                );
            }

            if (bytesWritten == 0) {
                Thread.yield();
                continue;
            }

            currentPosition += bytesWritten;
        }
    }

    public byte[] readPage(
            String path,
            long pagePosition,
            int pageMaxSize
    ) throws InterruptedException, ExecutionException {

        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException(
                    "Path cannot be null or empty."
            );
        }

        if (pagePosition < 0) {
            throw new IllegalArgumentException(
                    "Page position cannot be negative."
            );
        }

        if (pageMaxSize <= 0
                || pageMaxSize % Page.BLOCK_SIZE != 0) {
            throw new IllegalArgumentException(
                    "Invalid page size: " + pageMaxSize
            );
        }

        Path normalizedPath = normalizePath(path);

        FutureTask<byte[]> readTask = new FutureTask<>(() -> {
            ByteBuffer buffer = ByteBuffer.allocate(pageMaxSize);
            FileChannel channel = getReadChannel(normalizedPath);

            int totalRead = 0;

            while (buffer.hasRemaining()) {
                int bytesRead = channel.read(
                        buffer,
                        pagePosition + totalRead
                );

                if (bytesRead < 0) {
                    break;
                }

                if (bytesRead == 0) {
                    break;
                }

                totalRead += bytesRead;
            }

            if (totalRead == 0) {
                return null;
            }

            /*
             * buffer.array() contains pageMaxSize bytes, so return only
             * the portion actually read for a partial final page.
             */
            return Arrays.copyOf(buffer.array(), totalRead);
        });

        fileIOThread.submit(readTask);
        return readTask.get();
    }

    public void truncateFile(
            String path,
            int pageSize
    ) throws ExecutionException, InterruptedException {

        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException(
                    "Path cannot be null or empty."
            );
        }

        if (pageSize <= 0 || pageSize % Page.BLOCK_SIZE != 0) {
            throw new IllegalArgumentException(
                    "Page size must be a positive multiple of "
                            + Page.BLOCK_SIZE
            );
        }

        Path normalizedPath = normalizePath(path);

        FutureTask<Void> truncateTask = new FutureTask<>(() -> {
            FileChannel channel =
                    getReadChannel(normalizedPath);

            long currentSize = channel.size();
            long newSize = currentSize - pageSize;

            if (newSize < 0) {
                throw new IOException(
                        "File is smaller than one page: "
                                + normalizedPath
                );
            }

            channel.truncate(newSize);

            logger.fine(
                    "Truncated " + pageSize
                            + " bytes from " + normalizedPath
            );

            return null;
        });

        fileIOThread.submit(truncateTask);
        truncateTask.get();
    }

    /**
     * Waits for all previously queued writes and asks the operating
     * system to flush file contents to storage.
     *
     * Call this for a durable commit, not after every individual page.
     */
    public void forceAll()
            throws ExecutionException, InterruptedException {

        FutureTask<Void> forceTask = new FutureTask<>(() -> {
            for (FileChannel channel : channels.values()) {
                if (channel.isOpen()) {
                    channel.force(false);
                }
            }

            return null;
        });

        fileIOThread.submit(forceTask);
        forceTask.get();
    }

    private FileChannel getWriteChannel(Path path)
            throws IOException {

        FileChannel existing = channels.get(path);

        if (existing != null && existing.isOpen()) {
            return existing;
        }

        FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );

        channels.put(path, channel);
        return channel;
    }

    private FileChannel getReadChannel(Path path)
            throws IOException {

        FileChannel existing = channels.get(path);

        if (existing != null && existing.isOpen()) {
            return existing;
        }

        /*
         * Do not use CREATE here. A read of a nonexistent database file
         * should fail instead of silently creating an empty file.
         *
         * WRITE is included so the same channel can later be reused for
         * database writes.
         */
        FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        channels.put(path, channel);
        return channel;
    }

    private static Path normalizePath(String path) {
        return Path.of(path)
                .toAbsolutePath()
                .normalize();
    }

    private static void validateWrite(
            String path,
            byte[] pageBuffer,
            long pagePosition
    ) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException(
                    "Path cannot be null or empty."
            );
        }

        if (pageBuffer == null || pageBuffer.length == 0) {
            throw new IllegalArgumentException(
                    "Page buffer cannot be null or empty."
            );
        }

        if (pageBuffer.length % Page.BLOCK_SIZE != 0) {
            throw new IllegalArgumentException(
                    "Page size must be a multiple of "
                            + Page.BLOCK_SIZE
            );
        }

        if (pagePosition < 0) {
            throw new IllegalArgumentException(
                    "Page position cannot be negative."
            );
        }
    }

    /**
     * Must run on the FileIOThread after all queued operations.
     */
    private void closeChannels() {
        for (Map.Entry<Path, FileChannel> entry
                : channels.entrySet()) {

            FileChannel channel = entry.getValue();

            try {
                if (channel.isOpen()) {
                    channel.force(false);
                    channel.close();
                }
            } catch (IOException e) {
                logger.log(
                        Level.WARNING,
                        "Failed to flush or close channel for "
                                + entry.getKey(),
                        e
                );
            }
        }
        channels.clear();
    }

    /**
     * Queues channel cleanup after all existing operations, then shuts
     * down the worker.
     */
    @Override
    public void close() throws InterruptedException {
        fileIOThread.shutdown(this::closeChannels);
    }

    private static final class PageWrite {
        private final Path path;
        private final long position;
        private final byte[] data;

        private PageWrite(
                Path path,
                long position,
                byte[] data
        ) {
            this.path = path;
            this.position = position;
            this.data = data;
        }
    }
    public void deleteFiles(Collection<String> filePaths)
        throws InterruptedException, ExecutionException {
        if (filePaths == null) {
            throw new IllegalArgumentException("File paths cannot be null.");
        }

        List<Path> normalizedPaths = filePaths.stream()
                .map(FileIO::normalizePath)
                .toList();

        FutureTask<Void> deleteTask = new FutureTask<>(() -> {
            for (Path path : normalizedPaths) {
                closeChannelNow(path);
                Files.deleteIfExists(path);
            }

            return null;
        });

        /*
        * Because this is submitted to the same I/O thread:
        *
        * previous writes
        * close channels
        * delete files
        *
        * execute in that order.
        */
        fileIOThread.submit(deleteTask);
        deleteTask.get();
    }

    private void closeChannelNow(Path path) throws IOException {
        FileChannel channel = channels.remove(path);

        if (channel == null) {
            return;
        }

        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException e) {
            logger.log(
                    Level.WARNING,
                    "Failed to close channel for " + path,
                    e
            );

            throw e;
        }
    }
}