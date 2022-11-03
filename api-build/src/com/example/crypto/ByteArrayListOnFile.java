package com.example.crypto;

import javax.lang.model.element.Element;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This class implements file based list of byte[]. you can have virtually unlimited number of the list entries. The
 * available operation is append / random read / sequential read / random delete. You can persist the list by using the
 * SyncAdapter.
 */
public class ByteArrayListOnFile /*implements ByteArrayList*/ {
//    public static final int MINIMUM_OVERHEAD = Shard.ElementHeader.HEADER_SIZE * 2;
//    public static final int DEFAULT_SHARD_SIZE = 100 * 1024 * 1024 + MINIMUM_OVERHEAD; // 100 MB.
    private static final int UNINITIALIZED = -1;
//    private final Optional<SyncAdapter> syncAdapter;
    private Optional<BasicCipherAdapter> cipherAdapter;
    private int shardSize;
    private int firstFileNo = UNINITIALIZED;
//    private final CopyOnWriteArrayList<ByteArrayListOnFileCursor> registeredCursors = new CopyOnWriteArrayList<>();
//    private final ByteArrayListOnFileCursor cursor;

    private volatile int writeFileNo = UNINITIALIZED;
    private Object listLock = new Object();
//    private Map<Integer, Shard> shards = new ConcurrentHashMap<>();

//    private final File fileLocation;
    private File fileLocation;

    /** @param path path the directory where underlying files are stored. */
//    public ByteArrayListOnFile(String path, int shardSize) throws IOException {
//        this(path, shardSize, null, null);
//    }

    public ByteArrayListOnFile(String path/*, SyncAdapter syncAdapter, CipherAdapter cipherAdapter*/) throws IOException {
//        this(path, DEFAULT_SHARD_SIZE/*, syncAdapter, cipherAdapter*/);
    }

    public ByteArrayListOnFile(String path, int shardSize/*, SyncAdapter syncAdapter*/, BasicCipherAdapter cipherAdapter)
            throws IOException {
        this.shardSize = shardSize;
        fileLocation = new File(path);
        if (!fileLocation.isDirectory()) {
            if (!fileLocation.mkdirs()) {
//                throw new ByteArrayListException("making a temporary directory " + fileLocation.toString() + " failed");
            }
        } else {
            try (Stream<Path> list = Files.list(Paths.get(fileLocation.getAbsolutePath()))) {
                // Not using streams forEach to avoid IOException relay.
                Path[] paths = list.toArray(size -> new Path[size]);
                for (Path p : paths) {
                    Files.delete(Paths.get(p.toString()));
                }
            }
        }
//        this.syncAdapter = Optional.ofNullable(syncAdapter);
        this.cipherAdapter = Optional.ofNullable(cipherAdapter);
//        cursor = (ByteArrayListOnFileCursor) createCursor();
//        cursor.readFileNo = UNINITIALIZED;
        sync();
    }

    private void sync() throws IOException {
//        if (syncAdapter.isPresent()) {
//            firstFileNo = syncAdapter.get().getFirstShardId();
//            writeFileNo = syncAdapter.get().getLastShardId();
            // Shared gets created and downloaded at first read
//        }
        if (firstFileNo == UNINITIALIZED) {
            // no files to download. create an empty shard
            firstFileNo = 0;
            writeFileNo = 0;
//            Shard s = new Shard(this, fileLocation, writeFileNo, shardSize);
//            s.setSynced(true);
//            shards.put(writeFileNo, s);
        }
//        cursor.readFileNo = firstFileNo;
    }

//    @Override
    public void rewind() {
//        rewind(cursor);
    }

//    @Override
    public void rewind(Object cursor) {
//        ByteArrayListOnFileCursor c = (ByteArrayListOnFileCursor) cursor;
//        c.readFileNo = firstFileNo;
//        Shard s = shards.get(c.readFileNo);
//        if (s != null) {
//            s.rewind(c);
//        }
    }

//    @Override
    public Element getLast() throws IOException {
        for (int i = writeFileNo; i >= firstFileNo; i--) {
//            Shard s = shards.get(i);
//            if (s != null) {
//                ElementImpl e = decrypt(s.get(-1));
//                if (e != null) return e;
//            }
        }
        return null;
    }

//    @Override
    public void seek(Element e) throws IOException {
//        seek(e, cursor);
    }

//    @Override
    public void seek(Element e, Object cursor) throws IOException {
//        ByteArrayListOnFileCursor c = (ByteArrayListOnFileCursor) cursor;
        ElementImpl impl = getImplementation(e);
//        impl.getShard().seek(impl.getOffset(), c);
//        c.readFileNo = impl.getShard().getId();
        return;
    }

//    @Override
    public Element append(byte[] bytes) throws Exception {
//        Shard s;
        ElementImpl ret = null;
        bytes = encrypt(bytes);
        while (ret == null) {
//            s = shards.get(writeFileNo);
//            if (s == null) {
//                synchronized (listLock) {
//                    s = shards.get(writeFileNo);
//                    if (s == null) {
//                        s = new Shard(this, fileLocation, writeFileNo, shardSize);
//                        shards.put(writeFileNo, s);
//                    }
//                }
//            }
//            if (s.isDeleted()) {
//                increaseWriteFileNo(s);
//                continue;
//            }
//            ret = s.append(bytes);
            if (ret == null) {
//                if (s.getMaxElementSize() < bytes.length) {
//                    throw new ByteArrayListException(
//                            "Cannot add "
//                                    + bytes.length
//                                    + " bytes element. The max supported size is "
//                                    + s.getMaxElementSize());
//                }
//                s.closeWrite();
//                increaseWriteFileNo(s);
            }
        }

//        return ret;
        return null;
    }

//    private void increaseWriteFileNo(Shard s) {
//        synchronized (listLock) {
//            if (s.getId() == writeFileNo) { // nobody has added a new shard yet. So I am doing it now and retry.
//                writeFileNo = writeFileNo + 1;
//            }
//        }
//    }

//    private Shard createShardForRead(Object cursor) throws IOException {
//        ByteArrayListOnFileCursor c = (ByteArrayListOnFileCursor) cursor;
//        synchronized (listLock) {
//            Shard s = shards.get(c.readFileNo);
//            if (s == null) {
//                s = new Shard(this, fileLocation, c.readFileNo, shardSize);
//                if (syncAdapter.isPresent()) {
//                    if (!syncAdapter.get().download(s)) {
//                        s.setDeleted(true);
//                    }
//                    s.setSynced(true);
//                }
//                s.rewind(c);
//                if (s.hasFile()) {
//                    shards.put(c.readFileNo, s);
//                    return s;
//                }
//            }
//            return s;
//        }
//    }

//    @Override
//    public ElementImpl get() throws IOException {
//        return get(cursor);
//    }

//    @Override
//    public ElementImpl get(Object cursor) throws IOException {
//        ByteArrayListOnFileCursor c = (ByteArrayListOnFileCursor) cursor;
//        if (c.readFileNo < firstFileNo) {
//            rewind(c);
//        }
//        Shard s = shards.get(c.readFileNo);
//        while (true) {
//            if (s == null) {
//                s = createShardForRead(c);
//            }
//            ElementImpl e = null;
//            if (!s.isDeleted()) e = decrypt(s.get(c));
//            if (e != null) {
//                return e;
//            }
//            synchronized (listLock) {
//                if (c.readFileNo == writeFileNo) {
//                    return null;
//                }
//                c.readFileNo++;
//            }
//            // rewind the next shard if exists.
//            s = shards.get(c.readFileNo);
//            if (s != null) {
//                s.rewind(c);
//            }
//        }
//    }

//    @Override
    public Element reload(Element e) throws IOException {
        ElementImpl impl = getImplementation(e);
//        return decrypt(impl.getShard().get(impl.getOffset()));
        return null;
    }

//    @Override
    public void checkPoint() throws IOException {
//        synchronized (listLock) {
//            if (syncAdapter.isPresent()) {
//                // The checkpointing must happen in the order of shard id.
//                List<Integer> updated =
//                        shards.entrySet()
//                                .stream()
//                                .filter(s -> !s.getValue().isSynced())
//                                .map(s -> s.getKey())
//                                .sorted()
//                                .collect(Collectors.toList());
//                for (int i : updated) {
//                    Shard s = shards.get(i);
//                    synchronized (s) { // lockup shard to be consistent during the checkpoint
//                        if (s.closeWrite()) {
//                            deleteShard(s.getId());
//                        }
//                        if (!s.isSynced()) {
//                            syncAdapter.get().upload(s);
//                            s.setSynced(true);
//                        }
//                    }
//                }
//            }
//        }
    }

//    @Override
    public Object createCursor() {
//        ByteArrayListOnFileCursor c = new ByteArrayListOnFileCursor();
//        registeredCursors.add(c);
//        return c;
        return null;
    }

//    @Override
//    public void deleteCursor(Object c) {
//        registeredCursors.remove(c);
//    }

//    @Override
    public void delete(Element e) throws IOException {
        ElementImpl impl = getImplementation(e);
//        impl.getShard().deleteElement(impl.getOffset());
    }

    @Override
    public String toString() {
        return String.format("FileQueue-(%s)", fileLocation.getAbsolutePath());
    }

//    @Override
    public void cleanup() {
        synchronized (listLock) {
            try {
//                for (Shard s : shards.values()) {
//                    s.setDeleted(true);
//                    s.setSynced(true);
//                    s.delete();
//                }
                Files.delete(Paths.get(fileLocation.getAbsolutePath()));
            } catch (IOException e) {
//                LOG.log(Level.INFO, "the file deletion failed.", e);
            }
        }
    }

    void deleteShard(int id) throws IOException {
//        Shard s = shards.get(id);
//        s.setDeleted(true);
//        s.setSynced(false);
//        s.delete();
    }

    public ElementImpl decrypt(ElementImpl e) throws Exception {
        if (e != null && cipherAdapter.isPresent()) {
            byte[] data = e.getData();

            byte[] decrypted = cipherAdapter.get().decrypt(data);
            ByteBuffer b = ByteBuffer.wrap(decrypted);
            b.order(ByteOrder.LITTLE_ENDIAN);
            e.setByteBuffer(b);
        }

        return e;
    }

    public byte[] encrypt(byte[] bytes) throws Exception {
        if (cipherAdapter.isPresent()) {
            bytes = cipherAdapter.get().encrypt(bytes);
        }
        return bytes;
    }

    public ElementImpl getImplementation(Element e) {
//        if (!(e instanceof ElementImpl)) {
//            throw new ByteArrayListException("Invalid element " + e);
//        }
        return (ElementImpl) e;
    }

    // the methods below are for test.
    int getWriteFileNo() {
        return writeFileNo;
    }

    void setShardSize(int shardSize) {
        this.shardSize = shardSize;
    }

    int getNumberOfShards() {
//        return (int) shards.values().stream().filter(s -> !s.isDeleted()).count();
        return -1;
    }

    // This is only for test. No need to access shards directly.
//    Map<Integer, Shard> getShards() {
//        return shards;
//    }

    void updateCursor(int id, int offset, int newLastRead) {
//        for (ByteArrayListOnFileCursor c : registeredCursors) {
//            if (c.lastRead == offset && c.readFileNo == id) {
//                c.lastRead = newLastRead;
//            }
//        }
    }
}
