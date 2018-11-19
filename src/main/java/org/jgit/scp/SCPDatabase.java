package org.jgit.scp;

import org.eclipse.jgit.storage.dht.DhtRepository;
import org.eclipse.jgit.storage.dht.DhtRepositoryBuilder;
import org.eclipse.jgit.storage.dht.spi.Database;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.util.FS;

import javax.sql.DataSource;
import java.io.IOException;

import static org.jgit.scp.ITableConstants.*;

public class SCPDatabase implements Database {

    public static DhtRepository open(SCPDatabase db, String name) throws IOException {
        DhtRepositoryBuilder<DhtRepositoryBuilder, DhtRepository, SCPDatabase> builder = new DhtRepositoryBuilder<>();
        builder.setDatabase(db);
        builder.setRepositoryName(name);
        builder.setMustExist(false);
        builder.setFS(FS.DETECTED);
        return builder.build();
    }

    private final SCPRepositoryIndexTable repositoryIndex;

    private final SCPRepositoryTable repository;

    private final SCPRefTable ref;

    private final SCPObjectIndexTable objectIndex;

    private final SCPChunkTable chunk;

    public SCPDatabase(final DataSource dataSource) throws Exception {
        repositoryIndex = new SCPRepositoryIndexTable(dataSource.getConnection(),
                SCPUtils.getCollection(dataSource, REPO_INDEX));

        repository = new SCPRepositoryTable(dataSource.getConnection(),
                SCPUtils.getCollection(dataSource, REPOS),
                SCPUtils.getCollection(dataSource, REPO_INFO));

        ref = new SCPRefTable(dataSource.getConnection(), SCPUtils.getCollection(dataSource, REFS));

        objectIndex = new SCPObjectIndexTable(dataSource.getConnection(),
                SCPUtils.getCollection(dataSource, OBJECTS));

        chunk = new SCPChunkTable(dataSource.getConnection(), SCPUtils.getCollection(dataSource, CHUNKS));
    }

    public SCPDatabase(String dbUrl, String username, String password) throws Exception {
        this(SCPUtils.getDB(dbUrl, username, password));
    }

    @Override
    public SCPRepositoryIndexTable repositoryIndex() {
        return repositoryIndex;
    }

    @Override
    public SCPRepositoryTable repository() {
        return repository;
    }

    @Override
    public SCPRefTable ref() {
        return ref;
    }

    @Override
    public SCPObjectIndexTable objectIndex() {
        return objectIndex;
    }

    @Override
    public SCPChunkTable chunk() {
        return chunk;
    }

    @Override
    public WriteBuffer newWriteBuffer() {
        return new SCPWriteBuffer();
    }
}