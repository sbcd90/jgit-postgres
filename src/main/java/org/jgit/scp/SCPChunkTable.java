package org.jgit.scp;

import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.jgit.generated.storage.dht.proto.GitStore;
import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.PackChunk;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SCPChunkTable implements ChunkTable {

    private final Connection connection;

    private final String tableName;

    public SCPChunkTable(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public void get(Context context, Set<ChunkKey> set,
                    AsyncCallback<Collection<PackChunk.Members>> asyncCallback) {
        try {
            List<PackChunk.Members> out = new ArrayList<>(set.size());
            for (ChunkKey chunkKey : set) {
                String sql = "SELECT * FROM \"" + this.tableName + "\" WHERE \"" + IPropertyConstants.CHUNKS.ID + "\" = ?";
                PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
                sqlStmt.setObject(1, chunkKey.asString());
                ResultSet value = sqlStmt.executeQuery();
                value.next();

                byte[] buffer = value.getBytes(IPropertyConstants.CHUNKS.DATA);
                if (buffer == null) {
                    continue;
                }
                PackChunk.Members members = new PackChunk.Members();
                members.setChunkKey(chunkKey);
                members.setChunkData(buffer);

                buffer = value.getBytes(IPropertyConstants.CHUNKS.INDEX);
                if (buffer != null) {
                    members.setChunkIndex(buffer);
                }
                buffer = value.getBytes(IPropertyConstants.CHUNKS.META);
                if (buffer != null) {
                    try {
                        members.setMeta(GitStore.ChunkMeta.parseFrom(buffer));
                    } catch (InvalidProtocolBufferException e) {
                        asyncCallback.onFailure(new DhtException(e));
                        return;
                    }
                }
                out.add(members);
            }
            asyncCallback.onSuccess(out);
        } catch (SQLException ex) {
            asyncCallback.onFailure(new DhtException(ex));
        }
    }

    @Override
    public void getMeta(Context context, Set<ChunkKey> set,
                        AsyncCallback<Map<ChunkKey, GitStore.ChunkMeta>> asyncCallback) {
        Map<ChunkKey, GitStore.ChunkMeta> out = new HashMap<>();
        try {
            for (ChunkKey chunkKey: set) {
                String sql = "SELECT * FROM \"" + this.tableName + "\" WHERE \"" + IPropertyConstants.CHUNKS.ID + "\" = ?";
                PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
                sqlStmt.setObject(1, chunkKey.asString());

                ResultSet object = sqlStmt.executeQuery();
                object.next();
                byte[] value = object.getBytes(IPropertyConstants.CHUNKS.META);
                if (value != null) {
                    out.put(chunkKey, GitStore.ChunkMeta.parseFrom(value));
                }
            }
            asyncCallback.onSuccess(out);
        } catch (SQLException ex) {
            asyncCallback.onFailure(new DhtException(ex));
        } catch (InvalidProtocolBufferException ex) {
            asyncCallback.onFailure(new DhtException(ex));
        }
    }

    protected void upsert(String id, String field, byte[] data) {
        try {
            String sql = "INSERT INTO \"" + this.tableName + "\" (\"" + IPropertyConstants.CHUNKS.ID + "\", \"" + field + "\") VALUES (?, ?)" +
                    " ON CONFLICT (\"" + IPropertyConstants.CHUNKS.ID + "\") DO UPDATE SET \"" + IPropertyConstants.CHUNKS.ID + "\" = ?, " +
                    "\"" + field + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, id);
            sqlStmt.setObject(2, data);
            sqlStmt.setObject(3, id);
            sqlStmt.setObject(4, data);
            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void put(PackChunk.Members members, WriteBuffer writeBuffer) {
        final String id = members.getChunkKey().asString();
        if (members.hasChunkData()) {
            upsert(id, IPropertyConstants.CHUNKS.DATA, members.getChunkData());
        }
        if (members.hasChunkIndex()) {
            upsert(id, IPropertyConstants.CHUNKS.INDEX, members.getChunkIndex());
        }
        if (members.hasMeta()) {
            upsert(id, IPropertyConstants.CHUNKS.META, members.getMeta().toByteArray());
        }
    }

    @Override
    public void remove(ChunkKey chunkKey, WriteBuffer writeBuffer) {
        try {
            String sql = "DELETE FROM \"" + this.tableName + "\" WHERE \"" + IPropertyConstants.CHUNKS.ID + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, chunkKey.asString());
            sqlStmt.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}