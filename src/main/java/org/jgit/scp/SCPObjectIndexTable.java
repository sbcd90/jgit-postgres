package org.jgit.scp;

import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.jgit.generated.storage.dht.proto.GitStore;
import org.eclipse.jgit.storage.dht.*;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SCPObjectIndexTable implements ObjectIndexTable {

    private final Connection connection;

    private final String tableName;

    public SCPObjectIndexTable(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public void get(Context context, Set<ObjectIndexKey> set,
                    AsyncCallback<Map<ObjectIndexKey, Collection<ObjectInfo>>> asyncCallback) {
        try {
            Map<ObjectIndexKey, Collection<ObjectInfo>> out = new HashMap<>();
            for (ObjectIndexKey objId : set) {
                String sql = "SELECT * FROM \"" + this.tableName + "\" WHERE \"" + IPropertyConstants.OBJECTS.ID + "\" = ?";
                PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
                sqlStmt.setObject(1, objId.asString());

                ResultSet fetch = sqlStmt.executeQuery();
                Collection<ObjectInfo> chunks = out.get(objId);
                if (chunks == null) {
                    chunks = new ArrayList<>(4);
                    out.put(objId, chunks);
                }
                while (fetch.next()) {
                    String key = fetch.getString(IPropertyConstants.OBJECTS.VALUES);
                    byte[] value = fetch.getBytes(IPropertyConstants.OBJECTS.DATA);
                    key = unescapeKey(key);
                    try {
                        chunks.add(new ObjectInfo(ChunkKey.fromString(key), 0,
                                GitStore.ObjectInfo.parseFrom(value)));
                    } catch (InvalidProtocolBufferException ex) {
                        asyncCallback.onFailure(new DhtException(ex));
                        return;
                    }
                }
            }
            asyncCallback.onSuccess(out);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String escapeKey(String key) {
        return key.replace(".", ":");
    }

    private String unescapeKey(String key) {
        return key.replace(":", ".");
    }

    @Override
    public void add(ObjectIndexKey objectIndexKey, ObjectInfo objectInfo, WriteBuffer writeBuffer) {
        try {
            String sql = "INSERT INTO \"" + this.tableName + "\" (\"" + IPropertyConstants.OBJECTS.ID + "\", \"" + IPropertyConstants.OBJECTS.VALUES + "\", \"" +
                    IPropertyConstants.OBJECTS.DATA + "\") VALUES (?, ?, ?) ON CONFLICT (\"" + IPropertyConstants.OBJECTS.ID + "\") DO UPDATE SET " +
                    "\"" + IPropertyConstants.OBJECTS.ID + "\" = ?, \"" + IPropertyConstants.OBJECTS.DATA + "\" = ?, \"" + IPropertyConstants.OBJECTS.VALUES + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, objectIndexKey.asString());
            sqlStmt.setObject(2, escapeKey(objectInfo.getChunkKey().asString()));
            sqlStmt.setObject(3, objectInfo.getData().toByteArray());
            sqlStmt.setObject(4, objectIndexKey.asString());
            sqlStmt.setObject(5, objectInfo.getData().toByteArray());
            sqlStmt.setObject(6, escapeKey(objectInfo.getChunkKey().asString()));

            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void remove(ObjectIndexKey objectIndexKey, ChunkKey chunkKey, WriteBuffer writeBuffer) {
        try {
            String id = objectIndexKey.asString();

            String sql = "UPDATE \"" + this.tableName + "\" SET \"" + IPropertyConstants.OBJECTS.VALUES + "\" = NULL, " +
                    "\"" + IPropertyConstants.OBJECTS.DATA + "\" = NULL WHERE \"" + IPropertyConstants.OBJECTS.ID + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, id);
            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}