package org.jgit.scp;

import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.jgit.generated.storage.dht.proto.GitStore;
import org.eclipse.jgit.storage.dht.*;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SCPRepositoryTable implements RepositoryTable {

    private final Connection connection;

    private final String tableName;

    private final String metaTableName;

    public SCPRepositoryTable(Connection connection, String tableName, String metaTableName) {
        this.connection = connection;
        this.tableName = tableName;
        this.metaTableName = metaTableName;
    }

    @Override
    public RepositoryKey nextKey() throws DhtException {
        try {
            String sql = "SELECT \"" + IPropertyConstants.REPO_INFO.KEY + "\" FROM \"" + this.metaTableName + "\" WHERE \"" +
                    IPropertyConstants.REPO_INFO.METADATA + "\" = 0";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            ResultSet rs = sqlStmt.executeQuery();
            int key = 0;
            if (rs.next()) {
                key = rs.getInt(IPropertyConstants.REPO_INFO.KEY);
            }

            sql = "INSERT INTO \"" + this.metaTableName + "\" (\"" + IPropertyConstants.REPO_INFO.METADATA + "\", \"" + IPropertyConstants.REPO_INFO.KEY + "\"" +
                    ") VALUES (0, " + (key+1) + ") ON CONFLICT (\"" + IPropertyConstants.REPO_INFO.KEY + "\") DO UPDATE SET \"" + IPropertyConstants.REPO_INFO.KEY + "\" = " +
                    (key+1) + ", \"" + IPropertyConstants.REPO_INFO.METADATA + "\" = 0";
            sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.executeUpdate();

            sql = "SELECT \"" + IPropertyConstants.REPO_INFO.KEY + "\" FROM \"" + this.metaTableName + "\" WHERE \"" +
                    IPropertyConstants.REPO_INFO.METADATA + "\" = 0";
            sqlStmt = this.connection.prepareStatement(sql);
            rs = sqlStmt.executeQuery();
            rs.next();
            return RepositoryKey.fromInt(rs.getInt(IPropertyConstants.REPO_INFO.KEY));
        } catch (SQLException ex) {
            throw new DhtException(ex);
        }
    }

    @Override
    public void put(RepositoryKey repositoryKey, ChunkInfo chunkInfo, WriteBuffer writeBuffer) throws DhtException {
        try {
            String key = chunkInfo.getChunkKey().asString();

            String sql = "INSERT INTO \"" + this.tableName + "\" (\"" + IPropertyConstants.REPOS.ID + "\", " +
                    "\"" + IPropertyConstants.REPOS.KEY + "\", \"" +
                    IPropertyConstants.REPOS.DATA + "\") VALUES (?, ?, ?) ON CONFLICT (\"" + IPropertyConstants.REPOS.ID + "\") DO UPDATE SET " +
                    "\"" + IPropertyConstants.REPOS.ID + "\" = ?, \"" + IPropertyConstants.REPOS.KEY + "\" = ?, \"" + IPropertyConstants.REPOS.DATA + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());
            sqlStmt.setObject(2, key);
            sqlStmt.setObject(3, chunkInfo.getData().toByteArray());
            sqlStmt.setObject(4, repositoryKey.asInt());
            sqlStmt.setObject(5, key);
            sqlStmt.setObject(6, chunkInfo.getData().toByteArray());

            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new DhtException(ex);
        }
    }

    @Override
    public void remove(RepositoryKey repositoryKey, ChunkKey chunkKey, WriteBuffer writeBuffer) throws DhtException {
        try {
            String sql = "INSERT INTO \"" + this.tableName + "\" (\"" + IPropertyConstants.REPOS.ID + "\", " +
                    "\"" + IPropertyConstants.REPOS.KEY + "\") VALUES (?, ?) WHERE " +
                    "ON CONFLICT (\"" + IPropertyConstants.REPOS.ID + "\") DO UPDATE SET " +
                    "\"" + IPropertyConstants.REPOS.ID + "\" = ?, \"" + IPropertyConstants.REPOS.KEY + "\" = ?, " +
                    "\"" + IPropertyConstants.REPOS.KEY + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());
            sqlStmt.setObject(2, null);
            sqlStmt.setObject(3, repositoryKey.asInt());
            sqlStmt.setObject(4, null);

            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new DhtException(ex);
        }
    }

    @Override
    public Collection<GitStore.CachedPackInfo> getCachedPacks(RepositoryKey repositoryKey) throws DhtException {
        try {
            String sql = "SELECT \"" + IPropertyConstants.REPOS.PACKS_KEY + "\", \"" + IPropertyConstants.REPOS.PACKS_VALUE
                    + "\" FROM \"" + this.tableName + "\"" +
                    " WHERE \"" + IPropertyConstants.REPOS.ID + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());

            ResultSet object = sqlStmt.executeQuery();
            if (object == null) {
                return Collections.emptyList();
            }

            List<GitStore.CachedPackInfo> info = new ArrayList<>();
            while (object.next()) {
                String key = object.getString(IPropertyConstants.REPOS.PACKS_KEY);
                byte[] value = object.getBytes(IPropertyConstants.REPOS.PACKS_VALUE);
                try {
                    info.add(GitStore.CachedPackInfo.parseFrom(value));
                } catch (InvalidProtocolBufferException e) {
                    throw new DhtException(e);
                }
            }
            return info;
        } catch (SQLException ex) {
            throw new DhtException(ex);
        }
    }

    @Override
    public void put(RepositoryKey repositoryKey, GitStore.CachedPackInfo cachedPackInfo,
                    WriteBuffer writeBuffer) throws DhtException {
        try {
            CachedPackKey key = CachedPackKey.fromInfo(cachedPackInfo);
            String sql = "INSERT INTO \"" + this.tableName + "\" (\"" + IPropertyConstants.REPOS.ID + "\", \"" + IPropertyConstants.REPOS.PACKS_KEY + "\", " +
                    "\"" + IPropertyConstants.REPOS.PACKS_VALUE + "\") VALUES (?, ?, ?) ON CONFLICT (\"" + IPropertyConstants.REPOS.ID + "\") DO UPDATE SET " +
                    "\"" + IPropertyConstants.REPOS.ID + "\" = ?, \"" + IPropertyConstants.REPOS.PACKS_KEY + "\" = ?, \"" + IPropertyConstants.REPOS.PACKS_VALUE + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());
            sqlStmt.setObject(2, key.asString());
            sqlStmt.setObject(3, cachedPackInfo.toByteArray());
            sqlStmt.setObject(4, repositoryKey.asInt());
            sqlStmt.setObject(5, key.asString());
            sqlStmt.setObject(6, cachedPackInfo.toByteArray());

            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new DhtException(ex);
        }
    }

    @Override
    public void remove(RepositoryKey repositoryKey, CachedPackKey cachedPackKey,
                       WriteBuffer writeBuffer) throws DhtException {
        try {
            String sql = "UPDATE \"" + this.tableName + "\" SET \"" + IPropertyConstants.REPOS.PACKS_KEY + "\" = NULL " +
                    "WHERE \"" + IPropertyConstants.REPOS.ID + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());
            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new DhtException(ex);
        }
    }
}