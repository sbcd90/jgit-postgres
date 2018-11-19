package org.jgit.scp;

import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.jgit.generated.storage.dht.proto.GitStore;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RefTable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SCPRefTable implements RefTable {

    private final Connection connection;

    private final String tableName;

    public SCPRefTable(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    private PreparedStatement createRepoObject(RepositoryKey key) throws SQLException {
        String sql = "SELECT * FROM \"" + this.tableName + "\" WHERE \"" + IPropertyConstants.REFS.REPO + "\" = ?";
        PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
        sqlStmt.setObject(1, key.asString());
        return sqlStmt;
    }

    @Override
    public Map<RefKey, GitStore.RefData> getAll(Context context, RepositoryKey repositoryKey) throws DhtException {
        try {
            Map<RefKey, GitStore.RefData> out = new HashMap<>();
            PreparedStatement sqlStmt = createRepoObject(repositoryKey);

            ResultSet object = sqlStmt.executeQuery();
            while (object.next()) {
                byte[] data = object.getBytes(IPropertyConstants.REFS.DATA);
                GitStore.RefData parsed;
                try {
                    parsed = GitStore.RefData.parseFrom(data);
                } catch (InvalidProtocolBufferException e) {
                    throw new DhtException(e);
                }
                String name = object.getString(IPropertyConstants.REFS.NAME);
                out.put(RefKey.create(repositoryKey, name), parsed);
            }
            return out;
        } catch (SQLException | DhtException ex) {
            throw new DhtException(ex);
        }
    }

    @Override
    public boolean compareAndPut(RefKey refKey, GitStore.RefData oldData,
                                 GitStore.RefData newData) throws DhtException {
        try {
            String name = refKey.getName();
            String sql = "INSERT INTO \"" + this.tableName + "\" (\"" + IPropertyConstants.REFS.REPO + "\", \"" + IPropertyConstants.REFS.DATA + "\", \"" +
                    IPropertyConstants.REFS.NAME + "\") VALUES (?, ?, ?) ON CONFLICT (\"" + IPropertyConstants.REFS.REPO + "\") DO UPDATE SET " +
                    "\"" + IPropertyConstants.REFS.REPO + "\" = ?, \"" + IPropertyConstants.REFS.DATA + "\" = ?, \"" + IPropertyConstants.REFS.NAME + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, refKey.getRepositoryKey().asString());
            sqlStmt.setObject(2, newData.toByteArray());
            sqlStmt.setObject(3, name);
            sqlStmt.setObject(4, refKey.getRepositoryKey().asString());
            sqlStmt.setObject(5, newData.toByteArray());
            sqlStmt.setObject(6, name);

            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new DhtException(ex);
        }

        return true;
    }

    @Override
    public boolean compareAndRemove(RefKey refKey, GitStore.RefData refData) {
        try {
            String name = refKey.getName();
            String sql = "UPDATE \"" + this.tableName + "\" SET \"" + IPropertyConstants.REFS.DATA + "\" = ?, \"" +
                    IPropertyConstants.REFS.NAME + "\" = ? WHERE \"" + IPropertyConstants.REFS.REPO + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, name);
            sqlStmt.setObject(2, refData.toByteArray());
            sqlStmt.setObject(3, refKey.getRepositoryKey().asString());

            sqlStmt.executeUpdate();
        } catch (SQLException ex) {

        }
        return true;
    }
}