package org.jgit.scp;

import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SCPRepositoryIndexTable implements RepositoryIndexTable {

    private final Connection connection;
    private final String tableName;

    public SCPRepositoryIndexTable(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public RepositoryKey get(RepositoryName repositoryName) {
        try {
            String sql = "SELECT * FROM \"" + this.tableName + "\"";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            ResultSet rs = sqlStmt.executeQuery();
            Object value = null;
            while (rs.next()) {
                if (repositoryName.asString().equals(rs.getString(IPropertyConstants.REPO_INDEX.REPOSITORY_NAME))) {
                    value = rs.getObject(IPropertyConstants.REPO_INDEX.REPOSITORY_KEY);
                }
            }
            if (value == null) {
                return null;
            }
            return RepositoryKey.fromInt(SCPUtils.getInt(value));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void putUnique(RepositoryName repositoryName, RepositoryKey repositoryKey) {
        try {
            String sql = "INSERT INTO \"" + this.tableName + "\" (\"" + IPropertyConstants.REPO_INDEX.REPOSITORY_KEY + "\"," +
                    " \"" + IPropertyConstants.REPO_INDEX.REPOSITORY_NAME + "\")" + " VALUES(?, ?) ON CONFLICT (\"" + IPropertyConstants.REPO_INDEX.REPOSITORY_KEY + "\") DO UPDATE SET " +
                    "\"" + IPropertyConstants.REPO_INDEX.REPOSITORY_KEY + "\" = ?, \"" + IPropertyConstants.REPO_INDEX.REPOSITORY_NAME + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());
            sqlStmt.setObject(2, repositoryName.asString());
            sqlStmt.setObject(3, repositoryKey.asInt());
            sqlStmt.setObject(4, repositoryName.asString());

            sqlStmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void remove(RepositoryName repositoryName, RepositoryKey repositoryKey) {
        try {
            String sql = "SELECT \"" + IPropertyConstants.REPO_INDEX.REPOSITORY_NAME + "\" FROM \"" + this.tableName
                    + "\" WHERE \"" + IPropertyConstants.REPO_INDEX.REPOSITORY_KEY + "\" = ?";
            PreparedStatement sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());
            ResultSet rs = sqlStmt.executeQuery();
            rs.next();
            String currentName = rs.getString(IPropertyConstants.REPO_INDEX.REPOSITORY_NAME);
            if (currentName == null || !currentName.equals(repositoryName.asString())) {
                return;
            }
            sql = "UPDATE \"" + this.tableName + "\" SET \"" + IPropertyConstants.REPO_INDEX.REPOSITORY_NAME + "\" = NULL WHERE " +
                    "\"" + IPropertyConstants.REPO_INDEX.REPOSITORY_KEY + "\" = ?";
            sqlStmt = this.connection.prepareStatement(sql);
            sqlStmt.setObject(1, repositoryKey.asInt());
            sqlStmt.executeUpdate();

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}