package org.jgit.scp;

import org.eclipse.persistence.internal.jpa.jdbc.DataSourceImpl;

import javax.naming.InitialContext;
import javax.sql.DataSource;

public class SCPUtils {

    public static int getInt(Object value) {
        return value instanceof Integer ? ((Integer) value).intValue(): -1;
    }

    public static DataSource getDB(String dbUrl, String username, String password) {
        return new DataSourceImpl("default", dbUrl,
                username, password);
    }

    public static String getCollection(DataSource dataSource, String name) throws Exception {
        return name;
    }
}