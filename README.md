jgit-sap-hana
=============

- This project is a [JGit](https://www.eclipse.org/jgit/) DHT implementation using PostGRESql as the backing database that uses the PostGRESql JDBC Driver for connecting to PostGRESql.

Build from source
=================

- For migrating database, start a `PostGRESql` db,

```
mvn flyway:migrate
```

- Build library

```
mvn clean install
```

Examples
========

[JGitSCPApp.java](src/test/java/org/jgit/scp/JGitSCPApp.java)