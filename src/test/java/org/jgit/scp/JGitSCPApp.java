package org.jgit.scp;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;

public class JGitSCPApp {
    private final String dbUrl;
    private final String username;
    private final String password;
    private final SCPDatabase db;

    public JGitSCPApp(String dbUrl, String username, String password) throws Exception {
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;

        this.db = new SCPDatabase(this.dbUrl,
                this.username, this.password);
    }

    public Repository createRepository(String repoName, String repoUri, boolean isHttps) throws Exception {
        Repository repository = SCPDatabase.open(db, repoName);
        repository.create(true);
        StoredConfig config = repository.getConfig();
        RemoteConfig remoteConfig = new RemoteConfig(config, "origin");
        URIish uri = new URIish(repoUri);
        remoteConfig.addURI(uri);
        remoteConfig.update(config);

        if (isHttps) {
            config.setBoolean("http", null, "sslVerify", false);
        }

        config.save();
        RefSpec spec = new RefSpec("refs/heads/*:refs/remotes/origin/*");
        Git.wrap(repository).fetch().setRemote("origin").setRefSpecs(spec).call();

        return repository;
    }


    public RevWalk getWalkObject(Repository repository) throws Exception {
        RevWalk walk = new RevWalk(repository);
        walk.markStart(walk.parseCommit(repository.resolve("origin/master")));
        return walk;
    }

    public byte[] findAFile(String filePath, Repository repository, RevWalk walk) throws Exception {
        for (RevCommit commit: walk) {
            RevTree tree = commit.getTree();

            TreeWalk treeWalk = new TreeWalk(repository);
            treeWalk.addTree(tree);
            treeWalk.setRecursive(true);
            treeWalk.setFilter(PathFilter.create(filePath));

            if (treeWalk.next()) {
                ObjectId objectId = treeWalk.getObjectId(0);
                ObjectLoader loader = repository.open(objectId);
                return loader.getBytes();
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {

        JGitSCPApp jGitSCPApp = new JGitSCPApp("jdbc:postgresql://host:port/",
                "****", "****");

        String uuid = java.util.UUID.randomUUID().toString();
        Repository repository = jGitSCPApp.createRepository("hello-world-serial-" + uuid,
                "git://github.com/sbcd90/orientdb-binary-protocol-implementation.git",
                false);
        RevWalk walk = jGitSCPApp.getWalkObject(repository);

        byte[] file = jGitSCPApp.findAFile("README.md", repository, walk);
        System.out.println(new String(file));
    }
}