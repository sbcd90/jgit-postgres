package org.jgit.scp;

public interface IPropertyConstants {

    interface REPO_INDEX {
        String REPOSITORY_KEY = "REPOSITORY_KEY";
        String REPOSITORY_NAME = "REPOSITORY_NAME";
    }

    interface REPO_INFO {
        String KEY = "KEY";
        String METADATA = "METADATA";
    }

    interface REPOS {
        String ID = "ID";
        String KEY = "KEY";
        String DATA = "DATA";
        String PACKS_KEY = "PACKS_KEY";
        String PACKS_VALUE = "PACKS_VALUE";
    }

    interface REFS {
        String REPO = "REPO";
        String NAME = "NAME";
        String DATA = "DATA";
    }

    interface OBJECTS {
        String ID = "ID";
        String VALUES = "VALUES";
        String DATA = "DATA";
    }

    interface CHUNKS {
        String ID = "ID";
        String DATA = "DATA";
        String INDEX = "INDEX";
        String META = "META";
    }
}