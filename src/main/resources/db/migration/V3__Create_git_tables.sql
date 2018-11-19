create table "REPO_INDEX2" (
    "REPOSITORY_KEY" VARCHAR(1000),
    "REPOSITORY_NAME" VARCHAR(1000),
    PRIMARY KEY("REPOSITORY_KEY")
);

create table "REPO_INFO2" (
    "KEY" INTEGER,
    "METADATA" INTEGER,
    PRIMARY KEY("KEY")
);

create table "REPOS2" (
    "ID" INTEGER,
    "KEY" VARCHAR(1000),
    "DATA" BYTEA,
    "PACKS_KEY" VARCHAR(1000),
    "PACKS_VALUE" BYTEA,
    PRIMARY KEY("ID")
);

create table "REFS2" (
    "REPO" VARCHAR(1000),
    "NAME" VARCHAR(1000),
    "DATA" BYTEA,
    PRIMARY KEY("REPO")
);

create table "OBJECTS2" (
    "ID" VARCHAR(1000),
    "VALUES" VARCHAR(1000),
    "DATA" BYTEA,
    PRIMARY KEY("ID")
);

create table "CHUNKS2" (
    "ID" VARCHAR(1000),
    "INDEX" BYTEA,
    "DATA" BYTEA,
    "META" BYTEA,
    PRIMARY KEY("ID")
);