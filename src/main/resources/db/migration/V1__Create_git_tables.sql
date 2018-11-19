create table "REPO_INDEX" (
    "REPOSITORY_KEY" VARCHAR(1000),
    "REPOSITORY_NAME" VARCHAR(1000)
);

create table "REPO_INFO" (
    "KEY" INTEGER,
    "METADATA" INTEGER
);

create table "REPOS" (
    "ID" INTEGER,
    "KEY" VARCHAR(1000),
    "DATA" BYTEA,
    "PACKS_KEY" VARCHAR(1000),
    "PACKS_VALUE" BYTEA
);

create table "REFS" (
    "REPO" VARCHAR(1000),
    "NAME" VARCHAR(1000),
    "DATA" BYTEA
);

create table "OBJECTS" (
    "ID" VARCHAR(1000),
    "VALUES" VARCHAR(1000),
    "DATA" BYTEA
);

create table "CHUNKS" (
    "ID" VARCHAR(1000),
    "INDEX" BYTEA,
    "DATA" BYTEA,
    "META" BYTEA
);