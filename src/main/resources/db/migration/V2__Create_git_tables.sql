create table "REPO_INDEX1" (
    "REPOSITORY_KEY" VARCHAR(1000),
    "REPOSITORY_NAME" VARCHAR(1000)
);

create table "REPO_INFO1" (
    "KEY" INTEGER,
    "METADATA" INTEGER
);

create table "REPOS1" (
    "ID" INTEGER,
    "KEY" VARCHAR(1000),
    "DATA" BYTEA,
    "PACKS_KEY" VARCHAR(1000),
    "PACKS_VALUE" BYTEA
);

create table "REFS1" (
    "REPO" VARCHAR(1000),
    "NAME" VARCHAR(1000),
    "DATA" BYTEA
);

create table "OBJECTS1" (
    "ID" VARCHAR(1000),
    "VALUES" VARCHAR(1000),
    "DATA" BYTEA
);

create table "CHUNKS1" (
    "ID" VARCHAR(1000),
    "INDEX" BYTEA,
    "DATA" BYTEA,
    "META" BYTEA
);