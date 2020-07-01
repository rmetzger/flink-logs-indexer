create table test_results
(
    id       INTEGER
        constraint table_name_pk
            primary key autoincrement,
    name     TEXT    not null,
    duration INTEGER not null,
    "commit" TEXT,
    added_at DATE
);

