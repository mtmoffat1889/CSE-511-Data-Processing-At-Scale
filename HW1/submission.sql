CREATE TABLE users(
    userid INT,
    name TEXT NOT NULL,
    PRIMARY KEY (userid)
);

CREATE TABLE movies(
    movieid INT,
    title TEXT NOT NULL,
    PRIMARY KEY (movieid)
);

CREATE TABLE genres(
    genreid INT,
    name TEXT NOT NULL,
    PRIMARY KEY (genreid)
);

CREATE TABLE taginfo(
    tagid INT,
    content TEXT NOT NULL,
    PRIMARY KEY (tagid)
);

CREATE TABLE ratings(
    userid INT,
    movieid INT,
    rating NUMERIC NOT NULL CHECK (rating BETWEEN 0 AND 5),
    timestamp BIGINT,
    PRIMARY KEY (userid, movieid),
    FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
    FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE
);

CREATE TABLE tags(
    userid INT,
    movieid INT,
    tagid INT,
    timestamp BIGINT,
    PRIMARY KEY (userid, movieid, tagid),
    FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
    FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE,
    FOREIGN KEY (tagid) REFERENCES taginfo ON DELETE CASCADE
);

CREATE TABLE hasagenre(
    movieid INT,
    genreid INT,
    PRIMARY KEY (movieid, genreid),
    FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE,
    FOREIGN KEY (genreid) REFERENCES genres ON DELETE CASCADE
);