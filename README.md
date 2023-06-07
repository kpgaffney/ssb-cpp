# SSB on C++

SSB queries hardcoded in C++.

## Getting started

### Loading the database

This project takes as input a SQLite database containing encoded SSB data. To produce this database, navigate into a directory containing the SSB `.tbl` files. Then, run the following.

```shell
sqlite3 ssb.db < path/to/ssb-cpp/sql/load.sql
```

Replace `path/to/ssb-cpp` with the path to this project.

### Building the executable

Create and navigate into a build directory.

```shell
mkdir build && cd build
```

Configure with CMake.

```shell
cmake ..
```

Build with CMake.

```shell
cmake --build .
```

### Running the queries

From the build directory, run the following.

```shell
./ssb_cpp path/to/ssb.db
```

To save only the timing results to a file, run the following.

```shell
./ssb_cpp path/to/ssb.db 2>results.csv
```

Replace `path/to/ssb.db` with the path to the SQLite database created earlier.
