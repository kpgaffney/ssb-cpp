#include "absl/container/flat_hash_set.h"
#include "oneapi/tbb.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;

template <typename T>
void log(const std::string &query, const std::string &key, T &&value) {
  std::cout << R"({"query": ")" << query << R"(", ")" << key << R"(": ")"
            << value << R"("})" << std::endl;
}

namespace csv {

template <typename T> struct Column {
  Column(size_t argIndex, std::vector<T> &argValues)
      : index(argIndex), values(argValues) {}
  size_t index;
  std::vector<T> &values;
};

void parse_item(const std::string &item, uint8_t &value) {
  value = std::stoi(item);
}

void parse_item(const std::string &item, uint16_t &value) {
  value = std::stoi(item);
}

void parse_item(const std::string &item, uint32_t &value) {
  value = std::stoi(item);
}

void parse_item(const std::string &item, std::string &value) { value = item; }

template <typename T>
void parse_item(const std::vector<std::string> &items, Column<T> &column) {
  T value;
  parse_item(items.at(column.index), value);
  column.values.push_back(value);
}

template <typename... T>
void read(const std::filesystem::path &path, char delim, Column<T>... columns) {
  std::ifstream file_stream(path);
  if (!file_stream) {
    throw std::runtime_error(std::strerror(errno));
  }

  std::string line;
  std::string item;
  std::vector<std::string> items;

  while (std::getline(file_stream, line)) {
    std::istringstream line_stream(line);
    while (std::getline(line_stream, item, delim)) {
      items.push_back(item);
    }

    ((parse_item(items, columns)), ...);

    items.clear();
  }
}

} // namespace csv

struct Database {
  // Part columns.
  std::vector<uint32_t> p_partkey;
  std::vector<std::string> p_mfgr;
  std::vector<std::string> p_category;
  std::vector<std::string> p_brand1;

  // Supplier columns.
  std::vector<uint32_t> s_suppkey;
  std::vector<std::string> s_city;
  std::vector<std::string> s_nation;
  std::vector<std::string> s_region;

  // Customer columns
  std::vector<uint32_t> c_custkey;
  std::vector<std::string> c_city;
  std::vector<std::string> c_nation;
  std::vector<std::string> c_region;

  // Date columns.
  std::vector<uint32_t> d_datekey;
  std::vector<uint16_t> d_year;
  std::vector<uint32_t> d_yearmonthnum;
  std::vector<std::string> d_yearmonth;
  std::vector<uint8_t> d_weeknuminyear;

  // Lineorder columns.
  std::vector<uint32_t> lo_custkey;
  std::vector<uint32_t> lo_partkey;
  std::vector<uint32_t> lo_suppkey;
  std::vector<uint32_t> lo_orderdate;
  std::vector<uint8_t> lo_quantity;
  std::vector<uint32_t> lo_extendedprice;
  std::vector<uint8_t> lo_discount;
  std::vector<uint32_t> lo_revenue;
  std::vector<uint32_t> lo_supplycost;
};

template <typename F> double time(F &&f) {
  auto t0 = std::chrono::high_resolution_clock::now();
  f();
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double>(t1 - t0).count();
}

template <typename HashSet, typename C1, typename C2>
void q1(const std::string &query, const Database &db, C1 &&c1, C2 &&c2) {
  double latency;
  uint64_t result;

  HashSet hash_set;
  latency = time([&] {
    for (size_t i = 0; i < db.d_datekey.size(); ++i) {
      if (c1(i)) {
        hash_set.insert(db.d_datekey[i]);
      }
    }
  });

  log(query, "BuildHashSet", latency);

  latency = time([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo_orderdate.size()), uint64_t(0),
        [&](const tbb::blocked_range<size_t> &r, uint64_t acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            if (c2(i) && hash_set.find(db.lo_orderdate[i]) != hash_set.end()) {
              acc += db.lo_extendedprice[i] * db.lo_discount[i];
            }
          }
          return acc;
        },
        std::plus<>());
  });

  log(query, "ProbeHashSet", latency);
  log(query, "Result", result);

  std::vector<size_t> indexes;
  for (size_t i = 0; i < db.lo_orderdate.size(); ++i) {
    if (c2(i) && hash_set.find(db.lo_orderdate[i]) != hash_set.end()) {
      indexes.push_back(i);
    }
  }

  latency = time([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, indexes.size()), uint64_t(0),
        [&](const tbb::blocked_range<size_t> &r, uint64_t acc) {
          for (size_t j = r.begin(); j < r.end(); ++j) {
            size_t i = indexes[j];
            acc += db.lo_extendedprice[i] * db.lo_discount[i];
          }
          return acc;
        },
        std::plus<>());
  });

  log(query, "AggIndex", latency);
  log(query, "Result", result);
}

template <typename HashSet>
void q1p1(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) { return db.d_year[i] == 1993; };
  auto c2 = [&](size_t i) {
    return db.lo_discount[i] >= 1 && db.lo_discount[i] <= 3 &&
           db.lo_quantity[i] < 25;
  };
  q1<HashSet>(query, db, c1, c2);
}

template <typename HashSet>
void q1p2(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) { return db.d_yearmonthnum[i] == 199401; };
  auto c2 = [&](size_t i) {
    return db.lo_discount[i] >= 4 && db.lo_discount[i] <= 6 &&
           db.lo_quantity[i] >= 26 && db.lo_quantity[i] <= 35;
  };
  q1<HashSet>(query, db, c1, c2);
}

template <typename HashSet>
void q1p3(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) {
    return db.d_weeknuminyear[i] == 6 && db.d_year[i] == 1994;
  };
  auto c2 = [&](size_t i) {
    return db.lo_discount[i] >= 5 && db.lo_discount[i] <= 7 &&
           db.lo_quantity[i] >= 36 && db.lo_quantity[i] <= 40;
  };
  q1<HashSet>(query, db, c1, c2);
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "USAGE: " << std::endl;
    std::cerr << argv[0] << " DATA_DIR" << std::endl;
    return 1;
  }

  fs::path data_dir = argv[1];

  Database db;

  csv::read(data_dir / "part.tbl", '|', csv::Column(0, db.p_partkey),
            csv::Column(2, db.p_mfgr), csv::Column(3, db.p_category),
            csv::Column(4, db.p_brand1));

  csv::read(data_dir / "supplier.tbl", '|', csv::Column(0, db.s_suppkey),
            csv::Column(3, db.s_city), csv::Column(4, db.s_nation),
            csv::Column(5, db.s_region));

  csv::read(data_dir / "customer.tbl", '|', csv::Column(0, db.c_custkey),
            csv::Column(3, db.c_city), csv::Column(4, db.c_nation),
            csv::Column(5, db.c_region));

  csv::read(data_dir / "date.tbl", '|', csv::Column(0, db.d_datekey),
            csv::Column(4, db.d_year), csv::Column(5, db.d_yearmonthnum),
            csv::Column(6, db.d_yearmonth),
            csv::Column(11, db.d_weeknuminyear));

  csv::read(data_dir / "lineorder.tbl", '|', csv::Column(2, db.lo_custkey),
            csv::Column(3, db.lo_partkey), csv::Column(4, db.lo_suppkey),
            csv::Column(5, db.lo_orderdate), csv::Column(8, db.lo_quantity),
            csv::Column(9, db.lo_extendedprice),
            csv::Column(11, db.lo_discount), csv::Column(12, db.lo_revenue),
            csv::Column(13, db.lo_supplycost));

  q1p1<std::unordered_set<uint32_t>>("q1p1<std::unordered_set>", db);

  q1p2<std::unordered_set<uint32_t>>("q1p2<std::unordered_set>", db);

  q1p3<std::unordered_set<uint32_t>>("q1p3<std::unordered_set>", db);

  q1p1<absl::flat_hash_set<uint32_t>>("q1p1<absl::flat_hash_set>", db);

  q1p2<absl::flat_hash_set<uint32_t>>("q1p2<absl::flat_hash_set>", db);

  q1p3<absl::flat_hash_set<uint32_t>>("q1p3<absl::flat_hash_set>", db);

  return 0;
}
