#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "oneapi/tbb.h"

#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;

using Accumulator = std::vector<std::pair<bool, int64_t>>;

Accumulator agg_merge(Accumulator a, const Accumulator &b) {
  for (size_t i = 0; i < a.size(); ++i) {
    a[i].first = a[i].first || b[i].first;
    a[i].second += b[i].second;
  }
  return a;
}

struct Q2Row {
  Q2Row(uint16_t d_year, uint16_t p_brand1, uint32_t sum_lo_revenue)
      : d_year(d_year), p_brand1(p_brand1), sum_lo_revenue(sum_lo_revenue) {}

  friend bool operator==(const Q2Row &a, const Q2Row &b) {
    return a.d_year == b.d_year && a.p_brand1 == b.p_brand1 &&
           a.sum_lo_revenue == b.sum_lo_revenue;
  }

  friend std::ostream &operator<<(std::ostream &os, const Q2Row &row) {
    os << row.d_year << '|' << std::setw(4) << row.p_brand1 << '|'
       << row.sum_lo_revenue;
    return os;
  }

  uint16_t d_year;
  uint16_t p_brand1;
  uint32_t sum_lo_revenue;
};

template <typename T> void print(std::vector<T> result) {
  if (result.empty()) {
    return;
  }

  std::cout << result.front() << std::endl;

  if (result.size() > 2) {
    std::cout << "..." << std::endl;
  }

  std::cout << result.back() << std::endl;
}

template <typename T>
void log(const std::string &query, const std::string &key, T &&value) {
  std::cout << R"({"query": ")" << query << R"(", ")" << key << R"(": ")"
            << value << R"("})" << std::endl;
}

template <typename T> struct Column {
  Column(size_t argIndex, std::vector<T> &argValues)
      : index(argIndex), values(argValues) {}
  size_t index;
  std::vector<T> &values;
};

void read_column(sqlite3_stmt *stmt, Column<uint8_t> &column) {
  int value = sqlite3_column_int(stmt, (int)column.index);
  column.values.push_back(value);
}

void read_column(sqlite3_stmt *stmt, Column<uint16_t> &column) {
  int value = sqlite3_column_int(stmt, (int)column.index);
  column.values.push_back(value);
}

void read_column(sqlite3_stmt *stmt, Column<uint32_t> &column) {
  int value = sqlite3_column_int(stmt, (int)column.index);
  column.values.push_back(value);
}

void read_column(sqlite3_stmt *stmt, Column<std::string> &column) {
  std::string value = (char *)sqlite3_column_text(stmt, (int)column.index);
  column.values.push_back(value);
}

template <typename... T>
void read_table(const char *path, const char *table, Column<T>... columns) {
  int rc;

  sqlite3 *db;
  rc = sqlite3_open(path, &db);
  if (rc != SQLITE_OK) {
    throw std::runtime_error(sqlite3_errmsg(db));
  }

  std::string sql = "SELECT * FROM " + std::string(table);

  sqlite3_stmt *stmt;
  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    throw std::runtime_error(sqlite3_errmsg(db));
  }

  while (true) {
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
      (read_column(stmt, columns), ...);
    } else if (rc == SQLITE_DONE) {
      break;
    } else {
      throw std::runtime_error(sqlite3_errmsg(db));
    }
  }
}

struct Database {
  // Part columns.
  std::vector<uint32_t> p_partkey;
  std::vector<uint8_t> p_mfgr;
  std::vector<uint8_t> p_category;
  std::vector<uint16_t> p_brand1;

  // Supplier columns.
  std::vector<uint32_t> s_suppkey;
  std::vector<uint8_t> s_city;
  std::vector<uint8_t> s_nation;
  std::vector<uint8_t> s_region;

  // Customer columns
  std::vector<uint32_t> c_custkey;
  std::vector<uint8_t> c_city;
  std::vector<uint8_t> c_nation;
  std::vector<uint8_t> c_region;

  // Date columns.
  std::vector<uint32_t> d_datekey;
  std::vector<uint16_t> d_year;
  std::vector<uint32_t> d_yearmonthnum;
  std::vector<uint32_t> d_yearmonth;
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

template <typename HSK32, typename C1, typename C2>
void q1(const std::string &query, const Database &db, C1 &&c1, C2 &&c2) {
  double latency;
  uint64_t result;

  HSK32 hash_set;
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

template <typename HSK32, typename HMK32V16, typename C1, typename C2>
void q2(const std::string &query, const Database &db, C1 &&c1, C2 &&c2) {
  double latency;

  HSK32 hash_set_supplier;
  latency = time([&] {
    for (size_t i = 0; i < db.s_suppkey.size(); ++i) {
      if (c1(i)) {
        hash_set_supplier.insert(db.s_suppkey[i]);
      }
    }
  });

  log(query, "BuildHashSetSupplier", latency);

  HMK32V16 hash_map_part;
  hash_map_part.reserve(db.p_partkey.size());
  latency = time([&] {
    for (size_t i = 0; i < db.p_partkey.size(); ++i) {
      if (c2(i)) {
        hash_map_part.emplace(db.p_partkey[i], db.p_brand1[i]);
      }
    }
  });

  log(query, "BuildHashMapPart", latency);

  HMK32V16 hash_map_date;
  hash_map_date.reserve(db.d_datekey.size());
  latency = time([&] {
    for (size_t i = 0; i < db.d_datekey.size(); ++i) {
      hash_map_date.emplace(db.d_datekey[i], db.d_year[i]);
    }
  });

  log(query, "BuildHashMapDate", latency);

  Accumulator acc;
  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo_orderdate.size()), Accumulator(512),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto part_it = hash_map_part.find(db.lo_partkey[i]);
            if (part_it != hash_map_part.end() &&
                hash_set_supplier.find(db.lo_suppkey[i]) !=
                    hash_set_supplier.end()) {
              auto date_it = hash_map_date.find(db.lo_orderdate[i]);
              if (date_it != hash_map_date.end()) {
                std::pair<bool, int64_t> &slot =
                    acc[((date_it->second - 1992) << 6) |
                        ((part_it->second - 40) & 0b111111)];
                slot.first = true;
                slot.second += db.lo_revenue[i];
              }
            }
          }
          return acc;
        },
        agg_merge);
  });

  log(query, "Probe", latency);

  std::vector<Q2Row> result;

  for (size_t i = 0; i < acc.size(); ++i) {
    if (acc[i].first) {
      result.emplace_back((i >> 6) + 1992, (i & 0b111111) + 40, acc[i].second);
    }
  }

  std::sort(result.begin(), result.end(), [](const Q2Row &a, const Q2Row &b) {
    return a.d_year < b.d_year ||
           (a.d_year == b.d_year && a.p_brand1 < b.p_brand1);
  });

  print(result);

  std::vector<std::tuple<uint32_t, uint16_t, uint16_t>> agg_input;
  for (size_t i = 0; i < db.lo_orderdate.size(); ++i) {
    auto part_it = hash_map_part.find(db.lo_partkey[i]);
    if (part_it != hash_map_part.end() &&
        hash_set_supplier.find(db.lo_suppkey[i]) != hash_set_supplier.end()) {
      auto date_it = hash_map_date.find(db.lo_orderdate[i]);
      if (date_it != hash_map_date.end()) {
        agg_input.emplace_back(db.lo_revenue[i], date_it->second,
                               part_it->second);
      }
    }
  }

  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, agg_input.size()), Accumulator(512),
        [&agg_input](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto &[lo_revenue, d_year, p_brand1] = agg_input[i];
            std::pair<bool, int64_t> &slot =
                acc[((d_year - 1992) << 6) | ((p_brand1 - 40) & 0b111111)];
            slot.first = true;
            slot.second += lo_revenue;
          }
          return acc;
        },
        agg_merge);
  });

  log(query, "Agg", latency);
}

template <typename HSK32>
void q1p1(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) { return db.d_year[i] == 1993; };
  auto c2 = [&](size_t i) {
    return db.lo_discount[i] >= 1 && db.lo_discount[i] <= 3 &&
           db.lo_quantity[i] < 25;
  };
  q1<HSK32>(query, db, c1, c2);
}

template <typename HSK32>
void q1p2(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) { return db.d_yearmonthnum[i] == 199401; };
  auto c2 = [&](size_t i) {
    return db.lo_discount[i] >= 4 && db.lo_discount[i] <= 6 &&
           db.lo_quantity[i] >= 26 && db.lo_quantity[i] <= 35;
  };
  q1<HSK32>(query, db, c1, c2);
}

template <typename HSK32>
void q1p3(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) {
    return db.d_weeknuminyear[i] == 6 && db.d_year[i] == 1994;
  };
  auto c2 = [&](size_t i) {
    return db.lo_discount[i] >= 5 && db.lo_discount[i] <= 7 &&
           db.lo_quantity[i] >= 36 && db.lo_quantity[i] <= 40;
  };
  q1<HSK32>(query, db, c1, c2);
}

template <typename HSK32, typename HMK32V16>
void q2p1(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) { return db.s_region[i] == 2; };
  auto c2 = [&](size_t i) { return db.p_category[i] == 2; };
  q2<HSK32, HMK32V16>(query, db, c1, c2);
}

template <typename HSK32, typename HMK32V16>
void q2p2(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) { return db.s_region[i] == 3; };
  auto c2 = [&](size_t i) {
    return db.p_brand1[i] >= 254 && db.p_brand1[i] <= 261;
  };
  q2<HSK32, HMK32V16>(query, db, c1, c2);
}

template <typename HSK32, typename HMK32V16>
void q2p3(const std::string &query, const Database &db) {
  auto c1 = [&](size_t i) { return db.s_region[i] == 4; };
  auto c2 = [&](size_t i) { return db.p_brand1[i] == 254; };
  q2<HSK32, HMK32V16>(query, db, c1, c2);
}

template <typename HSK32, typename HMK32V16>
void all(const std::string &name, const Database &db) {
  q1p1<HSK32>("q1p1<" + name + ">", db);
  q1p2<HSK32>("q1p2<" + name + ">", db);
  q1p3<HSK32>("q1p3<" + name + ">", db);
  q2p1<HSK32, HMK32V16>("q2p1<" + name + ">", db);
  q2p2<HSK32, HMK32V16>("q2p2<" + name + ">", db);
  q2p3<HSK32, HMK32V16>("q2p3<" + name + ">", db);
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "USAGE: " << std::endl;
    std::cerr << argv[0] << " DATA_DIR" << std::endl;
    return 1;
  }

  char *data_dir = argv[1];

  Database db;

  read_table(data_dir, "part_encoded", Column(0, db.p_partkey),
             Column(2, db.p_mfgr), Column(3, db.p_category),
             Column(4, db.p_brand1));

  read_table(data_dir, "supplier_encoded", Column(0, db.s_suppkey),
             Column(3, db.s_city), Column(4, db.s_nation),
             Column(5, db.s_region));

  read_table(data_dir, "customer_encoded", Column(0, db.c_custkey),
             Column(3, db.c_city), Column(4, db.c_nation),
             Column(5, db.c_region));

  read_table(data_dir, "date_encoded", Column(0, db.d_datekey),
             Column(4, db.d_year), Column(5, db.d_yearmonthnum),
             Column(6, db.d_yearmonth), Column(11, db.d_weeknuminyear));

  read_table(data_dir, "lineorder", Column(2, db.lo_custkey),
             Column(3, db.lo_partkey), Column(4, db.lo_suppkey),
             Column(5, db.lo_orderdate), Column(8, db.lo_quantity),
             Column(9, db.lo_extendedprice), Column(11, db.lo_discount),
             Column(12, db.lo_revenue), Column(13, db.lo_supplycost));

  all<std::unordered_set<uint32_t>, std::unordered_map<uint32_t, uint16_t>>(
      "std", db);

  all<absl::flat_hash_set<uint32_t>, absl::flat_hash_map<uint32_t, uint16_t>>(
      "absl", db);

  return 0;
}
