#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "oneapi/tbb.h"

#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

using Accumulator = std::vector<std::pair<bool, int64_t>>;

template <typename K> using hash_set = absl::flat_hash_set<K>;
template <typename K, typename V> using hash_map = absl::flat_hash_map<K, V>;

constexpr size_t n_pt = 256;

Accumulator agg_merge(Accumulator a, const Accumulator &b) {
  for (size_t i = 0; i < a.size(); ++i) {
    a[i].first = a[i].first || b[i].first;
    a[i].second += b[i].second;
  }
  return a;
}

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

struct Part {
  std::vector<uint32_t> partkey;
  std::vector<uint8_t> mfgr;
  std::vector<uint8_t> category;
  std::vector<uint16_t> brand1;
};

struct Supplier {
  std::vector<uint32_t> suppkey;
  std::vector<uint8_t> city;
  std::vector<uint8_t> nation;
  std::vector<uint8_t> region;
};

struct Customer {
  std::vector<uint32_t> custkey;
  std::vector<uint8_t> city;
  std::vector<uint8_t> nation;
  std::vector<uint8_t> region;
};

struct Date {
  std::vector<uint32_t> datekey;
  std::vector<uint16_t> year;
  std::vector<uint32_t> yearmonthnum;
  std::vector<uint32_t> yearmonth;
  std::vector<uint8_t> weeknuminyear;
};

struct Lineorder {
  std::vector<uint32_t> custkey;
  std::vector<uint32_t> partkey;
  std::vector<uint32_t> suppkey;
  std::vector<uint32_t> orderdate;
  std::vector<uint8_t> quantity;
  std::vector<uint32_t> extendedprice;
  std::vector<uint8_t> discount;
  std::vector<uint32_t> revenue;
  std::vector<uint32_t> supplycost;
};

struct Database {
  Part p;
  Supplier s;
  Customer c;
  Date d;
  Lineorder lo;

  std::vector<Part> p_pt;
  std::vector<Customer> c_pt;
};

template <typename F> double time(F &&f) {
  auto t0 = std::chrono::high_resolution_clock::now();
  f();
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double>(t1 - t0).count();
}

// -----------------------------------------------------------------------------
// QUERY FLIGHT 1
// -----------------------------------------------------------------------------

template <typename C1, typename C2>
void q1(const std::string &query, const Database &db, C1 &&c1, C2 &&c2) {
  double latency;
  uint64_t result;

  hash_set<uint32_t> hs;
  latency = time([&] {
    for (size_t i = 0; i < db.d.datekey.size(); ++i) {
      if (c1(i)) {
        hs.insert(db.d.datekey[i]);
      }
    }
  });

  log(query, "BuildHashSet", latency);

  latency = time([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        uint64_t(0),
        [&](const tbb::blocked_range<size_t> &r, uint64_t acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            if (c2(i) && hs.find(db.lo.orderdate[i]) != hs.end()) {
              acc += db.lo.extendedprice[i] * db.lo.discount[i];
            }
          }
          return acc;
        },
        std::plus<>());
  });

  log(query, "ProbeHashSet", latency);

  std::cerr << result << std::endl;

  std::vector<size_t> idx;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    if (c2(i) && hs.find(db.lo.orderdate[i]) != hs.end()) {
      idx.push_back(i);
    }
  }

  latency = time([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, idx.size()),
        uint64_t(0),
        [&](const tbb::blocked_range<size_t> &r, uint64_t acc) {
          for (size_t j = r.begin(); j < r.end(); ++j) {
            size_t i = idx[j];
            acc += db.lo.extendedprice[i] * db.lo.discount[i];
          }
          return acc;
        },
        std::plus<>());
  });

  log(query, "Agg", latency);

  std::cerr << result << std::endl;
}

void q1p1(const Database &db) {
  auto c1 = [&](size_t i) { return db.d.year[i] == 1993; };
  auto c2 = [&](size_t i) {
    return db.lo.discount[i] >= 1 && db.lo.discount[i] <= 3 &&
           db.lo.quantity[i] < 25;
  };
  q1("Q1.1", db, c1, c2);
}

void q1p2(const Database &db) {
  auto c1 = [&](size_t i) { return db.d.yearmonthnum[i] == 199401; };
  auto c2 = [&](size_t i) {
    return db.lo.discount[i] >= 4 && db.lo.discount[i] <= 6 &&
           db.lo.quantity[i] >= 26 && db.lo.quantity[i] <= 35;
  };
  q1("Q1.2", db, c1, c2);
}

void q1p3(const Database &db) {
  auto c1 = [&](size_t i) {
    return db.d.weeknuminyear[i] == 6 && db.d.year[i] == 1994;
  };
  auto c2 = [&](size_t i) {
    return db.lo.discount[i] >= 5 && db.lo.discount[i] <= 7 &&
           db.lo.quantity[i] >= 36 && db.lo.quantity[i] <= 40;
  };
  q1("Q1.3", db, c1, c2);
}

// -----------------------------------------------------------------------------
// QUERY FLIGHT 2
// -----------------------------------------------------------------------------

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

void q2_finalize(Accumulator &acc, std::vector<Q2Row> &result) {
  result.clear();

  for (size_t i = 0; i < acc.size(); ++i) {
    if (acc[i].first) {
      result.emplace_back((i >> 6) + 1992, (i & 0b111111) + 40, acc[i].second);
    }
  }

  std::sort(result.begin(), result.end(), [](const Q2Row &a, const Q2Row &b) {
    return a.d_year < b.d_year ||
           (a.d_year == b.d_year && a.p_brand1 < b.p_brand1);
  });
}

template <typename C1, typename C2>
void q2(const std::string &query, const Database &db, C1 &&c1, C2 &&c2) {
  double latency;
  std::vector<Q2Row> result;

  hash_set<uint32_t> hs_supplier;

  latency = time([&] {
    for (size_t i = 0; i < db.s.suppkey.size(); ++i) {
      if (c1(i)) {
        hs_supplier.insert(db.s.suppkey[i]);
      }
    }
  });

  log(query, "BuildHashSetSupplier", latency);

  std::vector<hash_map<uint32_t, uint16_t>> hm_part(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Part &p_pt = db.p_pt[i];
      for (size_t j = 0; j < p_pt.partkey.size(); ++j) {
        if (c2(i, j)) {
          hm_part[i].emplace(p_pt.partkey[j], p_pt.brand1[j]);
        }
      }
    });
  });

  log(query, "BuildHashMapPart", latency);

  hash_map<uint32_t, uint16_t> hm_date;
  hm_date.reserve(db.d.datekey.size());

  latency = time([&] {
    for (size_t i = 0; i < db.d.datekey.size(); ++i) {
      hm_date.emplace(db.d.datekey[i], db.d.year[i]);
    }
  });

  log(query, "BuildHashMapDate", latency);

  Accumulator acc;
  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        Accumulator(512),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto &hm_part_pt = hm_part[db.lo.partkey[i] % n_pt];
            auto part_it = hm_part_pt.find(db.lo.partkey[i]);
            if (part_it != hm_part_pt.end() &&
                hs_supplier.find(db.lo.suppkey[i]) != hs_supplier.end()) {
              auto date_it = hm_date.find(db.lo.orderdate[i]);
              if (date_it != hm_date.end()) {
                std::pair<bool, int64_t> &slot =
                    acc[((date_it->second - 1992) << 6) |
                        ((part_it->second - 40) & 0b111111)];
                slot.first = true;
                slot.second += db.lo.revenue[i];
              }
            }
          }
          return acc;
        },
        agg_merge);
  });

  log(query, "Probe", latency);

  latency = time([&] { q2_finalize(acc, result); });

  log(query, "Finalize", latency);

  print(result);

  std::vector<std::tuple<uint32_t, uint16_t, uint16_t>> agg_input;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    auto &hm_part_pt = hm_part[db.lo.partkey[i] % n_pt];
    auto part_it = hm_part_pt.find(db.lo.partkey[i]);
    if (part_it != hm_part_pt.end() &&
        hs_supplier.find(db.lo.suppkey[i]) != hs_supplier.end()) {
      auto date_it = hm_date.find(db.lo.orderdate[i]);
      if (date_it != hm_date.end()) {
        agg_input.emplace_back(
            db.lo.revenue[i], date_it->second, part_it->second);
      }
    }
  }

  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, agg_input.size()),
        Accumulator(512),
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

  q2_finalize(acc, result);

  print(result);
}

void q2p1(const Database &db) {
  auto c1 = [&](size_t i) { return db.s.region[i] == 2; };
  auto c2 = [&](size_t i, size_t j) { return db.p_pt[i].category[j] == 2; };
  q2("Q2.1", db, c1, c2);
}

void q2p2(const Database &db) {
  auto c1 = [&](size_t i) { return db.s.region[i] == 3; };
  auto c2 = [&](size_t i, size_t j) {
    return db.p_pt[i].brand1[j] >= 254 && db.p_pt[i].brand1[j] <= 261;
  };
  q2("Q2.2", db, c1, c2);
}

void q2p3(const Database &db) {
  auto c1 = [&](size_t i) { return db.s.region[i] == 4; };
  auto c2 = [&](size_t i, size_t j) { return db.p_pt[i].brand1[j] == 254; };
  q2("Q2.3", db, c1, c2);
}

// -----------------------------------------------------------------------------
// QUERY FLIGHT 3
// -----------------------------------------------------------------------------

struct Q3P1Row {
  Q3P1Row(uint8_t c_nation,
          uint8_t s_nation,
          uint16_t d_year,
          uint16_t sum_lo_revenue)
      : c_nation(c_nation), s_nation(s_nation), d_year(d_year),
        sum_lo_revenue(sum_lo_revenue) {}

  friend bool operator==(const Q3P1Row &a, const Q3P1Row &b) {
    return a.c_nation == b.c_nation && a.s_nation == b.s_nation &&
           a.d_year == b.d_year && a.sum_lo_revenue == b.sum_lo_revenue;
  }

  friend std::ostream &operator<<(std::ostream &os, const Q3P1Row &row) {
    os << (int)row.c_nation << '|' << (int)row.s_nation << '|' << row.d_year
       << '|' << row.sum_lo_revenue;
    return os;
  }

  uint8_t c_nation;
  uint8_t s_nation;
  uint16_t d_year;
  uint16_t sum_lo_revenue;
};

struct Q3P234Row {
  Q3P234Row(uint8_t c_city,
            uint8_t s_city,
            uint16_t d_year,
            uint16_t sum_lo_revenue)
      : c_city(c_city), s_city(s_city), d_year(d_year),
        sum_lo_revenue(sum_lo_revenue) {}

  friend bool operator==(const Q3P234Row &a, const Q3P234Row &b) {
    return a.c_city == b.c_city && a.s_city == b.s_city &&
           a.d_year == b.d_year && a.sum_lo_revenue == b.sum_lo_revenue;
  }

  friend std::ostream &operator<<(std::ostream &os, const Q3P234Row &row) {
    os << (int)row.c_city << '|' << (int)row.s_city << '|' << row.d_year << '|'
       << row.sum_lo_revenue;
    return os;
  }

  uint8_t c_city;
  uint8_t s_city;
  uint16_t d_year;
  uint16_t sum_lo_revenue;
};

void q3p1_finalize(Accumulator &acc, std::vector<Q3P1Row> &result) {
  result.clear();

  for (size_t i = 0; i < acc.size(); ++i) {
    if (acc[i].first) {
      uint8_t c_nation = i >> 8;
      uint8_t s_nation = (i >> 3) & 0b11111;
      uint16_t d_year = (i & 0b111) + 1992;
      result.emplace_back(c_nation, s_nation, d_year, acc[i].second);
    }
  }

  std::sort(
      result.begin(), result.end(), [](const Q3P1Row &a, const Q3P1Row &b) {
        return a.d_year < b.d_year ||
               (a.d_year == b.d_year && a.sum_lo_revenue > b.sum_lo_revenue);
      });
}

void q3p1(const Database &db) {
  double latency;
  std::vector<Q3P1Row> result;

  std::vector<hash_map<uint32_t, uint8_t>> hm_customer(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Customer &c_pt = db.c_pt[i];
      for (size_t j = 0; j < c_pt.custkey.size(); ++j) {
        if (c_pt.region[j] == 3) {
          hm_customer[i].emplace(c_pt.custkey[j], c_pt.nation[j]);
        }
      }
    });
  });

  log("Q3.1", "BuildHashMapCustomer", latency);

  hash_map<uint32_t, uint8_t> hm_supplier;
  latency = time([&] {
    for (size_t i = 0; i < db.s.suppkey.size(); ++i) {
      if (db.s.region[i] == 3) {
        hm_supplier.emplace(db.s.suppkey[i], db.s.nation[i]);
      }
    }
  });

  log("Q3.1", "BuildHashMapSupplier", latency);

  hash_map<uint32_t, uint16_t> hm_date;
  latency = time([&] {
    for (size_t i = 0; i < db.d.datekey.size(); ++i) {
      if (db.d.year[i] >= 1992 && db.d.year[i] <= 1997) {
        hm_date.emplace(db.d.datekey[i], db.d.year[i]);
      }
    }
  });

  log("Q3.1", "BuildHashMapDate", latency);

  Accumulator acc;
  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        Accumulator(8192),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
            if (supp_it != hm_supplier.end()) {
              auto &hm_cust_pt = hm_customer[db.lo.custkey[i] % n_pt];
              auto cust_it = hm_cust_pt.find(db.lo.custkey[i]);
              if (cust_it != hm_cust_pt.end()) {
                auto date_it = hm_date.find(db.lo.orderdate[i]);
                if (date_it != hm_date.end()) {
                  std::pair<bool, int64_t> &slot =
                      acc[(cust_it->second << 8) | (supp_it->second << 3) |
                          (date_it->second - 1992)];
                  slot.first = true;
                  slot.second += db.lo.revenue[i];
                }
              }
            }
          }
          return acc;
        },
        agg_merge);
  });

  log("Q3.1", "Probe", latency);

  latency = time([&] { q3p1_finalize(acc, result); });

  log("Q3.1", "Finalize", latency);

  print(result);

  std::vector<std::tuple<uint32_t, uint8_t, uint8_t, uint16_t>> agg_input;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
    if (supp_it != hm_supplier.end()) {
      auto &hm_cust_pt = hm_customer[db.lo.custkey[i] % n_pt];
      auto cust_it = hm_cust_pt.find(db.lo.custkey[i]);
      if (cust_it != hm_cust_pt.end()) {
        auto date_it = hm_date.find(db.lo.orderdate[i]);
        if (date_it != hm_date.end()) {
          agg_input.emplace_back(db.lo.revenue[i],
                                 cust_it->second,
                                 supp_it->second,
                                 date_it->second);
        }
      }
    }
  }

  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, agg_input.size()),
        Accumulator(8192),
        [&agg_input](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto &[lo_revenue, c_nation, s_nation, d_year] = agg_input[i];
            std::pair<bool, int64_t> &slot =
                acc[(c_nation << 8) | (s_nation << 3) | (d_year - 1992)];
            slot.first = true;
            slot.second += lo_revenue;
          }
          return acc;
        },
        agg_merge);
  });

  log("Q3.1", "Agg", latency);

  q3p1_finalize(acc, result);

  print(result);
}

void q3p234_finalize(Accumulator &acc, std::vector<Q3P234Row> &result) {
  result.clear();

  for (size_t i = 0; i < acc.size(); ++i) {
    if (acc[i].first) {
      uint8_t c_city = (i >> 8);
      uint8_t s_city = (i >> 3) & 0b1111;
      uint16_t d_year = (i & 0b111) + 1992;
      result.emplace_back(c_city, s_city, d_year, acc[i].second);
    }
  }

  std::sort(
      result.begin(), result.end(), [](const Q3P234Row &a, const Q3P234Row &b) {
        return a.d_year < b.d_year ||
               (a.d_year == b.d_year && a.sum_lo_revenue > b.sum_lo_revenue);
      });
}

template <typename C1, typename C2, typename C3>
void q3p234(
    const std::string &query, const Database &db, C1 &&c1, C2 &&c2, C3 &&c3) {
  double latency;
  std::vector<Q3P234Row> result;

  std::vector<hash_map<uint32_t, uint8_t>> hm_customer(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Customer &c_pt = db.c_pt[i];
      for (size_t j = 0; j < c_pt.custkey.size(); ++j) {
        if (c1(i, j)) {
          hm_customer[i].emplace(c_pt.custkey[j], c_pt.city[j]);
        }
      }
    });
  });

  log(query, "BuildHashMapCustomer", latency);

  hash_map<uint32_t, uint8_t> hm_supplier;
  latency = time([&] {
    for (size_t i = 0; i < db.s.suppkey.size(); ++i) {
      if (c2(i)) {
        hm_supplier.emplace(db.s.suppkey[i], db.s.city[i]);
      }
    }
  });

  log(query, "BuildHashMapSupplier", latency);

  hash_map<uint32_t, uint16_t> hm_date;
  latency = time([&] {
    for (size_t i = 0; i < db.d.datekey.size(); ++i) {
      if (c3(i)) {
        hm_date.emplace(db.d.datekey[i], db.d.year[i]);
      }
    }
  });

  log(query, "BuildHashMapDate", latency);

  Accumulator acc;
  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        Accumulator(8192),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
            if (supp_it != hm_supplier.end()) {
              auto &hm_cust_pt = hm_customer[db.lo.custkey[i] % n_pt];
              auto cust_it = hm_cust_pt.find(db.lo.custkey[i]);
              if (cust_it != hm_cust_pt.end()) {
                auto date_it = hm_date.find(db.lo.orderdate[i]);
                if (date_it != hm_date.end()) {
                  std::pair<bool, int64_t> &slot =
                      acc[((cust_it->second - 221) << 8) |
                          (supp_it->second << 3) | (date_it->second - 1992)];
                  slot.first = true;
                  slot.second += db.lo.revenue[i];
                }
              }
            }
          }
          return acc;
        },
        agg_merge);
  });

  log(query, "Probe", latency);

  latency = time([&] { q3p234_finalize(acc, result); });

  log(query, "Finalize", latency);

  print(result);

  std::vector<std::tuple<uint32_t, uint8_t, uint8_t, uint16_t>> agg_input;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
    if (supp_it != hm_supplier.end()) {
      auto &hm_cust_pt = hm_customer[db.lo.custkey[i] % n_pt];
      auto cust_it = hm_cust_pt.find(db.lo.custkey[i]);
      if (cust_it != hm_cust_pt.end()) {
        auto date_it = hm_date.find(db.lo.orderdate[i]);
        if (date_it != hm_date.end()) {
          agg_input.emplace_back(db.lo.revenue[i],
                                 cust_it->second,
                                 supp_it->second,
                                 date_it->second);
        }
      }
    }
  }

  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, agg_input.size()),
        Accumulator(8192),
        [&agg_input](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto &[lo_revenue, c_city, s_city, d_year] = agg_input[i];
            std::pair<bool, int64_t> &slot =
                acc[((c_city << 8) - 221) | (s_city << 3) | (d_year - 1992)];
            slot.first = true;
            slot.second += lo_revenue;
          }
          return acc;
        },
        agg_merge);
  });

  log(query, "Agg", latency);

  q3p234_finalize(acc, result);

  print(result);
}

void q3p2(const Database &db) {
  auto c1 = [&](size_t i, size_t j) { return db.c_pt[i].nation[j] == 24; };
  auto c2 = [&](size_t i) { return db.s.nation[i] == 24; };
  auto c3 = [&](size_t i) {
    return db.d.year[i] >= 1992 && db.d.year[i] <= 1997;
  };
  q3p234("Q3.2", db, c1, c2, c3);
}

void q3p3(const Database &db) {
  auto c1 = [&](size_t i, size_t j) {
    return db.c_pt[i].city[j] == 222 || db.c_pt[i].city[j] == 226;
  };
  auto c2 = [&](size_t i) {
    return db.s.city[i] == 222 || db.s.city[i] == 226;
  };
  auto c3 = [&](size_t i) {
    return db.d.year[i] >= 1992 && db.d.year[i] <= 1997;
  };
  q3p234("Q3.3", db, c1, c2, c3);
}

void q3p4(const Database &db) {
  auto c1 = [&](size_t i, size_t j) {
    return db.c_pt[i].city[j] == 222 || db.c_pt[i].city[j] == 226;
  };
  auto c2 = [&](size_t i) {
    return db.s.city[i] == 222 || db.s.city[i] == 226;
  };
  auto c3 = [&](size_t i) { return db.d.yearmonth[i] == 20; };
  q3p234("Q3.4", db, c1, c2, c3);
}

// -----------------------------------------------------------------------------
// QUERY FLIGHT 4
// -----------------------------------------------------------------------------

struct Q4P1Row {
  Q4P1Row(uint16_t d_year, uint8_t c_nation, int64_t sum_profit)
      : d_year(d_year), c_nation(c_nation), sum_profit(sum_profit) {}

  friend bool operator==(const Q4P1Row &a, const Q4P1Row &b) {
    return a.d_year == b.d_year && a.c_nation == b.c_nation &&
           a.sum_profit == b.sum_profit;
  }

  friend std::ostream &operator<<(std::ostream &os, const Q4P1Row &row) {
    os << row.d_year << '|' << (int)row.c_nation << '|' << row.sum_profit;
    return os;
  }

  uint16_t d_year;
  uint8_t c_nation;
  int64_t sum_profit;
};

struct Q4P2Row {
  Q4P2Row(uint16_t d_year,
          uint8_t s_nation,
          uint8_t p_category,
          int64_t sum_profit)
      : d_year(d_year), s_nation(s_nation), p_category(p_category),
        sum_profit(sum_profit) {}

  friend bool operator==(const Q4P2Row &a, const Q4P2Row &b) {
    return a.d_year == b.d_year && a.s_nation == b.s_nation &&
           a.p_category == b.p_category && a.sum_profit == b.sum_profit;
  }

  friend std::ostream &operator<<(std::ostream &os, const Q4P2Row &row) {
    os << row.d_year << '|' << (int)row.s_nation << '|' << (int)row.p_category
       << '|' << row.sum_profit;
    return os;
  }

  uint16_t d_year;
  uint8_t s_nation;
  uint8_t p_category;
  int64_t sum_profit;
};

struct Q4P3Row {
  Q4P3Row(uint16_t d_year,
          uint8_t s_city,
          uint16_t p_brand1,
          int64_t sum_profit)
      : d_year(d_year), s_city(s_city), p_brand1(p_brand1),
        sum_profit(sum_profit) {}

  friend bool operator==(const Q4P3Row &a, const Q4P3Row &b) {
    return a.d_year == b.d_year && a.s_city == b.s_city &&
           a.p_brand1 == b.p_brand1 && a.sum_profit == b.sum_profit;
  }

  friend std::ostream &operator<<(std::ostream &os, const Q4P3Row &row) {
    os << row.d_year << '|' << (int)row.s_city << '|' << row.p_brand1 << '|'
       << row.sum_profit;
    return os;
  }

  uint16_t d_year;
  uint8_t s_city;
  uint16_t p_brand1;
  int64_t sum_profit;
};

void q4p1_finalize(Accumulator &acc, std::vector<Q4P1Row> &result) {
  result.clear();
  result.reserve(acc.size());
  for (size_t i = 0; i < acc.size(); ++i) {
    if (acc[i].first) {
      uint16_t d_year = (i >> 5) + 1992;
      uint8_t c_nation = i & 0b11111;
      result.emplace_back(d_year, c_nation, acc[i].second);
    }
  }
  std::sort(
      result.begin(), result.end(), [](const Q4P1Row &a, const Q4P1Row &b) {
        return a.d_year < b.d_year ||
               (a.d_year == b.d_year && a.c_nation < b.c_nation);
      });
}

void q4p1(const Database &db) {
  double latency;
  std::vector<Q4P1Row> result;

  hash_map<uint32_t, uint16_t> hm_date;
  latency = time([&] {
    for (size_t i = 0; i < db.d.datekey.size(); ++i) {
      hm_date.emplace(db.d.datekey[i], db.d.year[i]);
    }
  });

  log("Q4.1", "BuildHashMapDate", latency);

  std::vector<hash_map<uint32_t, uint8_t>> hm_customer(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Customer &c_pt = db.c_pt[i];
      for (size_t j = 0; j < c_pt.custkey.size(); ++j) {
        if (c_pt.region[j] == 2) {
          hm_customer[i].emplace(c_pt.custkey[j], c_pt.nation[j]);
        }
      }
    });
  });

  log("Q4.1", "BuildHashMapCustomer", latency);

  hash_set<uint32_t> hs_supplier;
  latency = time([&] {
    for (size_t i = 0; i < db.s.suppkey.size(); ++i) {
      if (db.s.region[i] == 2) {
        hs_supplier.emplace(db.s.suppkey[i]);
      }
    }
  });

  log("Q4.1", "BuildHashSetSupplier", latency);

  std::vector<hash_set<uint32_t>> hs_part(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Part &p_pt = db.p_pt[i];
      hs_part[i].reserve(p_pt.partkey.size());
      for (size_t j = 0; j < p_pt.partkey.size(); ++j) {
        if (p_pt.mfgr[j] == 1 || p_pt.mfgr[j] == 2) {
          hs_part[i].emplace(p_pt.partkey[j]);
        }
      }
    });
  });

  log("Q4.1", "BuildHashSetPart", latency);

  Accumulator acc;
  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        Accumulator(256),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            if (hs_supplier.find(db.lo.suppkey[i]) != hs_supplier.end()) {
              auto &hs_part_pt = hs_part[db.lo.partkey[i] % n_pt];
              if (hs_part_pt.find(db.lo.partkey[i]) != hs_part_pt.end()) {
                auto &hm_cust_pt = hm_customer[db.lo.custkey[i] % n_pt];
                auto cust_it = hm_cust_pt.find(db.lo.custkey[i]);
                if (cust_it != hm_cust_pt.end()) {
                  uint16_t d_year = hm_date.find(db.lo.orderdate[i])->second;
                  std::pair<bool, int64_t> &slot =
                      acc[((d_year - 1992) << 5) | cust_it->second];
                  slot.first = true;
                  slot.second += db.lo.revenue[i] - db.lo.supplycost[i];
                }
              }
            }
          }
          return acc;
        },
        agg_merge);
  });

  log("Q4.1", "Probe", latency);

  latency = time([&] { q4p1_finalize(acc, result); });

  log("Q4.1", "Finalize", latency);

  print(result);

  std::vector<std::tuple<uint32_t, uint32_t, uint16_t, uint8_t>> agg_input;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    if (hs_supplier.find(db.lo.suppkey[i]) != hs_supplier.end()) {
      auto &hs_part_pt = hs_part[db.lo.partkey[i] % n_pt];
      if (hs_part_pt.find(db.lo.partkey[i]) != hs_part_pt.end()) {
        auto &hm_cust_pt = hm_customer[db.lo.custkey[i] % n_pt];
        auto cust_it = hm_cust_pt.find(db.lo.custkey[i]);
        if (cust_it != hm_cust_pt.end()) {
          uint16_t d_year = hm_date.find(db.lo.orderdate[i])->second;
          agg_input.emplace_back(
              db.lo.revenue[i], db.lo.supplycost[i], d_year, cust_it->second);
        }
      }
    }
  }

  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, agg_input.size()),
        Accumulator(256),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto &[lo_revenue, lo_supplycost, d_year, c_nation] = agg_input[i];
            std::pair<bool, int64_t> &slot =
                acc[((d_year - 1992) << 5) | c_nation];
            slot.first = true;
            slot.second += lo_revenue - lo_supplycost;
          }
          return acc;
        },
        agg_merge);
  });

  log("Q4.1", "Agg", latency);

  q4p1_finalize(acc, result);

  print(result);
}

void q4p2_finalize(Accumulator &acc, std::vector<Q4P2Row> &result) {
  result.clear();
  result.reserve(acc.size());
  for (size_t i = 0; i < acc.size(); ++i) {
    if (acc[i].first) {
      uint16_t d_year = (i >> 9) + 1997;
      uint8_t s_nation = (i >> 4) & 0b11111;
      uint8_t p_category = i & 0b1111;
      result.emplace_back(d_year, s_nation, p_category, acc[i].second);
    }
  }
  std::sort(
      result.begin(), result.end(), [](const Q4P2Row &a, const Q4P2Row &b) {
        return a.d_year < b.d_year ||
               (a.d_year == b.d_year && a.s_nation < b.s_nation) ||
               (a.d_year == b.d_year && a.s_nation == b.s_nation &&
                a.p_category < b.p_category);
      });
}

void q4p2(const Database &db) {
  double latency;
  std::vector<Q4P2Row> result;

  hash_map<uint32_t, uint16_t> hm_date;
  latency = time([&] {
    for (size_t i = 0; i < db.d.datekey.size(); ++i) {
      if (db.d.year[i] == 1997 || db.d.year[i] == 1998) {
        hm_date.emplace(db.d.datekey[i], db.d.year[i]);
      }
    }
  });

  log("Q4.2", "BuildHashMapDate", latency);

  std::vector<hash_set<uint32_t>> hs_customer(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Customer &c_pt = db.c_pt[i];
      for (size_t j = 0; j < c_pt.custkey.size(); ++j) {
        if (c_pt.region[j] == 2) {
          hs_customer[i].emplace(c_pt.custkey[j]);
        }
      }
    });
  });

  log("Q4.2", "BuildHashSetCustomer", latency);

  hash_map<uint32_t, uint8_t> hm_supplier;
  latency = time([&] {
    for (size_t i = 0; i < db.s.suppkey.size(); ++i) {
      if (db.s.region[i] == 2) {
        hm_supplier.emplace(db.s.suppkey[i], db.s.nation[i]);
      }
    }
  });

  log("Q4.2", "BuildHashMapSupplier", latency);

  std::vector<hash_map<uint32_t, uint8_t>> hm_part(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Part &p_pt = db.p_pt[i];
      hm_part[i].reserve(p_pt.partkey.size());
      for (size_t j = 0; j < p_pt.partkey.size(); ++j) {
        if (p_pt.mfgr[j] == 1 || p_pt.mfgr[j] == 2) {
          hm_part[i].emplace(p_pt.partkey[j], p_pt.category[j]);
        }
      }
    });
  });

  log("Q4.2", "BuildHashMapPart", latency);

  Accumulator acc;
  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        Accumulator(1024),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
            if (supp_it != hm_supplier.end()) {
              auto date_it = hm_date.find(db.lo.orderdate[i]);
              if (date_it != hm_date.end()) {
                auto &hs_cust_pt = hs_customer[db.lo.custkey[i] % n_pt];
                if (hs_cust_pt.find(db.lo.custkey[i]) != hs_cust_pt.end()) {
                  auto &hm_part_pt = hm_part[db.lo.partkey[i] % n_pt];
                  auto part_it = hm_part_pt.find(db.lo.partkey[i]);
                  if (part_it != hm_part_pt.end()) {
                    std::pair<bool, int64_t> &slot =
                        acc[((date_it->second - 1997) << 9) |
                            (supp_it->second << 4) | part_it->second];
                    slot.first = true;
                    slot.second += db.lo.revenue[i] - db.lo.supplycost[i];
                  }
                }
              }
            }
          }
          return acc;
        },
        agg_merge);
  });

  log("Q4.2", "Probe", latency);

  latency = time([&] { q4p2_finalize(acc, result); });

  log("Q4.2", "Finalize", latency);

  print(result);

  std::vector<std::tuple<uint32_t, uint32_t, uint16_t, uint8_t, uint8_t>>
      agg_input;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
    if (supp_it != hm_supplier.end()) {
      auto date_it = hm_date.find(db.lo.orderdate[i]);
      if (date_it != hm_date.end()) {
        auto &hs_cust_pt = hs_customer[db.lo.custkey[i] % n_pt];
        if (hs_cust_pt.find(db.lo.custkey[i]) != hs_cust_pt.end()) {
          auto &hm_part_pt = hm_part[db.lo.partkey[i] % n_pt];
          auto part_it = hm_part_pt.find(db.lo.partkey[i]);
          if (part_it != hm_part_pt.end()) {
            agg_input.emplace_back(db.lo.revenue[i],
                                   db.lo.supplycost[i],
                                   date_it->second,
                                   supp_it->second,
                                   part_it->second);
          }
        }
      }
    }
  }

  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, agg_input.size()),
        Accumulator(1024),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto &[lo_revenue, lo_supplycost, d_year, s_nation, p_category] =
                agg_input[i];
            std::pair<bool, int64_t> &slot =
                acc[((d_year - 1997) << 9) | (s_nation << 4) | p_category];
            slot.first = true;
            slot.second += lo_revenue - lo_supplycost;
          }
          return acc;
        },
        agg_merge);
  });

  log("Q4.2", "Agg", latency);

  q4p2_finalize(acc, result);

  print(result);
}

void q4p3_finalize(Accumulator &acc, std::vector<Q4P3Row> &result) {
  result.clear();
  result.reserve(acc.size());
  for (size_t i = 0; i < acc.size(); ++i) {
    if (acc[i].first) {
      uint16_t d_year = (i >> 10) + 1997;
      uint8_t s_city = ((i >> 6) & 0b11111) + 231;
      uint16_t p_brand1 = (i & 0b1111) + 121;
      result.emplace_back(d_year, s_city, p_brand1, acc[i].second);
    }
  }
  std::sort(
      result.begin(), result.end(), [](const Q4P3Row &a, const Q4P3Row &b) {
        return a.d_year < b.d_year ||
               (a.d_year == b.d_year && a.s_city < b.s_city) ||
               (a.d_year == b.d_year && a.s_city == b.s_city &&
                a.p_brand1 < b.p_brand1);
      });
}

void q4p3(const Database &db) {
  double latency;
  std::vector<Q4P3Row> result;

  hash_map<uint32_t, uint16_t> hm_date;
  latency = time([&] {
    for (size_t i = 0; i < db.d.datekey.size(); ++i) {
      if (db.d.year[i] == 1997 || db.d.year[i] == 1998) {
        hm_date.emplace(db.d.datekey[i], db.d.year[i]);
      }
    }
  });

  log("Q4.3", "BuildHashMapDate", latency);

  std::vector<hash_set<uint32_t>> hs_customer(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Customer &c_pt = db.c_pt[i];
      for (size_t j = 0; j < c_pt.custkey.size(); ++j) {
        if (c_pt.region[j] == 2) {
          hs_customer[i].emplace(c_pt.custkey[j]);
        }
      }
    });
  });

  log("Q4.3", "BuildHashSetCustomer", latency);

  hash_map<uint32_t, uint8_t> hm_supplier;
  latency = time([&] {
    for (size_t i = 0; i < db.s.suppkey.size(); ++i) {
      if (db.s.nation[i] == 24) {
        hm_supplier.emplace(db.s.suppkey[i], db.s.city[i]);
      }
    }
  });

  log("Q4.3", "BuildHashMapSupplier", latency);

  std::vector<hash_map<uint32_t, uint16_t>> hm_part(n_pt);
  latency = time([&] {
    tbb::parallel_for(size_t(0), n_pt, [&](size_t i) {
      const Part &p_pt = db.p_pt[i];
      hm_part[i].reserve(p_pt.partkey.size());
      for (size_t j = 0; j < p_pt.partkey.size(); ++j) {
        if (p_pt.category[j] == 4) {
          hm_part[i].emplace(p_pt.partkey[j], p_pt.brand1[j]);
        }
      }
    });
  });

  log("Q4.3", "BuildHashMapPart", latency);

  Accumulator acc;
  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        Accumulator(2048),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
            if (supp_it != hm_supplier.end()) {
              auto date_it = hm_date.find(db.lo.orderdate[i]);
              if (date_it != hm_date.end()) {
                auto &hs_cust_pt = hs_customer[db.lo.custkey[i] % n_pt];
                if (hs_cust_pt.find(db.lo.custkey[i]) != hs_cust_pt.end()) {
                  auto &hm_part_pt = hm_part[db.lo.partkey[i] % n_pt];
                  auto part_it = hm_part_pt.find(db.lo.partkey[i]);
                  if (part_it != hm_part_pt.end()) {
                    std::pair<bool, int64_t> &slot =
                        acc[((date_it->second - 1997) << 10) |
                            ((supp_it->second - 231) << 6) |
                            (part_it->second - 121)];
                    slot.first = true;
                    slot.second += db.lo.revenue[i] - db.lo.supplycost[i];
                  }
                }
              }
            }
          }
          return acc;
        },
        agg_merge);
  });

  log("Q4.3", "Probe", latency);

  latency = time([&] { q4p3_finalize(acc, result); });

  log("Q4.3", "Finalize", latency);

  print(result);

  std::vector<std::tuple<uint32_t, uint32_t, uint16_t, uint8_t, uint16_t>>
      agg_input;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    auto supp_it = hm_supplier.find(db.lo.suppkey[i]);
    if (supp_it != hm_supplier.end()) {
      auto date_it = hm_date.find(db.lo.orderdate[i]);
      if (date_it != hm_date.end()) {
        auto &hs_cust_pt = hs_customer[db.lo.custkey[i] % n_pt];
        if (hs_cust_pt.find(db.lo.custkey[i]) != hs_cust_pt.end()) {
          auto &hm_part_pt = hm_part[db.lo.partkey[i] % n_pt];
          auto part_it = hm_part_pt.find(db.lo.partkey[i]);
          if (part_it != hm_part_pt.end()) {
            agg_input.emplace_back(db.lo.revenue[i],
                                   db.lo.supplycost[i],
                                   date_it->second,
                                   supp_it->second,
                                   part_it->second);
          }
        }
      }
    }
  }

  latency = time([&] {
    acc = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, agg_input.size()),
        Accumulator(2048),
        [&](const tbb::blocked_range<size_t> &r, Accumulator acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            auto &[lo_revenue, lo_supplycost, d_year, s_city, p_brand1] =
                agg_input[i];
            std::pair<bool, int64_t> &slot =
                acc[((d_year - 1997) << 10) | ((s_city - 231) << 6) |
                    (p_brand1 - 121)];
            slot.first = true;
            slot.second += lo_revenue - lo_supplycost;
          }
          return acc;
        },
        agg_merge);
  });

  log("Q4.3", "Agg", latency);

  q4p3_finalize(acc, result);

  print(result);
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "USAGE: " << std::endl;
    std::cerr << argv[0] << " DATA_DIR" << std::endl;
    return 1;
  }

  char *data_dir = argv[1];

  Database db;

  read_table(data_dir,
             "part_encoded",
             Column(0, db.p.partkey),
             Column(2, db.p.mfgr),
             Column(3, db.p.category),
             Column(4, db.p.brand1));

  read_table(data_dir,
             "supplier_encoded",
             Column(0, db.s.suppkey),
             Column(3, db.s.city),
             Column(4, db.s.nation),
             Column(5, db.s.region));

  read_table(data_dir,
             "customer_encoded",
             Column(0, db.c.custkey),
             Column(3, db.c.city),
             Column(4, db.c.nation),
             Column(5, db.c.region));

  read_table(data_dir,
             "date_encoded",
             Column(0, db.d.datekey),
             Column(4, db.d.year),
             Column(5, db.d.yearmonthnum),
             Column(6, db.d.yearmonth),
             Column(11, db.d.weeknuminyear));

  read_table(data_dir,
             "lineorder",
             Column(2, db.lo.custkey),
             Column(3, db.lo.partkey),
             Column(4, db.lo.suppkey),
             Column(5, db.lo.orderdate),
             Column(8, db.lo.quantity),
             Column(9, db.lo.extendedprice),
             Column(11, db.lo.discount),
             Column(12, db.lo.revenue),
             Column(13, db.lo.supplycost));

  db.p_pt = std::vector<Part>(n_pt);
  for (size_t i = 0; i < db.p.partkey.size(); ++i) {
    Part &pt = db.p_pt[db.p.partkey[i] % n_pt];
    pt.partkey.push_back(db.p.partkey[i]);
    pt.mfgr.push_back(db.p.mfgr[i]);
    pt.category.push_back(db.p.category[i]);
    pt.brand1.push_back(db.p.brand1[i]);
  }

  db.c_pt = std::vector<Customer>(n_pt);
  for (size_t i = 0; i < db.c.custkey.size(); ++i) {
    Customer &pt = db.c_pt[db.c.custkey[i] % n_pt];
    pt.custkey.push_back(db.c.custkey[i]);
    pt.city.push_back(db.c.city[i]);
    pt.nation.push_back(db.c.nation[i]);
    pt.region.push_back(db.c.region[i]);
  }

  q1p1(db);
  q1p2(db);
  q1p3(db);
  q2p1(db);
  q2p2(db);
  q2p3(db);
  q3p1(db);
  q3p2(db);
  q3p3(db);
  q3p4(db);
  q4p1(db);
  q4p2(db);
  q4p3(db);

  return 0;
}
