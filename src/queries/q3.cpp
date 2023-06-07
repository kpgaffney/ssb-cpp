#include "../common.hpp"

#include "oneapi/tbb.h"

struct Q3P1Row {
  Q3P1Row(uint8_t c_nation,
          uint8_t s_nation,
          uint16_t d_year,
          uint64_t sum_lo_revenue)
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
  uint64_t sum_lo_revenue;
};

struct Q3P234Row {
  Q3P234Row(uint8_t c_city,
            uint8_t s_city,
            uint16_t d_year,
            uint64_t sum_lo_revenue)
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
  uint64_t sum_lo_revenue;
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
      uint8_t c_city = (i >> 8) + 221;
      uint8_t s_city = ((i >> 3) & 0b11111) + 221;
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
                          ((supp_it->second - 221) << 3) |
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
                acc[((c_city - 221) << 8) | ((s_city - 221) << 3) |
                    (d_year - 1992)];
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
