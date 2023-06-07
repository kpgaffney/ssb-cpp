#include "../common.hpp"

#include "oneapi/tbb.h"

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
      uint8_t s_city = ((i >> 6) & 0b1111) + 231;
      uint16_t p_brand1 = (i & 0b111111) + 121;
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
