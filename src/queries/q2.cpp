#include "../common.hpp"

#include "oneapi/tbb.h"

#include <iomanip>

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
            if (part_it != hm_part_pt.end()) {
              if (hs_supplier.contains(db.lo.suppkey[i])) {
                auto date_it = hm_date.find(db.lo.orderdate[i]);
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
    if (part_it != hm_part_pt.end() && hs_supplier.contains(db.lo.suppkey[i])) {
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
