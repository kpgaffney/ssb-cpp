#include "../common.hpp"

#include "oneapi/tbb.h"

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

  log(query, "BuildHashSetDate", latency);

  latency = time([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, db.lo.orderdate.size()),
        uint64_t(0),
        [&](const tbb::blocked_range<size_t> &r, uint64_t acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            if (c2(i) && hs.contains(db.lo.orderdate[i])) {
              acc += db.lo.extendedprice[i] * db.lo.discount[i];
            }
          }
          return acc;
        },
        std::plus<>());
  });

  log(query, "Probe", latency);

  std::cout << result << std::endl;

  std::vector<size_t> idx;
  for (size_t i = 0; i < db.lo.orderdate.size(); ++i) {
    if (c2(i) && hs.contains(db.lo.orderdate[i])) {
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

  std::cout << result << std::endl;
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
