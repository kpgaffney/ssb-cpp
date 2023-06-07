#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using Accumulator = std::vector<std::pair<bool, int64_t>>;

template <typename K> using hash_set = absl::flat_hash_set<K>;
template <typename K, typename V> using hash_map = absl::flat_hash_map<K, V>;

// Number of hash table partitions.
constexpr size_t n_pt = 256;

inline Accumulator agg_merge(Accumulator a, const Accumulator &b) {
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
  std::cerr << query << ',' << key << ',' << value << std::endl;
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

void q1p1(const Database &db);
void q1p2(const Database &db);
void q1p3(const Database &db);
void q2p1(const Database &db);
void q2p2(const Database &db);
void q2p3(const Database &db);
void q3p1(const Database &db);
void q3p2(const Database &db);
void q3p3(const Database &db);
void q3p4(const Database &db);
void q4p1(const Database &db);
void q4p2(const Database &db);
void q4p3(const Database &db);
