#include "common.hpp"

#include <sqlite3.h>

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

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
