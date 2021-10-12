// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/spanner/client.h"
#include <iostream>
#include <stdexcept>

std::string project_id = "just-sunrise-326603";
std::string instance_id = "test-instance";
std::string db_id = "new-db";
namespace spanner = ::google::cloud::spanner;


void ScanFullTable(spanner::Client client, std::string table) {
    spanner::SqlStatement select("SELECT * FROM " + table);

    auto rows = client.ExecuteQuery(std::move(select));
    for (auto const& row : rows) {
        if (!row) throw std::runtime_error(row.status().message());
        
        if (auto singer_id = row->get<std::int64_t>("SingerId")) {
            std::cout << "SingerId: " << *singer_id << "\t";
        }
        
        if (auto album_id = row->get<std::int64_t>("AlbumId")) {
            std::cout << "AlbumId: " << *album_id << "\t";
        } 
        
        if (auto album_title = row->get<std::string>("AlbumTitle")) {
            std::cout << "AlbumTitle: " << *album_title << "\t";
        } 
        
        if (auto marketing_budget = row->get<std::int64_t>("MarketingBudget")) {
            std::cout << "MarketingBudget: " << *marketing_budget << "\t";
        } else {
            std::cout << "MarketingBudget: NULL";
        }
        std::cout << "\n";
    }
}

std::string GenerateReadQuery(std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value, std::vector<std::string> *fields) {
    std::string query = "SELECT ";
    if (fields && !fields->empty()) {
        query += (*fields)[0];
        for (int i = 1; i < fields->size(); i++) {
        query += ", " + (*fields)[i];
        }
    } else {
        query += "*";
    }
    query += " FROM " + table + " WHERE ";
    bool first = true;
    for (int index = 0; index < key_name.size(); ++index) {
      if (!first) {
        query += " AND ";
      } else {
        first = false;
      }
      query += key_name[index] + "=" + key_value[index];
    }
    return query;
}

std::string GenerateScanQuery(std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value, std::vector<std::string> *fields, int len) {
    std::string query = "SELECT ";
    if (fields && !fields->empty()) {
        query += (*fields)[0];
        for (int i = 1; i < fields->size(); i++) {
        query += ", " + (*fields)[i];
        }
    } else {
        query += "*";
    }
    query += " FROM " + table + " WHERE ";
    bool first = true;
    for (int index = 0; index < key_name.size(); ++index) {
      if (!first) {
        query += " AND ";
      } else {
        first = false;
      }
      query += key_name[index] + "=" + key_value[index];
    }
    query += " LIMIT " + std::to_string(len);
    return query;
}

std::string GenerateUpdateQuery(std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value, std::vector<std::string> repl_name, std::vector<std::string> repl_value) {
    std::string query = "UPDATE " + table + " SET ";
    query += repl_name[0] + "=" + repl_value[0];
    for (size_t i = 1; i < repl_value.size(); ++i) {
        query += ", " + repl_name[i] + "=" + repl_value[i];
    }
    query += " WHERE ";
    bool first = true;
    for (int index = 0; index < key_name.size(); ++index) {
      if (!first) {
        query += " AND ";
      } else {
        first = false;
      }
      query += key_name[index] + "=" + key_value[index];
    }
    return query;
}

std::string GenerateInsertQuery(std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value) {
    std::string query = "INSERT INTO " + table + "(";
    bool first = true;
    for (int index = 0; index < key_name.size(); ++index) {
        if (!first) {
            query += ", " + key_name[index];
        } else {
            first = false;
            query += key_name[index];
        }
    }
    first = true;
    query += ") VALUES (";
    for (int index = 0; index < key_value.size(); ++index) {
        if (!first) {
            query += ", " + key_value[index];
        } else {
            first = false;
            query += key_value[index];
        }
    }
    query += ")";
    return query;
}

std::string GenerateDeleteQuery(std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value) {
    std::string query = "DELETE FROM " + table + " WHERE ";
    bool first = true;
    for (int index = 0; index < key_name.size(); ++index) {
      if (!first) {
        query += " AND ";
      } else {
        first = false;
      }
      query += key_name[index] + "=" + key_value[index];
    }
    return query;
}

void Read(spanner::Client client, std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value, std::vector<std::string> *fields) {
    std::string query = GenerateReadQuery(table, key_name, key_value, fields);
    spanner::SqlStatement select(query);
    auto rows = client.ExecuteQuery(std::move(select));
    for (auto const& row : rows) {
        if (!row) throw std::runtime_error(row.status().message());
        
        if (auto singer_id = row->get<std::int64_t>("SingerId")) {
            std::cout << "SingerId: " << *singer_id << "\t";
        }
        
        if (auto album_id = row->get<std::int64_t>("AlbumId")) {
            std::cout << "AlbumId: " << *album_id << "\t";
        } 
        
        if (auto album_title = row->get<std::string>("AlbumTitle")) {
            std::cout << "AlbumTitle: " << *album_title << "\t";
        } 
        
        if (auto marketing_budget = row->get<std::int64_t>("MarketingBudget")) {
            std::cout << "MarketingBudget: " << *marketing_budget << "\t";
        } else {
            std::cout << "MarketingBudget: NULL";
        }
        std::cout << "\n";
    }
}

void Scan(spanner::Client client, std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value, int len, std::vector<std::string> *fields) {
    std::string query = GenerateScanQuery(table, key_name, key_value, fields, len);
    spanner::SqlStatement select(query);
    auto rows = client.ExecuteQuery(std::move(select));
    for (auto const& row : rows) {
        if (!row) throw std::runtime_error(row.status().message());
        
        if (auto singer_id = row->get<std::int64_t>("SingerId")) {
            std::cout << "SingerId: " << *singer_id << "\t";
        }
        
        if (auto album_id = row->get<std::int64_t>("AlbumId")) {
            std::cout << "AlbumId: " << *album_id << "\t";
        } 
        
        if (auto album_title = row->get<std::string>("AlbumTitle")) {
            std::cout << "AlbumTitle: " << *album_title << "\t";
        } 
        
        if (auto marketing_budget = row->get<std::int64_t>("MarketingBudget")) {
            std::cout << "MarketingBudget: " << *marketing_budget << "\t";
        } else {
            std::cout << "MarketingBudget: NULL";
        }
        std::cout << "\n";
    }
}

void Update(spanner::Client client, std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value, std::vector<std::string> repl_name, std::vector<std::string> repl_value) {
    using ::google::cloud::StatusOr;
    auto commit_result = client.Commit(
      [&client, query = GenerateUpdateQuery(table, key_name, key_value, repl_name, repl_value)](spanner::Transaction txn) -> StatusOr<spanner::Mutations> {
        auto update = client.ExecuteDml(
            std::move(txn),
            spanner::SqlStatement(query));
        if (!update) return update.status();
        return spanner::Mutations{};
      });
  if (!commit_result) {
    throw std::runtime_error(commit_result.status().message());
  }
  std::cout << "Update was successful\n";
}

void Insert(spanner::Client client, std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value) {
    using ::google::cloud::StatusOr;
    auto commit_result = client.Commit(
      [&client, query = GenerateInsertQuery(table, key_name, key_value)](spanner::Transaction txn) -> StatusOr<spanner::Mutations> {
        auto insert = client.ExecuteDml(
            std::move(txn),
            spanner::SqlStatement(query));
        if (!insert) return insert.status();
        return spanner::Mutations{};
      });
  if (!commit_result) {
    throw std::runtime_error(commit_result.status().message());
  }
  std::cout << "Insert was successful\n";
}

void Delete(spanner::Client client, std::string table, std::vector<std::string> key_name, std::vector<std::string> key_value) {
    using ::google::cloud::StatusOr;
    auto commit_result = client.Commit(
      [&client, query = GenerateDeleteQuery(table, key_name, key_value)](spanner::Transaction txn) -> StatusOr<spanner::Mutations> {
        auto insert = client.ExecuteDml(
            std::move(txn),
            spanner::SqlStatement(query));
        if (!insert) return insert.status();
        return spanner::Mutations{};
      });
  if (!commit_result) {
    throw std::runtime_error(commit_result.status().message());
  }
  std::cout << "Delete was successful\n";
}

int main(int argc, char* argv[]) try {
    spanner::Client client(
        spanner::MakeConnection(spanner::Database(project_id, instance_id, db_id)));
    
    ScanFullTable(client, "Albums");

    std::cout << "-----------------------------\n";

    std::vector<std::string> key_names = {"AlbumId"};
    std::vector<std::string> key_vals = {"2"};
    std::vector<std::string> fields = {"SingerId", "AlbumId", "AlbumTitle"};
    Read(client, "Albums", key_names, key_vals, &fields);

    std::cout << "-----------------------------\n";

    Scan(client, "Albums", key_names, key_vals, 1, &fields);

    std::cout << "-----------------------------\n";

    std::vector<std::string> repl_names = {"AlbumTitle"};
    std::vector<std::string> repl_vals = {"'DEI DEI'"};
    Update(client, "Albums", key_names, key_vals, repl_names, repl_vals);

    std::cout << "-----------------------------\n";

    ScanFullTable(client, "Albums");

    std::cout << "-----------------------------\n";

    std::vector<std::string> all_keys = {"SingerId", "AlbumId", "AlbumTitle"};
    std::vector<std::string> all_vals = {"3", "4", "'YELLEI'"};
    Insert(client, "Albums", all_keys, all_vals);

    std::cout << "-----------------------------\n";

    ScanFullTable(client, "Albums");

    std::cout << "-----------------------------\n";

    std::vector<std::string> delete_keys = {"SingerId"};
    std::vector<std::string> delete_vals = {"3"};
    Delete(client, "Albums", delete_keys, delete_vals);

    std::cout << "-----------------------------\n";

    ScanFullTable(client, "Albums");

    std::cout << "-----------------------------\n";

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << "\n";
  return 1;
}