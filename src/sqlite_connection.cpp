#include <SQLiteCpp/SQLiteCpp.h>
#include <cppformat/format.h>
#include <ookoto/ookoto.h>
#include <exception>

namespace ookoto {

class SqliteConnection::Impl {
 public:
  Impl() = default;
  ~Impl() = default;

  bool exists_table(const std::string &table_name) const {
    return _db->tableExists(table_name);
  }

  Status connect(const Config &config) {
    if (config.database.empty()) {
      return Status::invalid_argument();
    }

    _config = config;
    _db.reset(new SQLite::Database(config.database,
                                   SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE));
    return Status::ok();
  }

  Status disconnect() {
    _config = {};
    _db.release();
    return Status::ok();
  }

  Status execute_sql(const std::string &sql) {
    fmt::print("SQL: {}\n", sql);
    _db->exec(sql);
    return Status::ok();
  }

  Status execute_sql_for_each(const std::string &sql,
                              const std::function<void(const RowType &)> &fn) {
    if (fn == nullptr) {
      return Status::status_ailment();
    }

    fmt::print("SQL: {}\n", sql);

    bool loaded = false;
    SQLite::Statement query(*_db, sql);
    while (query.executeStep()) {
      loaded = true;
      RowType row;
      auto count = query.getColumnCount();
      for (int i = 0; i < count; i++) {
        auto column = query.getColumn(i);
        auto t = column.getText();
        std::string value = (t) ? t : "";
        row.emplace(std::make_pair(column.getName(), value));
      }
      fn(row);
    }

    return (loaded) ? Status::ok() : Status::not_found();
  }

  Status drop_table(const std::string &table_name) {
    return execute_sql(fmt::format("DROP TABLE {}", table_name));
  }

  Status create_table(std::shared_ptr<Schema> schema) {
    fmt::MemoryWriter buf;

    buf << "CREATE TABLE " << schema->table_name() << " (";

    auto size = schema->defined_column_size();
    schema->each_define([&](const Schema::ColumnType &def) {
      size -= 1;
      buf << std::get<Schema::kColumnName>(def) << " "
          << column_type_to_string(std::get<Schema::kColumnType>(def));
      auto prop =
          column_prop_to_string(std::get<Schema::kColumnProperties>(def));
      if (!prop.empty()) {
        buf << " " << prop;
      }
      if (0 < size) {
        buf << ", ";
      }
    });

    buf << ")";

    return execute_sql(buf.str());
  }

  Status transaction(const std::function<Status()> &t) {
    if (t == nullptr) return Status::invalid_argument();

    SQLite::Transaction transaction(*_db);
    auto status = t();
    if (status.is_ok()) transaction.commit();

    return Status::ok();
  }

  int64_t last_row_id() const { return _db->getLastInsertRowid(); }

 private:
  std::unique_ptr<SQLite::Database> _db;
  Config _config;

  std::string column_type_to_string(Schema::Type type) {
    static std::map<Schema::Type, std::string> mapping = {
        {Schema::Type::kInteger, "INTEGER"},
        {Schema::Type::kBoolean, "INTEGER"},
        {Schema::Type::kFloat, "REAL"},
        {Schema::Type::kString, "TEXT"},
        {Schema::Type::kText, "TEXT"},
        {Schema::Type::kDateTime, "NUMERIC"},
        {Schema::Type::kDate, "NUMERIC"},
        {Schema::Type::kTime, "NUMERIC"},
        {Schema::Type::kBinary, "BLOB"},
    };

    return mapping[type];
  }

  std::string column_prop_to_string(Schema::PropertyPtr prop) {
    std::vector<std::string> results;
    if (prop->not_null()) {
      results.emplace_back("NOT NULL");
    }
    if (prop->unique()) {
      results.emplace_back("UNIQUE");
    }
    if (prop->primary_key()) {
      results.emplace_back("PRIMARY KEY");
    }
    if (prop->auto_increment()) {
      results.emplace_back("AUTOINCREMENT");
    }

    fmt::MemoryWriter buf;
    auto size = results.size();
    for (auto &one : results) {
      size -= 1;
      buf << one;
      if (0 < size) {
        buf << " ";
      }
    }

    return buf.str();
  }
};

SqliteConnection::SqliteConnection() { _impl.reset(new Impl); }

bool SqliteConnection::exists_table(const std::string &table_name) const {
  return _impl->exists_table(table_name);
}

int64_t SqliteConnection::last_row_id() const { return _impl->last_row_id(); }

Status SqliteConnection::connect(const Config &config) {
  auto result = _impl->connect(config);
  if (result.is_ok()) {
    _has_connection = true;
  }
  return result;
}

Status SqliteConnection::disconnect() {
  auto result = _impl->disconnect();
  if (result.is_ok()) {
    _has_connection = false;
  }
  return result;
}

Status SqliteConnection::create_table(std::shared_ptr<Schema> schema) {
  return _impl->create_table(schema);
}

Status SqliteConnection::drop_table(const std::string &table_name) {
  return _impl->drop_table(table_name);
}

Status SqliteConnection::transaction(const std::function<Status()> &t) {
  return _impl->transaction(t);
}

Status SqliteConnection::execute_sql(const std::string &sql) {
  return _impl->execute_sql(sql);
}

Status SqliteConnection::execute_sql_for_each(
    const std::string &sql, const std::function<void(const RowType &)> &fn) {
  return _impl->execute_sql_for_each(sql, fn);
}

}  // ookoto
