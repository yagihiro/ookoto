#include <cppformat/format.h>
#include <mysql.h>
#include <ookoto/ookoto.h>
#include <cstdlib>
#include <exception>
#include "ConnectionImpl.h"

namespace ookoto {

class MysqlConnection::Impl : public ConnectionImpl {
 public:
  class MysqlResultSet {
   public:
    MysqlResultSet(MYSQL_RES *res) : _res(res) {
      MYSQL_FIELD *schema;
      while ((schema = mysql_fetch_field(_res)) != nullptr) {
        _schemas.emplace_back(schema);
      }
    }

    void each(const std::function<void(const RowType &)> &fn) {
      MYSQL_ROW row;
      while ((row = mysql_fetch_row(_res)) != nullptr) {
        RowType row2;
        for (auto i = 0; i < _schemas.size(); i++) {
          auto value = row[i] ? row[i] : "NULL";
          row2.emplace(std::make_pair(_schemas[i]->name, value));
        }
        fn(row2);
      }
    }

   private:
    MYSQL_RES *_res = nullptr;
    std::vector<MYSQL_FIELD *> _schemas;
  };

  Impl() { _connection = mysql_init(nullptr); }

  virtual ~Impl() { mysql_close(_connection); }

  Status connect(const Config &config) {
    unsigned port =
        config.port.size() == 0 ? 0 : std::atoi(config.port.c_str());

    if (!mysql_real_connect(_connection, config.host.c_str(),
                            config.username.c_str(), config.password.c_str(),
                            config.database.c_str(), port, nullptr, 0)) {
      return Status::status_ailment();
    }

    auto result = start_auto_transaction();
    if (!result.is_ok()) {
      disconnect();
      return result;
    }

    return result;
  }

  Status disconnect() {
    mysql_close(_connection);
    _connection = mysql_init(nullptr);
    return Status::ok();
  }

  Status transaction(const std::function<Status()> &t) {
    if (t == nullptr) return Status::invalid_argument();

    auto result =
        do_auto_transaction([&]() -> Status { return do_transaction(t); });

    return result;
  }

  Status execute_sql(const std::string &sql) override {
    fmt::print("SQL: {}\n", sql);

    if (mysql_query(_connection, sql.c_str()) != 0) {
      return Status::status_ailment(err2str());
    }

    return Status::ok();
  }

  Status execute_sql_for_each(const std::string &sql,
                              const std::function<void(const RowType &)> &fn) {
    fmt::print("SQL: {}\n", sql);

    if (mysql_query(_connection, sql.c_str()) != 0) {
      return Status::status_ailment(err2str());
    }

    auto res = mysql_use_result(_connection);
    if (res == nullptr) {
      return Status::status_ailment(err2str());
    }

    MysqlResultSet results(res);
    results.each(fn);
    mysql_free_result(res);

    return Status::ok();
  }

 private:
  MYSQL *_connection = nullptr;

  std::string err2str() const { return mysql_error(_connection); }

  std::string column_type_to_string(Schema::Type type) override {
    static std::map<Schema::Type, std::string> mapping = {
        {Schema::Type::kInteger, "INT"}, {Schema::Type::kBoolean, "TINYINT"},
        {Schema::Type::kFloat, "FLOAT"}, {Schema::Type::kString, "VARCHAR"},
        {Schema::Type::kText, "TEXT"},   {Schema::Type::kDateTime, "DATETIME"},
        {Schema::Type::kDate, "DATE"},   {Schema::Type::kTime, "TIME"},
        {Schema::Type::kBinary, "BLOB"},
    };

    return mapping[type];
  }

  std::string column_prop_to_string(Schema::PropertyPtr prop) override {
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

  Status start_auto_transaction() {
    if (!mysql_autocommit(_connection, 1)) {
      return Status::status_ailment();
    }
    return Status::ok();
  }

  Status stop_auto_transaction() {
    if (!mysql_autocommit(_connection, 0)) {
      return Status::status_ailment();
    }
    return Status::ok();
  }

  Status do_auto_transaction(const std::function<Status()> &t) {
    stop_auto_transaction();
    auto result = t();
    start_auto_transaction();

    return result;
  }

  Status start_transaction() { return execute_sql("START TRANSACTION"); }

  Status commit() {
    if (!mysql_commit(_connection)) {
      return Status::status_ailment();
    }
    return Status::ok();
  }

  Status rollback() {
    if (!mysql_rollback(_connection)) {
      return Status::status_ailment();
    }
    return Status::ok();
  }

  Status do_transaction(const std::function<Status()> &t) {
    start_transaction();
    auto result = t();
    return (result.is_ok()) ? commit() : rollback();
  }
};

MysqlConnection::MysqlConnection() { _impl.reset(new Impl); }

bool MysqlConnection::exists_table(const std::string &table_name) const {
  return false;
}

int64_t MysqlConnection::last_row_id() const { return 0; }

Status MysqlConnection::connect(const Config &config) {
  return _impl->connect(config);
}

Status MysqlConnection::disconnect() { return _impl->disconnect(); }

Status MysqlConnection::create_table(std::shared_ptr<Schema> schema) {
  return _impl->create_table(schema);
}

Status MysqlConnection::drop_table(const std::string &table_name) {
  return _impl->drop_table(table_name);
}

Status MysqlConnection::transaction(const std::function<Status()> &t) {
  return _impl->transaction(t);
}

Status MysqlConnection::execute_sql(const std::string &sql) {
  return _impl->execute_sql(sql);
}

Status MysqlConnection::execute_sql_for_each(
    const std::string &sql, const std::function<void(const RowType &)> &fn) {
  return _impl->execute_sql_for_each(sql, fn);
}
}  // ookoto
