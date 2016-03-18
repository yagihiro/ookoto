#include <cppformat/format.h>
#include <mysql.h>
#include <ookoto/ookoto.h>
#include <cstdlib>
#include <exception>

namespace ookoto {

class MysqlConnection::Impl {
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

  MYSQL *_connection = nullptr;

  Impl() { _connection = mysql_init(nullptr); }

  ~Impl() { mysql_close(_connection); }

  Status connect(const Config &config) {
    unsigned port =
        config.port.size() == 0 ? 0 : std::atoi(config.port.c_str());

    if (!mysql_real_connect(_connection, config.host.c_str(),
                            config.username.c_str(), config.password.c_str(),
                            config.database.c_str(), port, nullptr, 0)) {
      return Status::status_ailment();
    }

    return Status::ok();
  }

  Status disconnect() {
    mysql_close(_connection);
    _connection = mysql_init(nullptr);
    return Status::ok();
  }

  Status execute_sql(const std::string &sql) {
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
  std::string err2str() const { return mysql_error(_connection); }
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
  return Status::ok();
}

Status MysqlConnection::drop_table(const std::string &table_name) {
  return Status::ok();
}

Status MysqlConnection::transaction(const std::function<Status()> &t) {
  return Status::ok();
}

Status MysqlConnection::execute_sql(const std::string &sql) {
  return _impl->execute_sql(sql);
}

Status MysqlConnection::execute_sql_for_each(
    const std::string &sql, const std::function<void(const RowType &)> &fn) {
  return _impl->execute_sql_for_each(sql, fn);
}
}  // ookoto
