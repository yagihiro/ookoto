#include <mysql.h>
#include <ookoto/ookoto.h>
#include <cstdlib>
#include <exception>

namespace ookoto {

class MysqlConnection::Impl {
 public:
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
  return Status::ok();
}

Status MysqlConnection::execute_sql_for_each(
    const std::string &sql, const std::function<void(const RowType &)> &fn) {
  return Status::ok();
}
}  // ookoto
