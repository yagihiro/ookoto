#pragma once

#include "connection_interface.h"

namespace ookoto {

/**
 @class MysqlConnection

 MySQL への接続およびクエリ実行を実装しています.
 */
class MysqlConnection : public ConnectionInterface {
 public:
  MysqlConnection();
  virtual ~MysqlConnection() = default;

  virtual bool exists_table(const std::string &table_name) const;
  virtual int64_t last_row_id() const;

  virtual Status connect(const Config &config);
  virtual Status disconnect();

  virtual Status create_table(std::shared_ptr<Schema> schema);
  virtual Status drop_table(const std::string &table_name);

  virtual Status transaction(const std::function<Status()> &t);

  virtual Status execute_sql(const std::string &sql);
  using RowType = ConnectionInterface::RowType;
  virtual Status execute_sql_for_each(
      const std::string &sql, const std::function<void(const RowType &)> &fn);

 private:
  class Impl;
  std::unique_ptr<Impl> _impl;
};
}
