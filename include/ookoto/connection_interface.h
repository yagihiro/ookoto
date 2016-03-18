#pragma once

#include <map>
#include <memory>
#include <string>
#include "config.h"
#include "schema.h"
#include "status.h"

namespace ookoto {

/**
 @class ConnectionInterface

 あらゆる DB への接続およびクエリ実行のインターフェイスです.
 */
class ConnectionInterface {
 public:
  virtual ~ConnectionInterface() = default;

  /**
   コネクションが確立していれば true を返します

   @retval true コネクションが確立している
   @retval false それ以外
   */
  bool has_connection() const { return _has_connection; }

  /**
   table_name が存在していれば true を返します。

   @retval true table_name が存在している
   @retval false それ以外
   @param table_name テーブル名
   */
  virtual bool exists_table(const std::string &table_name) const = 0;

  /**
   auto increment なカラムの最後の INSERT 時の ID を返します

   @see https://www.sqlite.org/c3ref/last_insert_rowid.html
   */
  virtual int64_t last_row_id() const = 0;

  /**
   DB とのコネクションを確立します

   @see Status
   @see Config
   */
  virtual Status connect(const Config &config) = 0;

  /**
   DB とのコネクションを切断します

   @see Status
   */
  virtual Status disconnect() = 0;

  /**
   テーブルを生成します

   @see Schema
   @see Status
   */
  virtual Status create_table(std::shared_ptr<Schema> schema) = 0;

  /**
   テーブルを削除します

   @see Schema
   @see Status
   */
  virtual Status drop_table(const std::string &table_name) = 0;

  /**
   トランザクションを開始してから t を実行し、t
   を実行後にトランザクションを終了します。

   @param t トランザクション間に実行する処理を指定してください
   @see Status
   */
  virtual Status transaction(const std::function<Status()> &t) = 0;

  /**
   sql を実行します

   @param sql 実行する SQL ステートメントを指定してください
   @see Status
   */
  virtual Status execute_sql(const std::string &sql) = 0;

  /**
   カラム名 : 値 の集合で 1 行を表現する型です
   */
  using RowType = std::map<std::string, std::string>;

  /**
   sql を実行し、取得した 1 レコード毎に fn を呼び出します

   @param sql 実行する SQL ステートメントを指定してください
   @param fn コールバックする関数を指定してください
   @see Status
   */
  virtual Status execute_sql_for_each(
      const std::string &sql,
      const std::function<void(const RowType &)> &fn) = 0;

 protected:
  bool _has_connection = false;
};
}
