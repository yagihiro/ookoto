#include <cppformat/format.h>
#include <ookoto/ookoto.h>
#include <memory>
#include "ConnectionImpl.h"

namespace ookoto {

Status ConnectionImpl::create_table(std::shared_ptr<Schema> schema) {
  fmt::MemoryWriter buf;

  buf << "CREATE TABLE " << schema->table_name() << " (";

  auto size = schema->defined_column_size();
  schema->each_define([&](const Schema::ColumnType &def) {
    size -= 1;
    buf << std::get<Schema::kColumnName>(def) << " "
        << column_type_to_string(std::get<Schema::kColumnType>(def));
    auto prop = column_prop_to_string(std::get<Schema::kColumnProperties>(def));
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

Status ConnectionImpl::drop_table(const std::string &table_name) {
  return execute_sql(fmt::format("DROP TABLE {}", table_name));
}

}  // ookoto
