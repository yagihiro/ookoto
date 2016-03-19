#include <cppformat/format.h>
#include <ookoto/ookoto.h>
#include <memory>
#include <string>

namespace ookoto {

class ConnectionImpl {
 public:
  virtual ~ConnectionImpl() = default;

  Status create_table(std::shared_ptr<Schema> schema);
  Status drop_table(const std::string &table_name);

  virtual Status execute_sql(const std::string &sql) = 0;
  virtual std::string column_type_to_string(Schema::Type type) = 0;
  virtual std::string column_prop_to_string(Schema::PropertyPtr prop) = 0;
};
}  // ookoto
