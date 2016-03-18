#pragma once

#include <limits>
#include <map>
#include <memory>
#include <string>

namespace ookoto {

/**
 @struct Config

 ConnectionInterface に渡す設定を格納する構造体です。
 */
struct Config {
  /**
   接続ドライバを指定します。現時点でサポートするドライバは以下のとおりです。
   - mysql
   - sqlite3
   */
  std::string driver;

  /**
   接続先のホストあるいは IP アドレスを指定します

   対応するドライバ:
   - mysql
   */
  std::string host;

  /**
   接続先のポートを指定します

   対応するドライバ:
   - mysql
   */
  std::string port;

  /**
   接続先のデータベース名を指定します

   対応するドライバ:
   - mysql
   - sqlite3
   */
  std::string database;

  /**
   接続先のユーザー名を指定します

   対応するドライバ:
   - mysql
   */
  std::string username;

  /**
   接続先のパスワードを指定します

   対応するドライバ:
   - mysql
   */
  std::string password;
};
}
