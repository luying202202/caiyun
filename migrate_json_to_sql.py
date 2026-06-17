import os
import json
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PICKS_DIR = os.path.join(SCRIPT_DIR, "picks_history")
PICKS520_DIR = os.path.join(SCRIPT_DIR, "picks520_history")
DIAG_DIR = os.path.join(SCRIPT_DIR, "diag_history")


def escape_sql(s):
    if s is None:
        return "NULL"
    return str(s).replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')


def json_to_sql_val(obj):
    if obj is None:
        return "NULL"
    text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return f"'{escape_sql(text)}'"


def migrate_picks():
    lines = []
    lines.append("-- 智能选股历史")
    if not os.path.isdir(PICKS_DIR):
        return lines
    for fname in sorted(os.listdir(PICKS_DIR)):
        if not fname.startswith("picks_") or not fname.endswith(".json"):
            continue
        filepath = os.path.join(PICKS_DIR, fname)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = [data]
        for idx, entry in enumerate(data):
            date_str = escape_sql(entry.get("date", ""))
            update_time = escape_sql(entry.get("update_time", ""))
            if idx == 0:
                kind = "single"
            elif idx == len(data) - 1 and len(data) > 1:
                kind = "last"
            else:
                kind = "first"
            picks_json = json_to_sql_val(entry.get("picks", []))
            lines.append(
                f"INSERT INTO picks_history (user_id, date, update_time, kind, picks_json) "
                f"VALUES (0, '{date_str}', '{update_time}', '{kind}', {picks_json});"
            )
    return lines


def migrate_picks520():
    lines = []
    lines.append("-- 520战法选股历史")
    if not os.path.isdir(PICKS520_DIR):
        return lines
    for fname in sorted(os.listdir(PICKS520_DIR)):
        if not fname.startswith("picks520_") or not fname.endswith(".json"):
            continue
        filepath = os.path.join(PICKS520_DIR, fname)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = [data]
        for idx, entry in enumerate(data):
            date_str = escape_sql(entry.get("date", ""))
            update_time = escape_sql(entry.get("update_time", ""))
            if idx == 0:
                kind = "single"
            elif idx == len(data) - 1 and len(data) > 1:
                kind = "last"
            else:
                kind = "first"
            picks_json = json_to_sql_val(entry.get("picks", []))
            lines.append(
                f"INSERT INTO picks520_history (user_id, date, update_time, kind, picks_json) "
                f"VALUES (0, '{date_str}', '{update_time}', '{kind}', {picks_json});"
            )
    return lines


def migrate_diag():
    lines = []
    lines.append("-- 个股诊断历史")
    if not os.path.isdir(DIAG_DIR):
        return lines
    for fname in sorted(os.listdir(DIAG_DIR)):
        if not fname.startswith("diag_") or not fname.endswith(".json"):
            continue
        filepath = os.path.join(DIAG_DIR, fname)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = [data]
        for entry in data:
            history_id = escape_sql(entry.get("history_id", ""))
            date_str = escape_sql(entry.get("date", ""))
            update_time = escape_sql(entry.get("update_time", ""))
            code = escape_sql(entry.get("code", ""))
            name = escape_sql(entry.get("name", ""))
            security_type = escape_sql(entry.get("security_type", ""))
            total_score = entry.get("total_score", 0)
            hold_advice = escape_sql(entry.get("hold_advice", ""))
            rate = entry.get("rate", 0)
            price = entry.get("price", 0)
            result_json = json_to_sql_val(entry.get("result", {}))
            lines.append(
                f"INSERT INTO diag_history (user_id, history_id, date, update_time, code, name, security_type, total_score, hold_advice, rate, price, result_json) "
                f"VALUES (0, '{history_id}', '{date_str}', '{update_time}', '{code}', '{name}', '{security_type}', {total_score}, '{hold_advice}', {rate}, {price}, {result_json});"
            )
    return lines


def main():
    sql_lines = [
        "SET NAMES utf8mb4;",
        "SET CHARACTER SET utf8mb4;",
        "SET character_set_connection=utf8mb4;",
        "ALTER DATABASE caiyun CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
        "",
        "-- A股资金流向历史数据迁移",
        "-- 所有数据 user_id=0 (公共数据)",
        "",
        "-- 建表语句",
        "CREATE TABLE IF NOT EXISTS users (",
        "  id INT AUTO_INCREMENT PRIMARY KEY,",
        "  username VARCHAR(50) NOT NULL UNIQUE,",
        "  password_hash VARCHAR(128) NOT NULL,",
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "  INDEX idx_username (username)",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "",
        "CREATE TABLE IF NOT EXISTS picks_history (",
        "  id BIGINT AUTO_INCREMENT PRIMARY KEY,",
        "  user_id INT NOT NULL DEFAULT 0,",
        "  date VARCHAR(10) NOT NULL,",
        "  update_time VARCHAR(20) NOT NULL,",
        "  kind VARCHAR(10) NOT NULL DEFAULT 'single',",
        "  picks_json MEDIUMTEXT NOT NULL,",
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "  INDEX idx_user_date (user_id, date),",
        "  INDEX idx_update_time (update_time)",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "",
        "CREATE TABLE IF NOT EXISTS picks520_history (",
        "  id BIGINT AUTO_INCREMENT PRIMARY KEY,",
        "  user_id INT NOT NULL DEFAULT 0,",
        "  date VARCHAR(10) NOT NULL,",
        "  update_time VARCHAR(20) NOT NULL,",
        "  kind VARCHAR(10) NOT NULL DEFAULT 'single',",
        "  picks_json MEDIUMTEXT NOT NULL,",
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "  INDEX idx_user_date (user_id, date),",
        "  INDEX idx_update_time (update_time)",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "",
        "CREATE TABLE IF NOT EXISTS diag_history (",
        "  id BIGINT AUTO_INCREMENT PRIMARY KEY,",
        "  user_id INT NOT NULL DEFAULT 0,",
        "  history_id VARCHAR(80) NOT NULL,",
        "  date VARCHAR(10) NOT NULL,",
        "  update_time VARCHAR(20) NOT NULL,",
        "  code VARCHAR(10) NOT NULL,",
        "  name VARCHAR(20) NOT NULL DEFAULT '',",
        "  security_type VARCHAR(10) NOT NULL DEFAULT '',",
        "  total_score FLOAT NOT NULL DEFAULT 0,",
        "  hold_advice VARCHAR(100) NOT NULL DEFAULT '',",
        "  rate FLOAT NOT NULL DEFAULT 0,",
        "  price FLOAT NOT NULL DEFAULT 0,",
        "  result_json MEDIUMTEXT NOT NULL,",
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "  UNIQUE INDEX idx_history_id (history_id),",
        "  INDEX idx_user_date (user_id, date),",
        "  INDEX idx_code (code)",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "",
        "CREATE TABLE IF NOT EXISTS sector_snapshot (",
        "  id BIGINT AUTO_INCREMENT PRIMARY KEY,",
        "  snapshot_time VARCHAR(20) NOT NULL,",
        "  data_json MEDIUMTEXT NOT NULL,",
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "  INDEX idx_snapshot_time (snapshot_time)",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "",
        "-- 补列：兼容旧表结构(无user_id)",
        "SET @dbname=DATABASE();",
        "SET @colname='user_id';",
        "SET @preparedStatement=(SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@dbname AND TABLE_NAME='picks_history' AND COLUMN_NAME=@colname)>0,'SELECT 1','ALTER TABLE picks_history ADD COLUMN user_id INT NOT NULL DEFAULT 0 AFTER id, ADD INDEX idx_user_date (user_id, date)'));",
        "PREPARE stmt FROM @preparedStatement; EXECUTE stmt; DEALLOCATE PREPARE stmt;",
        "SET @preparedStatement=(SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@dbname AND TABLE_NAME='picks520_history' AND COLUMN_NAME=@colname)>0,'SELECT 1','ALTER TABLE picks520_history ADD COLUMN user_id INT NOT NULL DEFAULT 0 AFTER id, ADD INDEX idx_user_date (user_id, date)'));",
        "PREPARE stmt FROM @preparedStatement; EXECUTE stmt; DEALLOCATE PREPARE stmt;",
        "SET @preparedStatement=(SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@dbname AND TABLE_NAME='diag_history' AND COLUMN_NAME=@colname)>0,'SELECT 1','ALTER TABLE diag_history ADD COLUMN user_id INT NOT NULL DEFAULT 0 AFTER id, ADD INDEX idx_user_date (user_id, date)'));",
        "PREPARE stmt FROM @preparedStatement; EXECUTE stmt; DEALLOCATE PREPARE stmt;",
        "ALTER TABLE diag_history MODIFY COLUMN hold_advice VARCHAR(100) NOT NULL DEFAULT '';",
        "",
        "-- 清空所有历史数据后重新导入",
        "TRUNCATE TABLE picks_history;",
        "TRUNCATE TABLE picks520_history;",
        "TRUNCATE TABLE diag_history;",
        "",
    ]
    sql_lines = [l for l in sql_lines if l is not None and l != ""]
    sql_lines.append("")
    sql_lines.extend(migrate_picks())
    sql_lines.append("")
    sql_lines.extend(migrate_picks520())
    sql_lines.append("")
    sql_lines.extend(migrate_diag())

    output_path = os.path.join(SCRIPT_DIR, "init_data.sql")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(sql_lines))

    picks_count = sum(1 for l in sql_lines if l.startswith("INSERT INTO picks_history"))
    picks520_count = sum(1 for l in sql_lines if l.startswith("INSERT INTO picks520_history"))
    diag_count = sum(1 for l in sql_lines if l.startswith("INSERT INTO diag_history"))
    print(f"生成完成: {output_path}")
    print(f"  picks_history: {picks_count} 条")
    print(f"  picks520_history: {picks520_count} 条")
    print(f"  diag_history: {diag_count} 条")


if __name__ == "__main__":
    main()