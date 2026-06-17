import os
import json
import time
import logging
import hashlib
import secrets
from datetime import datetime, timedelta

import jwt

log = logging.getLogger("caiyun")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "caiyun")
DB_USER = os.getenv("DB_USER", "caiyun")
DB_PASSWORD = os.getenv("DB_PASSWORD", "caiyun123")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
JWT_SECRET = os.getenv("JWT_SECRET", secrets.token_hex(32))
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "72"))

_db = None


def get_db():
    global _db
    if _db is None:
        import pymysql
        from dbutils.pooled_db import PooledDB
        _db = PooledDB(
            creator=pymysql,
            maxconnections=DB_POOL_SIZE,
            mincached=1,
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
    return _db


def _wait_for_mysql(max_retries=30, interval=2):
    import pymysql
    for i in range(max_retries):
        try:
            conn = pymysql.connect(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASSWORD,
                database=DB_NAME, charset="utf8mb4",
                connect_timeout=5,
            )
            conn.close()
            log.info("MySQL连接成功 (%s:%d/%s)", DB_HOST, DB_PORT, DB_NAME)
            return True
        except Exception as e:
            if i < max_retries - 1:
                log.warning("等待MySQL就绪...(%d/%d): %s", i + 1, max_retries, e)
                time.sleep(interval)
            else:
                log.error("MySQL连接失败，已重试%d次: %s", max_retries, e)
                return False


def _hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return f"{salt}${hashed}"


def _verify_password(password, stored):
    try:
        salt, hashed = stored.split("$", 1)
        return _hash_password(password, salt) == stored
    except (ValueError, AttributeError):
        return False


def create_token(user_id, username):
    payload = {
        "user_id": user_id,
        "username": username,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def decode_token(token):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def init_db(app=None):
    if not _wait_for_mysql():
        raise RuntimeError("无法连接MySQL数据库")
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(50) NOT NULL UNIQUE,
                    password_hash VARCHAR(128) NOT NULL,
                    is_admin TINYINT NOT NULL DEFAULT 0,
                    disabled TINYINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_username (username)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            try:
                cur.execute("ALTER TABLE users ADD COLUMN is_admin TINYINT NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE users ADD COLUMN disabled TINYINT NOT NULL DEFAULT 0")
            except Exception:
                pass
            cur.execute("""
                CREATE TABLE IF NOT EXISTS picks_history (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL DEFAULT 0,
                    date VARCHAR(10) NOT NULL,
                    update_time VARCHAR(20) NOT NULL,
                    kind VARCHAR(10) NOT NULL DEFAULT 'single',
                    picks_json MEDIUMTEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_date (user_id, date),
                    INDEX idx_update_time (update_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS picks520_history (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL DEFAULT 0,
                    date VARCHAR(10) NOT NULL,
                    update_time VARCHAR(20) NOT NULL,
                    kind VARCHAR(10) NOT NULL DEFAULT 'single',
                    picks_json MEDIUMTEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_date (user_id, date),
                    INDEX idx_update_time (update_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS diag_history (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL DEFAULT 0,
                    history_id VARCHAR(80) NOT NULL,
                    date VARCHAR(10) NOT NULL,
                    update_time VARCHAR(20) NOT NULL,
                    code VARCHAR(10) NOT NULL,
                    name VARCHAR(20) NOT NULL DEFAULT '',
                    security_type VARCHAR(10) NOT NULL DEFAULT '',
                    total_score FLOAT NOT NULL DEFAULT 0,
                    hold_advice VARCHAR(100) NOT NULL DEFAULT '',
                    rate FLOAT NOT NULL DEFAULT 0,
                    price FLOAT NOT NULL DEFAULT 0,
                    result_json MEDIUMTEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_history_id (history_id),
                    INDEX idx_user_date (user_id, date),
                    INDEX idx_code (code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sector_snapshot (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    snapshot_time VARCHAR(20) NOT NULL,
                    data_json MEDIUMTEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_snapshot_time (snapshot_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        log.info("数据库表初始化完成")
        _ensure_admin()
    finally:
        conn.close()


def _ensure_admin():
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN is_admin TINYINT NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE users ADD COLUMN disabled TINYINT NOT NULL DEFAULT 0")
            except Exception:
                pass
            cur.execute("SELECT id FROM users WHERE username=%s", ("luying",))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE users SET is_admin=1, disabled=0, password_hash=%s WHERE username=%s",
                            (_hash_password("Dove199822"), "luying"))
            else:
                cur.execute(
                    "INSERT INTO users (username, password_hash, is_admin, disabled) VALUES (%s, %s, 1, 0)",
                    ("luying", _hash_password("Dove199822"))
                )
            cur.execute("UPDATE users SET is_admin=1 WHERE username=%s", ("luying",))
        log.info("管理员账号已确保存在")
    except Exception as e:
        log.error("创建管理员失败: %s", e, exc_info=True)
    finally:
        conn.close()


def register_user(username, password):
    username = username.strip()
    if not username or not password:
        return None, "用户名和密码不能为空"
    if len(username) < 2 or len(username) > 50:
        return None, "用户名长度2-50个字符"
    if len(password) < 6:
        return None, "密码至少6个字符"
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE username=%s", (username,))
            if cur.fetchone():
                return None, "用户名已存在"
            pw_hash = _hash_password(password)
            cur.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                (username, pw_hash)
            )
            cur.execute("SELECT LAST_INSERT_ID() as uid")
            user_id = cur.fetchone()["uid"]
            token = create_token(user_id, username)
            return {"user_id": user_id, "username": username, "token": token, "is_admin": 0}, None
    except Exception as e:
        log.error("注册失败: %s", e)
        return None, "注册失败"
    finally:
        conn.close()


def login_user(username, password):
    username = username.strip()
    if not username or not password:
        return None, "用户名和密码不能为空"
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, password_hash, is_admin, disabled FROM users WHERE username=%s",
                (username,)
            )
            row = cur.fetchone()
            if not row:
                return None, "用户名或密码错误"
            if row.get("disabled"):
                return None, "账号已被禁用，请联系管理员"
            if not _verify_password(password, row["password_hash"]):
                return None, "用户名或密码错误"
            token = create_token(row["id"], row["username"])
            return {"user_id": row["id"], "username": row["username"], "token": token, "is_admin": row.get("is_admin", 0)}, None
    except Exception as e:
        log.error("登录失败: %s", e, exc_info=True)
        return None, f"登录失败: {e}"
    finally:
        conn.close()


def get_user_from_token(token_str):
    payload = decode_token(token_str)
    if not payload:
        return None
    user_id = payload.get("user_id")
    username = payload.get("username")
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT is_admin, disabled FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return None
            if row.get("disabled"):
                return None
            return {"user_id": user_id, "username": username, "is_admin": row.get("is_admin", 0)}
    except Exception:
        return {"user_id": user_id, "username": username, "is_admin": 0}
    finally:
        conn.close()


def save_picks_history(result, user_id=0):
    if not result or not result.get("success"):
        return
    date_str = datetime.now().strftime("%Y-%m-%d")
    update_time = result.get("update_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    picks_json = json.dumps(result.get("picks", []), ensure_ascii=False)
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, kind FROM picks_history WHERE user_id=%s AND date=%s ORDER BY id",
                (user_id, date_str)
            )
            rows = cur.fetchall()
            if len(rows) == 0:
                kind = "single"
            elif len(rows) == 1:
                kind = "last"
            else:
                kind = "last"
                cur.execute(
                    "DELETE FROM picks_history WHERE user_id=%s AND date=%s AND id NOT IN (SELECT min_id FROM (SELECT MIN(id) as min_id FROM picks_history WHERE user_id=%s AND date=%s) t)",
                    (user_id, date_str, user_id, date_str)
                )
            cur.execute(
                "INSERT INTO picks_history (user_id, date, update_time, kind, picks_json) VALUES (%s, %s, %s, %s, %s)",
                (user_id, date_str, update_time, kind, picks_json)
            )
            if len(rows) >= 2:
                cur.execute(
                    "DELETE FROM picks_history WHERE user_id=%s AND date=%s AND kind='last' AND id < (SELECT max_id FROM (SELECT MAX(id) as max_id FROM picks_history WHERE user_id=%s AND date=%s AND kind='last') t)",
                    (user_id, date_str, user_id, date_str)
                )
    except Exception as e:
        log.error("保存选股历史到DB失败: %s", e)
    finally:
        conn.close()


def load_picks_history(max_days=30, user_id=0):
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    records = []
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM picks_history WHERE user_id=%s AND date < %s",
                (user_id, cutoff)
            )
            cur.execute(
                "SELECT date, update_time, kind, picks_json FROM picks_history WHERE user_id=%s AND date >= %s ORDER BY date, id",
                (user_id, cutoff)
            )
            for row in cur.fetchall():
                try:
                    picks = json.loads(row["picks_json"])
                except (json.JSONDecodeError, TypeError):
                    picks = []
                records.append({
                    "date": row["date"],
                    "update_time": row["update_time"],
                    "kind": row.get("kind", "single"),
                    "picks": picks,
                })
    except Exception as e:
        log.error("加载选股历史从DB失败: %s", e)
    finally:
        conn.close()
    return records


def save_picks520_history(result, user_id=0):
    if not result or not result.get("success"):
        return
    date_str = datetime.now().strftime("%Y-%m-%d")
    update_time = result.get("update_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    picks_json = json.dumps(result.get("picks", []), ensure_ascii=False)
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, kind FROM picks520_history WHERE user_id=%s AND date=%s ORDER BY id",
                (user_id, date_str)
            )
            rows = cur.fetchall()
            if len(rows) == 0:
                kind = "single"
            elif len(rows) == 1:
                kind = "last"
            else:
                kind = "last"
                cur.execute(
                    "DELETE FROM picks520_history WHERE user_id=%s AND date=%s AND kind='last' AND id < (SELECT max_id FROM (SELECT MAX(id) as max_id FROM picks520_history WHERE user_id=%s AND date=%s AND kind='last') t)",
                    (user_id, date_str, user_id, date_str)
                )
            cur.execute(
                "INSERT INTO picks520_history (user_id, date, update_time, kind, picks_json) VALUES (%s, %s, %s, %s, %s)",
                (user_id, date_str, update_time, kind, picks_json)
            )
    except Exception as e:
        log.error("保存520历史到DB失败: %s", e)
    finally:
        conn.close()


def load_picks520_history(max_days=30, user_id=0):
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    records = []
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM picks520_history WHERE user_id=%s AND date < %s",
                (user_id, cutoff)
            )
            cur.execute(
                "SELECT date, update_time, kind, picks_json FROM picks520_history WHERE user_id=%s AND date >= %s ORDER BY date, id",
                (user_id, cutoff)
            )
            for row in cur.fetchall():
                try:
                    picks = json.loads(row["picks_json"])
                except (json.JSONDecodeError, TypeError):
                    picks = []
                records.append({
                    "date": row["date"],
                    "update_time": row["update_time"],
                    "kind": row.get("kind", "single"),
                    "picks": picks,
                })
    except Exception as e:
        log.error("加载520历史从DB失败: %s", e)
    finally:
        conn.close()
    return records


def save_diag_history(result, user_id=0):
    if not result or not result.get("success"):
        return
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    update_time = now.strftime("%Y-%m-%d %H:%M:%S")
    history_id = f"{int(now.timestamp() * 1000)}_{result.get('code', '')}"
    result_json = json.dumps(result, ensure_ascii=False)
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO diag_history (user_id, history_id, date, update_time, code, name, security_type, total_score, hold_advice, rate, price, result_json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (user_id, history_id, date_str, update_time,
                 result.get("code", ""), result.get("name", ""),
                 result.get("security_type", ""), result.get("total_score", 0),
                 result.get("hold_advice", ""), result.get("rate", 0),
                 result.get("price", 0), result_json)
            )
        log.info("诊断历史保存到DB: %s %s", date_str, result.get("code", ""))
    except Exception as e:
        log.error("保存诊断历史到DB失败: %s", e)
    finally:
        conn.close()


def load_diag_history(max_days=90, limit=200, user_id=0):
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    records = []
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM diag_history WHERE user_id=%s AND date < %s",
                (user_id, cutoff)
            )
            cur.execute(
                "SELECT history_id, date, update_time, code, name, security_type, total_score, hold_advice, rate, price, result_json FROM diag_history WHERE user_id=%s AND date >= %s ORDER BY update_time DESC LIMIT %s",
                (user_id, cutoff, limit)
            )
            for row in cur.fetchall():
                try:
                    result = json.loads(row["result_json"])
                except (json.JSONDecodeError, TypeError):
                    result = {}
                records.append({
                    "history_id": row["history_id"],
                    "date": row["date"],
                    "update_time": row["update_time"],
                    "code": row["code"],
                    "name": row["name"],
                    "security_type": row["security_type"],
                    "total_score": row["total_score"],
                    "hold_advice": row["hold_advice"],
                    "rate": row["rate"],
                    "price": row["price"],
                    "result": result,
                })
    except Exception as e:
        log.error("加载诊断历史从DB失败: %s", e)
    finally:
        conn.close()
    return records


def delete_diag_history(history_id, user_id=0):
    history_id = str(history_id or "").strip()
    if not history_id:
        return False
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM diag_history WHERE history_id=%s AND user_id=%s",
                (history_id, user_id)
            )
            return cur.rowcount > 0
    except Exception as e:
        log.error("删除诊断历史从DB失败: %s", e)
        return False
    finally:
        conn.close()


def save_sector_snapshot(data):
    if not data or not data.get("success"):
        return
    snapshot_time = data.get("update_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    data_json = json.dumps(data, ensure_ascii=False)
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sector_snapshot (snapshot_time, data_json) VALUES (%s, %s)",
                (snapshot_time, data_json)
            )
            cur.execute(
                "DELETE FROM sector_snapshot WHERE snapshot_time < %s",
                ((datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),)
            )
    except Exception as e:
        log.error("保存板块快照到DB失败: %s", e)
    finally:
        conn.close()


def list_users():
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, is_admin, disabled, created_at FROM users ORDER BY id"
            )
            rows = cur.fetchall()
            for r in rows:
                r["is_admin"] = bool(r.get("is_admin", 0))
                r["disabled"] = bool(r.get("disabled", 0))
                if "created_at" in r and r["created_at"]:
                    r["created_at"] = str(r["created_at"])
            return rows
    except Exception as e:
        log.error("列出用户失败: %s", e)
        return []
    finally:
        conn.close()


def toggle_user_disabled(user_id, disabled):
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT is_admin FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return False, "用户不存在"
            if row.get("is_admin"):
                return False, "不能禁用管理员"
            cur.execute("UPDATE users SET disabled=%s WHERE id=%s", (1 if disabled else 0, user_id))
            return True, None
    except Exception as e:
        log.error("切换用户状态失败: %s", e)
        return False, str(e)
    finally:
        conn.close()


def delete_user(user_id):
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT is_admin FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return False, "用户不存在"
            if row.get("is_admin"):
                return False, "不能删除管理员"
            cur.execute("DELETE FROM picks_history WHERE user_id=%s", (user_id,))
            cur.execute("DELETE FROM picks520_history WHERE user_id=%s", (user_id,))
            cur.execute("DELETE FROM diag_history WHERE user_id=%s", (user_id,))
            cur.execute("DELETE FROM users WHERE id=%s", (user_id,))
            return True, None
    except Exception as e:
        log.error("删除用户失败: %s", e)
        return False, str(e)
    finally:
        conn.close()


def load_picks_history_all(max_days=30):
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    records = []
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT h.date, h.update_time, h.kind, h.picks_json, h.user_id, u.username "
                "FROM picks_history h LEFT JOIN users u ON h.user_id=u.id "
                "WHERE h.date >= %s ORDER BY h.date, h.id",
                (cutoff,)
            )
            for row in cur.fetchall():
                try:
                    picks = json.loads(row["picks_json"])
                except (json.JSONDecodeError, TypeError):
                    picks = []
                records.append({
                    "date": row["date"],
                    "update_time": row["update_time"],
                    "kind": row.get("kind", "single"),
                    "picks": picks,
                    "user_id": row.get("user_id", 0),
                    "username": row.get("username", ""),
                })
    except Exception as e:
        log.error("加载全部选股历史失败: %s", e)
    finally:
        conn.close()
    return records


def load_picks520_history_all(max_days=30):
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    records = []
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT h.date, h.update_time, h.kind, h.picks_json, h.user_id, u.username "
                "FROM picks520_history h LEFT JOIN users u ON h.user_id=u.id "
                "WHERE h.date >= %s ORDER BY h.date, h.id",
                (cutoff,)
            )
            for row in cur.fetchall():
                try:
                    picks = json.loads(row["picks_json"])
                except (json.JSONDecodeError, TypeError):
                    picks = []
                records.append({
                    "date": row["date"],
                    "update_time": row["update_time"],
                    "kind": row.get("kind", "single"),
                    "picks": picks,
                    "user_id": row.get("user_id", 0),
                    "username": row.get("username", ""),
                })
    except Exception as e:
        log.error("加载全部520历史失败: %s", e)
    finally:
        conn.close()
    return records


def load_diag_history_all(max_days=90, limit=500):
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    records = []
    conn = get_db().connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT h.history_id, h.date, h.update_time, h.code, h.name, h.security_type, "
                "h.total_score, h.hold_advice, h.rate, h.price, h.result_json, h.user_id, u.username "
                "FROM diag_history h LEFT JOIN users u ON h.user_id=u.id "
                "WHERE h.date >= %s ORDER BY h.update_time DESC LIMIT %s",
                (cutoff, limit)
            )
            for row in cur.fetchall():
                try:
                    result = json.loads(row["result_json"])
                except (json.JSONDecodeError, TypeError):
                    result = {}
                records.append({
                    "history_id": row["history_id"],
                    "date": row["date"],
                    "update_time": row["update_time"],
                    "code": row["code"],
                    "name": row["name"],
                    "security_type": row["security_type"],
                    "total_score": row["total_score"],
                    "hold_advice": row["hold_advice"],
                    "rate": row["rate"],
                    "price": row["price"],
                    "result": result,
                    "user_id": row.get("user_id", 0),
                    "username": row.get("username", ""),
                })
    except Exception as e:
        log.error("加载全部诊断历史失败: %s", e)
    finally:
        conn.close()
    return records
