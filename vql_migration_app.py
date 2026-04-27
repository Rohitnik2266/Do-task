#!/usr/bin/env python3
"""
VQL Analyzer - Migration Server
================================
A lightweight Python server that:
1. Serves the VQL Analyzer HTML app
2. Provides /api/connections - lists Snowflake connections from connections.toml
3. Provides /api/execute - executes migration DDL against Snowflake (username/password auth)
4. Provides /api/test-connection - tests Snowflake connectivity

Usage:
    python vql_migration_app.py
    # Opens http://localhost:5000 in your browser
"""

import http.server
import json
import os
import re
import sys
import threading
import webbrowser
from pathlib import Path

try:
    import snowflake.connector
except ImportError:
    print("ERROR: snowflake-connector-python not installed.")
    print("Install with: pip install snowflake-connector-python")
    sys.exit(1)

PORT = 5000
BASE_DIR = Path(__file__).parent.resolve()


# ─── Parse connections.toml ───────────────────────────────────────────────────

def parse_connections_toml():
    """Read ~/.snowflake/connections.toml and return connection names + details."""
    toml_path = Path.home() / ".snowflake" / "connections.toml"
    if not toml_path.exists():
        return {}

    connections = {}
    current_section = None
    content = toml_path.read_text()

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Section header like [HEXAPPOC]
        m = re.match(r"^\[([^\]]+)\]$", line)
        if m:
            current_section = m.group(1)
            connections[current_section] = {}
            continue
        # Key = value
        if current_section and "=" in line:
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            connections[current_section][key] = val

    return connections


# ─── Execute SQL via snowflake.connector ──────────────────────────────────────

def execute_sql_statement(cursor, sql_text):
    """Execute a single SQL statement using an existing cursor.

    Returns (success: bool, message: str).
    """
    try:
        cursor.execute(sql_text)
        return True, "Executed successfully"
    except snowflake.connector.errors.ProgrammingError as e:
        return False, str(e).split("\n")[0]
    except Exception as e:
        return False, str(e).split("\n")[0]


def get_snowflake_connection(account, user, password, warehouse=None, role=None):
    """Create a Snowflake connection using username/password auth.

    Returns (connection, error_message). On success error_message is None.
    """
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse or None,
            role=role or None,
        )
        return conn, None
    except snowflake.connector.errors.DatabaseError as e:
        return None, str(e).split("\n")[0]
    except Exception as e:
        return None, str(e).split("\n")[0]


# ─── Split SQL into individual statements ─────────────────────────────────────

def split_sql_statements(full_sql):
    """
    Split a full SQL script into individual statements.
    Handles multi-line statements, comments, string literals, and $$ blocks properly.
    """
    statements = []
    current = []
    in_single_quote = False
    in_line_comment = False
    in_block_comment = False
    in_dollar_block = False
    i = 0
    chars = full_sql

    while i < len(chars):
        c = chars[i]

        # Handle $$ delimited blocks (stored procedures, UDFs)
        if not in_single_quote and not in_line_comment and not in_block_comment:
            if i + 1 < len(chars) and c == "$" and chars[i + 1] == "$":
                in_dollar_block = not in_dollar_block
                current.append(c)
                i += 1
                current.append(chars[i])
                i += 1
                continue

        if in_dollar_block:
            current.append(c)
            i += 1
            continue

        # Handle block comments
        if not in_single_quote and not in_line_comment:
            if not in_block_comment and i + 1 < len(chars) and c == "/" and chars[i + 1] == "*":
                in_block_comment = True
                current.append(c)
                i += 1
                current.append(chars[i])
                i += 1
                continue
            if in_block_comment and i + 1 < len(chars) and c == "*" and chars[i + 1] == "/":
                in_block_comment = False
                current.append(c)
                i += 1
                current.append(chars[i])
                i += 1
                continue

        if in_block_comment:
            current.append(c)
            i += 1
            continue

        # Handle line comments
        if not in_single_quote and not in_line_comment:
            if i + 1 < len(chars) and c == "-" and chars[i + 1] == "-":
                in_line_comment = True
                current.append(c)
                i += 1
                continue

        if in_line_comment:
            current.append(c)
            if c == "\n":
                in_line_comment = False
            i += 1
            continue

        # Handle string literals
        if c == "'" and not in_line_comment and not in_block_comment:
            # Check for escaped quote ''
            if in_single_quote and i + 1 < len(chars) and chars[i + 1] == "'":
                current.append(c)
                i += 1
                current.append(chars[i])
                i += 1
                continue
            in_single_quote = not in_single_quote
            current.append(c)
            i += 1
            continue

        # Semicolon outside quotes/comments/dollar-blocks = statement boundary
        if c == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt and not all(
                line.strip().startswith("--") or line.strip() == ""
                for line in stmt.splitlines()
            ):
                statements.append(stmt + ";")
            current = []
            i += 1
            continue

        current.append(c)
        i += 1

    # Handle last statement without trailing semicolon
    remaining = "".join(current).strip()
    if remaining and not all(
        line.strip().startswith("--") or line.strip() == ""
        for line in remaining.splitlines()
    ):
        statements.append(remaining)

    return statements


# ─── Categorize a SQL statement ───────────────────────────────────────────────

def categorize_statement(sql):
    """Return a human-readable category for a SQL statement."""
    upper = sql.upper().strip()
    # Skip pure comments
    if all(line.strip().startswith("--") or line.strip() == "" for line in sql.splitlines()):
        return "comment"
    if "CREATE DATABASE" in upper or "CREATE SCHEMA" in upper:
        return "database"
    if "CREATE OR REPLACE STAGE" in upper or "CREATE STAGE" in upper:
        return "stage"
    if "CREATE OR REPLACE DYNAMIC TABLE" in upper or "CREATE DYNAMIC TABLE" in upper:
        return "dynamic_table"
    if "CREATE OR REPLACE TABLE" in upper or "CREATE TABLE" in upper:
        return "table"
    if "CREATE OR REPLACE VIEW" in upper or "CREATE VIEW" in upper:
        return "view"
    if "CREATE OR REPLACE PROCEDURE" in upper or "CREATE PROCEDURE" in upper:
        return "procedure"
    if "ROW ACCESS POLICY" in upper:
        return "row_access_policy"
    if "MASKING POLICY" in upper:
        return "masking_policy"
    if "GRANT " in upper:
        return "grant"
    if "CREATE OR REPLACE TASK" in upper or "CREATE TASK" in upper or "ALTER TASK" in upper:
        return "task"
    if "USE WAREHOUSE" in upper or "USE ROLE" in upper or "USE DATABASE" in upper:
        return "setup"
    return "other"


# ─── HTTP Request Handler ─────────────────────────────────────────────────────

class MigrationHandler(http.server.SimpleHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def log_message(self, format, *args):
        """Log to stdout."""
        print(f"  [{self.command}] {self.path} - {args[0] if args else ''}", flush=True)

    def do_GET(self):
        # Serve the HTML app at root
        if self.path == "/" or self.path == "":
            self.path = "/vql_analyzer.html"
            return super().do_GET()

        # API: List available connections (pre-populates account/user from toml)
        if self.path == "/api/connections":
            connections = parse_connections_toml()
            conn_list = []
            for name, cfg in connections.items():
                conn_list.append({
                    "name": name,
                    "account": cfg.get("account", ""),
                    "user": cfg.get("user", ""),
                })
            self._send_json(200, {"connections": conn_list})
            return

        # API: Health check
        if self.path == "/api/health":
            self._send_json(200, {
                "status": "ok",
                "mode": "python_backend",
            })
            return

        # Serve static files
        return super().do_GET()

    def do_POST(self):
        # API: Execute migration SQL
        if self.path == "/api/execute":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self._send_json(400, {"error": "Invalid JSON"})
                return

            sql = data.get("sql", "")
            account = data.get("account", "")
            user = data.get("user", "")
            password = data.get("password", "")
            warehouse = data.get("warehouse", "")
            role = data.get("role", "")

            if not sql:
                self._send_json(400, {"error": "No SQL provided"})
                return
            if not account or not user or not password:
                self._send_json(400, {"error": "Account, username, and password are required"})
                return

            print(f"  [EXEC] Connecting to {account} as {user}...", flush=True)
            conn, err = get_snowflake_connection(account, user, password, warehouse, role)
            if err:
                print(f"  [EXEC] Connection failed: {err}", flush=True)
                self._send_json(401, {"error": f"Connection failed: {err}"})
                return

            print(f"  [EXEC] Connected. Executing migration...", flush=True)
            try:
                results = self._execute_migration(conn, sql)
                self._send_json(200, results)
            finally:
                conn.close()
                print(f"  [EXEC] Connection closed.", flush=True)
            return

        # API: Test connection
        if self.path == "/api/test-connection":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self._send_json(400, {"error": "Invalid JSON"})
                return

            account = data.get("account", "")
            user = data.get("user", "")
            password = data.get("password", "")
            warehouse = data.get("warehouse", "")
            role = data.get("role", "")

            if not account or not user or not password:
                self._send_json(400, {"error": "Account, username, and password are required"})
                return

            conn, err = get_snowflake_connection(account, user, password, warehouse, role)
            if err:
                self._send_json(500, {"status": "error", "error": err})
                return

            try:
                cur = conn.cursor()
                cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_USER()")
                row = cur.fetchone()
                info = {
                    "account": row[0],
                    "role": row[1],
                    "warehouse": row[2],
                    "user": row[3],
                }
                self._send_json(200, {"status": "connected", "info": info})
            except Exception as e:
                self._send_json(500, {"status": "error", "error": str(e)})
            finally:
                conn.close()
            return

        self._send_json(404, {"error": "Not found"})

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _send_json(self, code, data):
        response = json.dumps(data)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(response.encode("utf-8"))

    def _execute_migration(self, conn, full_sql):
        """Execute migration SQL statement by statement using the Snowflake connection."""
        statements = split_sql_statements(full_sql)
        results = []
        summary = {
            "total": len(statements),
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "categories": {},
        }

        cursor = conn.cursor()

        for i, stmt in enumerate(statements):
            category = categorize_statement(stmt)

            # Skip pure comments
            if category == "comment":
                summary["skipped"] += 1
                continue

            # Track category counts
            if category not in summary["categories"]:
                summary["categories"][category] = {"success": 0, "failed": 0}

            # Truncate for display (first meaningful line)
            display_lines = [l for l in stmt.splitlines() if l.strip() and not l.strip().startswith("--")]
            display = display_lines[0][:120] if display_lines else stmt[:120]

            print(f"  [{i+1}/{len(statements)}] {category}: {display[:80]}", flush=True)
            success, message = execute_sql_statement(cursor, stmt)

            if success:
                results.append({
                    "index": i + 1,
                    "sql": display,
                    "category": category,
                    "status": "success",
                    "message": "Executed successfully",
                })
                summary["success"] += 1
                summary["categories"][category]["success"] += 1
            else:
                results.append({
                    "index": i + 1,
                    "sql": display,
                    "category": category,
                    "status": "error",
                    "message": message,
                })
                summary["failed"] += 1
                summary["categories"][category]["failed"] += 1

        cursor.close()
        return {"results": results, "summary": summary}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    handler = MigrationHandler
    server = http.server.HTTPServer(("0.0.0.0", PORT), handler)

    print("=" * 60, flush=True)
    print("  VQL Analyzer - Migration Server", flush=True)
    print("=" * 60, flush=True)
    print(f"  URL:    http://localhost:{PORT}", flush=True)
    print(f"  Files:  {BASE_DIR}", flush=True)
    print(f"  Auth:   Username/Password (entered in browser UI)", flush=True)
    print(f"  API:    /api/connections, /api/execute, /api/test-connection", flush=True)
    print("=" * 60, flush=True)
    print("  Press Ctrl+C to stop the server", flush=True)
    print(flush=True)

    # Open browser after a short delay
    def open_browser():
        import time
        time.sleep(1)
        webbrowser.open(f"http://localhost:{PORT}")

    threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.", flush=True)
        server.server_close()


if __name__ == "__main__":
    main()
