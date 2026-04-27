#!/usr/bin/env python3
"""
VQL Analyzer - Streamlit Cloud Deployment
==========================================
Wraps the VQL Analyzer HTML app in Streamlit and runs
a lightweight API server on a background thread for
Snowflake migration execution.
"""

import json
import os
import re
import socket
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

import streamlit as st

# ---------- optional: snowflake connector ----------
try:
    import snowflake.connector
    SF_AVAILABLE = True
except ImportError:
    SF_AVAILABLE = False

BASE_DIR = Path(__file__).parent.resolve()
API_PORT = 5001  # background API server port

# ─── SQL helpers (ported from vql_migration_app.py) ──────────────────────────

def split_sql_statements(full_sql):
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
        if not in_single_quote and not in_line_comment and not in_block_comment:
            if i + 1 < len(chars) and c == "$" and chars[i + 1] == "$":
                in_dollar_block = not in_dollar_block
                current.append(c); i += 1; current.append(chars[i]); i += 1; continue
        if in_dollar_block:
            current.append(c); i += 1; continue
        if not in_single_quote and not in_line_comment:
            if not in_block_comment and i + 1 < len(chars) and c == "/" and chars[i + 1] == "*":
                in_block_comment = True; current.append(c); i += 1; current.append(chars[i]); i += 1; continue
            if in_block_comment and i + 1 < len(chars) and c == "*" and chars[i + 1] == "/":
                in_block_comment = False; current.append(c); i += 1; current.append(chars[i]); i += 1; continue
        if in_block_comment:
            current.append(c); i += 1; continue
        if not in_single_quote and not in_line_comment:
            if i + 1 < len(chars) and c == "-" and chars[i + 1] == "-":
                in_line_comment = True; current.append(c); i += 1; continue
        if in_line_comment:
            current.append(c)
            if c == "\n": in_line_comment = False
            i += 1; continue
        if c == "'" and not in_line_comment and not in_block_comment:
            if in_single_quote and i + 1 < len(chars) and chars[i + 1] == "'":
                current.append(c); i += 1; current.append(chars[i]); i += 1; continue
            in_single_quote = not in_single_quote; current.append(c); i += 1; continue
        if c == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt and not all(l.strip().startswith("--") or l.strip() == "" for l in stmt.splitlines()):
                statements.append(stmt + ";")
            current = []; i += 1; continue
        current.append(c); i += 1
    remaining = "".join(current).strip()
    if remaining and not all(l.strip().startswith("--") or l.strip() == "" for l in remaining.splitlines()):
        statements.append(remaining)
    return statements


def categorize_statement(sql):
    upper = sql.upper().strip()
    if all(l.strip().startswith("--") or l.strip() == "" for l in sql.splitlines()):
        return "comment"
    for kw, cat in [
        ("CREATE DATABASE", "database"), ("CREATE SCHEMA", "database"),
        ("CREATE OR REPLACE STAGE", "stage"), ("CREATE STAGE", "stage"),
        ("CREATE OR REPLACE DYNAMIC TABLE", "dynamic_table"), ("CREATE DYNAMIC TABLE", "dynamic_table"),
        ("CREATE OR REPLACE TABLE", "table"), ("CREATE TABLE", "table"),
        ("CREATE OR REPLACE VIEW", "view"), ("CREATE VIEW", "view"),
        ("CREATE OR REPLACE PROCEDURE", "procedure"), ("CREATE PROCEDURE", "procedure"),
        ("ROW ACCESS POLICY", "row_access_policy"), ("MASKING POLICY", "masking_policy"),
        ("GRANT ", "grant"),
        ("CREATE OR REPLACE TASK", "task"), ("CREATE TASK", "task"), ("ALTER TASK", "task"),
        ("USE WAREHOUSE", "setup"), ("USE ROLE", "setup"), ("USE DATABASE", "setup"),
    ]:
        if kw in upper:
            return cat
    return "other"


# ─── Background API Server ───────────────────────────────────────────────────

class APIHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silent

    def _send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/health":
            self._send_json(200, {"status": "ok", "mode": "streamlit_backend"})
            return
        if self.path == "/api/connections":
            self._send_json(200, {"connections": []})
            return
        self.send_error(404)

    def do_POST(self):
        if self.path == "/api/execute":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode()
            try:
                data = json.loads(body)
            except Exception:
                self._send_json(400, {"error": "Invalid JSON"}); return

            if not SF_AVAILABLE:
                self._send_json(500, {"error": "snowflake-connector-python not installed on server"}); return

            sql = data.get("sql", "")
            account = data.get("account", "")
            user = data.get("user", "")
            password = data.get("password", "")
            warehouse = data.get("warehouse", "")
            role = data.get("role", "")

            if not sql:
                self._send_json(400, {"error": "No SQL provided"}); return
            if not account or not user or not password:
                self._send_json(400, {"error": "Account, username, and password are required"}); return

            try:
                conn = snowflake.connector.connect(
                    account=account, user=user, password=password,
                    warehouse=warehouse or None, role=role or None,
                )
            except Exception as e:
                self._send_json(401, {"error": f"Connection failed: {str(e).splitlines()[0]}"}); return

            try:
                results, summary = [], {"total": 0, "success": 0, "failed": 0, "skipped": 0, "categories": {}}
                stmts = split_sql_statements(sql)
                summary["total"] = len(stmts)
                cursor = conn.cursor()
                for i, stmt in enumerate(stmts):
                    cat = categorize_statement(stmt)
                    if cat == "comment":
                        summary["skipped"] += 1; continue
                    if cat not in summary["categories"]:
                        summary["categories"][cat] = {"success": 0, "failed": 0}
                    display = [l for l in stmt.splitlines() if l.strip() and not l.strip().startswith("--")]
                    display = display[0][:120] if display else stmt[:120]
                    try:
                        cursor.execute(stmt)
                        results.append({"index": i+1, "sql": display, "category": cat, "status": "success", "message": "Executed successfully"})
                        summary["success"] += 1; summary["categories"][cat]["success"] += 1
                    except Exception as e:
                        msg = str(e).splitlines()[0]
                        results.append({"index": i+1, "sql": display, "category": cat, "status": "error", "message": msg})
                        summary["failed"] += 1; summary["categories"][cat]["failed"] += 1
                cursor.close()
                self._send_json(200, {"results": results, "summary": summary})
            finally:
                conn.close()
            return

        if self.path == "/api/test-connection":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode()
            try:
                data = json.loads(body)
            except Exception:
                self._send_json(400, {"error": "Invalid JSON"}); return

            if not SF_AVAILABLE:
                self._send_json(500, {"status": "error", "error": "snowflake-connector-python not installed"}); return

            account = data.get("account", "")
            user = data.get("user", "")
            password = data.get("password", "")
            warehouse = data.get("warehouse", "")
            role = data.get("role", "")

            if not account or not user or not password:
                self._send_json(400, {"error": "Account, username, and password are required"}); return
            try:
                conn = snowflake.connector.connect(
                    account=account, user=user, password=password,
                    warehouse=warehouse or None, role=role or None,
                )
                cur = conn.cursor()
                cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_USER()")
                row = cur.fetchone()
                self._send_json(200, {"status": "connected", "info": {"account": row[0], "role": row[1], "warehouse": row[2], "user": row[3]}})
                conn.close()
            except Exception as e:
                self._send_json(500, {"status": "error", "error": str(e).splitlines()[0]})
            return

        self.send_error(404)


def _start_api_server():
    """Start the background API server if not already running."""
    key = "_api_server_started"
    if key not in st.session_state:
        try:
            # Check if port is already in use
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            result = s.connect_ex(("127.0.0.1", API_PORT))
            s.close()
            if result == 0:
                st.session_state[key] = True
                return  # already running
        except Exception:
            pass

        server = HTTPServer(("127.0.0.1", API_PORT), APIHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        st.session_state[key] = True


# ─── Main Streamlit App ─────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="VQL Analyzer", layout="wide", initial_sidebar_state="collapsed")

    # Hide Streamlit chrome for a clean look
    st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}
        footer {visibility: hidden;}
        .stApp > header {display: none;}
        .block-container {padding: 0 !important; max-width: 100% !important;}
        iframe {border: none !important;}
    </style>
    """, unsafe_allow_html=True)

    # Start background API server
    _start_api_server()

    # Read the HTML file
    html_path = BASE_DIR / "vql_analyzer.html"
    if not html_path.exists():
        st.error("vql_analyzer.html not found in the same directory as this script.")
        return

    html_content = html_path.read_text(encoding="utf-8")

    # Patch the HTML to point API calls to our background server
    # Replace relative /api/ calls with absolute http://localhost:API_PORT/api/
    html_content = html_content.replace(
        "fetch('/api/",
        f"fetch('http://localhost:{API_PORT}/api/"
    )

    # Render the full HTML app
    st.components.v1.html(html_content, height=900, scrolling=True)


if __name__ == "__main__":
    main()
