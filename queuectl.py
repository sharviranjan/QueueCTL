#!/usr/bin/env python3
import sqlite3
import argparse
import json
import time
import subprocess
import sys
import signal
import os
from datetime import datetime, timedelta
import multiprocessing
from typing import Optional, Dict, Any

# --- Constants & Configuration ---
DB_FILE = "queuectl.db"
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2

# --- Database Manager ---
class Database:
    def __init__(self, db_file=DB_FILE):
        self.db_file = db_file
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Jobs Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT DEFAULT 'pending', -- pending, processing, completed, dlq
                attempts INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                next_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_error TEXT
            )
        ''')
        
        # Config Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        # Set defaults if not exist
        cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("max_retries", str(DEFAULT_MAX_RETRIES)))
        cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("backoff_base", str(DEFAULT_BACKOFF_BASE)))
        
        conn.commit()
        conn.close()

    def get_config(self, key):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM config WHERE key=?", (key,))
        row = cursor.fetchone()
        conn.close()
        return row['value'] if row else None

    def set_config(self, key, value):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()
        conn.close()

    def enqueue_job(self, job_id, command):
        conn = self.get_connection()
        max_retries = int(self.get_config("max_retries"))
        try:
            conn.execute(
                "INSERT INTO jobs (id, command, max_retries, next_run_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                (job_id, command, max_retries)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()

    def fetch_next_job(self):
        """
        Atomic fetch: Finds a pending job that is ready to run, locks it, and marks it processing.
        Using SQLite explicit transaction to prevent race conditions.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        job = None
        
        try:
            conn.execute("BEGIN IMMEDIATE")
            # Find job ready to run
            cursor.execute("""
                SELECT * FROM jobs 
                WHERE state = 'pending' 
                AND datetime(next_run_at) <= datetime('now')
                ORDER BY created_at ASC 
                LIMIT 1
            """)
            row = cursor.fetchone()
            
            if row:
                job = dict(row)
                cursor.execute("""
                    UPDATE jobs 
                    SET state = 'processing', updated_at = CURRENT_TIMESTAMP 
                    WHERE id = ?
                """, (job['id'],))
                conn.commit()
            else:
                conn.rollback()
        except Exception as e:
            conn.rollback()
            print(f"DB Error: {e}")
        finally:
            conn.close()
        
        return job

    def update_job_status(self, job_id, status, attempts=None, next_run_at=None, error_msg=None):
        conn = self.get_connection()
        query = "UPDATE jobs SET state = ?, updated_at = CURRENT_TIMESTAMP"
        params = [status]
        
        if attempts is not None:
            query += ", attempts = ?"
            params.append(attempts)
        
        if next_run_at:
            query += ", next_run_at = ?"
            params.append(next_run_at)
            
        if error_msg:
            query += ", last_error = ?"
            params.append(error_msg)
            
        query += " WHERE id = ?"
        params.append(job_id)
        
        conn.execute(query, tuple(params))
        conn.commit()
        conn.close()

    def get_status_summary(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state")
        stats = {row['state']: row['count'] for row in cursor.fetchall()}
        conn.close()
        return stats

    def list_jobs(self, state=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        if state:
            cursor.execute("SELECT * FROM jobs WHERE state = ?", (state,))
        else:
            cursor.execute("SELECT * FROM jobs")
        jobs = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jobs
    
    def retry_dlq_job(self, job_id):
        conn = self.get_connection()
        # Reset attempts and set to pending
        conn.execute("""
            UPDATE jobs 
            SET state = 'pending', attempts = 0, next_run_at = CURRENT_TIMESTAMP, last_error = NULL 
            WHERE id = ? AND state = 'dlq'
        """, (job_id,))
        count = conn.total_changes
        conn.commit()
        conn.close()
        return count > 0

# --- Worker Logic ---
class Worker:
    def __init__(self, worker_id, db):
        self.worker_id = worker_id
        self.db = db
        self.running = True
        self.backoff_base = int(db.get_config("backoff_base"))

    def run(self):
        print(f"[Worker-{self.worker_id}] Started.")
        signal.signal(signal.SIGINT, self.handle_stop)
        signal.signal(signal.SIGTERM, self.handle_stop)

        while self.running:
            job = self.db.fetch_next_job()
            
            if job:
                self.process_job(job)
            else:
                # No jobs, sleep briefly to prevent CPU spinning
                time.sleep(1)
        
        print(f"[Worker-{self.worker_id}] Stopped gracefully.")

    def process_job(self, job):
        print(f"[Worker-{self.worker_id}] Processing Job ID: {job['id']} Command: {job['command']}")
        
        try:
            # Execute command
            start_time = time.time()
            result = subprocess.run(
                job['command'], 
                shell=True, 
                capture_output=True, 
                text=True
            )
            
            if result.returncode == 0:
                self.db.update_job_status(job['id'], 'completed')
                print(f"[Worker-{self.worker_id}] Job {job['id']} COMPLETED.")
            else:
                raise Exception(f"Exit Code {result.returncode}. Stderr: {result.stderr.strip()}")
                
        except Exception as e:
            self.handle_failure(job, str(e))

    def handle_failure(self, job, error_msg):
        attempts = job['attempts'] + 1
        max_retries = job['max_retries']
        
        print(f"[Worker-{self.worker_id}] Job {job['id']} FAILED. ({attempts}/{max_retries}) Error: {error_msg}")

        if attempts < max_retries:
            # Exponential Backoff: delay = base ^ attempts
            delay_seconds = self.backoff_base ** attempts
            next_run = datetime.now() + timedelta(seconds=delay_seconds)
            
            self.db.update_job_status(
                job['id'], 
                'pending', # Return to pending queue
                attempts=attempts, 
                next_run_at=next_run,
                error_msg=error_msg
            )
            print(f"[Worker-{self.worker_id}] Job {job['id']} scheduled for retry in {delay_seconds}s.")
        else:
            # Move to Dead Letter Queue
            self.db.update_job_status(
                job['id'], 
                'dlq', 
                attempts=attempts,
                error_msg=error_msg
            )
            print(f"[Worker-{self.worker_id}] Job {job['id']} moved to DLQ.")

    def handle_stop(self, signum, frame):
        print(f"\n[Worker-{self.worker_id}] Stopping... finishing current task.")
        self.running = False

def start_worker_process(worker_id):
    # Re-init DB connection for new process
    db = Database()
    worker = Worker(worker_id, db)
    worker.run()

# --- CLI Implementation ---
class QueueCTL:
    def __init__(self):
        self.db = Database()
        self.parser = argparse.ArgumentParser(description="QueueCTL: Background Job Queue System")
        self.setup_parser()

    def setup_parser(self):
        subparsers = self.parser.add_subparsers(dest="command", help="Available commands")

        # Enqueue
        parser_enq = subparsers.add_parser("enqueue", help="Add a new job")
        parser_enq.add_argument("job_json", help="JSON string with 'id' and 'command'")

        # Worker
        parser_worker = subparsers.add_parser("worker", help="Manage workers")
        parser_worker.add_argument("action", choices=["start", "stop"], help="Action")
        parser_worker.add_argument("--count", type=int, default=1, help="Number of workers to start")

        # Status
        subparsers.add_parser("status", help="Show system status")

        # List
        parser_list = subparsers.add_parser("list", help="List jobs")
        parser_list.add_argument("--state", choices=["pending", "processing", "completed", "dlq"], help="Filter by state")

        # DLQ
        parser_dlq = subparsers.add_parser("dlq", help="Dead Letter Queue management")
        parser_dlq.add_argument("action", choices=["list", "retry"], help="Action")
        parser_dlq.add_argument("job_id", nargs="?", help="Job ID to retry")

        # Config
        parser_conf = subparsers.add_parser("config", help="Configuration")
        parser_conf.add_argument("action", choices=["set", "get"], help="Action")
        parser_conf.add_argument("key", choices=["max-retries", "backoff-base"], help="Config key")
        parser_conf.add_argument("value", nargs="?", help="Config value (for set)")

    def run(self):
        args = self.parser.parse_args()
        if not args.command:
            self.parser.print_help()
            return

        if args.command == "enqueue":
            self.cmd_enqueue(args)
        elif args.command == "worker":
            self.cmd_worker(args)
        elif args.command == "status":
            self.cmd_status()
        elif args.command == "list":
            self.cmd_list(args)
        elif args.command == "dlq":
            self.cmd_dlq(args)
        elif args.command == "config":
            self.cmd_config(args)

    def cmd_enqueue(self, args):
        try:
            data = json.loads(args.job_json)
            if "id" not in data or "command" not in data:
                print("Error: JSON must contain 'id' and 'command'")
                return
            
            if self.db.enqueue_job(data['id'], data['command']):
                print(f"Job {data['id']} enqueued.")
            else:
                print(f"Error: Job ID {data['id']} already exists.")
        except json.JSONDecodeError:
            print("Error: Invalid JSON format.")

    def cmd_worker(self, args):
        if args.action == "start":
            print(f"Starting {args.count} worker(s)... Press Ctrl+C to stop.")
            processes = []
            try:
                for i in range(args.count):
                    p = multiprocessing.Process(target=start_worker_process, args=(i+1,))
                    p.start()
                    processes.append(p)
                
                for p in processes:
                    p.join()
            except KeyboardInterrupt:
                print("\nShutting down workers...")
                # Multiprocessing handles SIGINT propagation usually, 
                # but we ensure we wait for them.
                for p in processes:
                    p.terminate()
                    p.join()
        
        elif args.action == "stop":
            print("To stop workers, press Ctrl+C in the terminal where they are running.")

    def cmd_status(self):
        stats = self.db.get_status_summary()
        print("\n📊 Queue Status Summary")
        print("-----------------------")
        total = 0
        for state, count in stats.items():
            print(f"{state.title():<12}: {count}")
            total += count
        print("-----------------------")
        print(f"Total Jobs  : {total}")

    def cmd_list(self, args):
        jobs = self.db.list_jobs(args.state)
        print(f"{'ID':<15} {'STATE':<12} {'ATTEMPTS':<10} {'COMMAND'}")
        print("-" * 60)
        for job in jobs:
            print(f"{job['id']:<15} {job['state']:<12} {job['attempts']:<10} {job['command']}")

    def cmd_dlq(self, args):
        if args.action == "list":
            self.cmd_list(argparse.Namespace(state="dlq"))
        elif args.action == "retry":
            if not args.job_id:
                print("Error: Please specify job ID to retry.")
                return
            if self.db.retry_dlq_job(args.job_id):
                print(f"Job {args.job_id} moved from DLQ back to Pending.")
            else:
                print(f"Job {args.job_id} not found in DLQ.")

    def cmd_config(self, args):
        key_map = {"max-retries": "max_retries", "backoff-base": "backoff_base"}
        db_key = key_map[args.key]
        
        if args.action == "get":
            val = self.db.get_config(db_key)
            print(f"{args.key}: {val}")
        elif args.action == "set":
            if not args.value:
                print("Error: Value required for set.")
                return
            self.db.set_config(db_key, args.value)
            print(f"Configuration updated: {args.key} = {args.value}")

if __name__ == "__main__":
    QueueCTL().run()