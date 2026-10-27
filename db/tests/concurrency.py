#!/usr/bin/env python3
"""Real concurrent psql sessions; Python stdlib only, no Kafka/Worker runtime."""
import os
import queue
import subprocess
import threading
import time
from pathlib import Path

PSQL = [os.environ.get("P01_PSQL", "psql"), "-X", "-v", "ON_ERROR_STOP=1", "-qAt"]
EXEC = "00000000-0000-0000-0000-000000000001"
OLD = "00000000-0000-0000-0000-000000000011"
NEW = "00000000-0000-0000-0000-000000000012"


def sql(statement):
    result = subprocess.run(PSQL, input=statement, text=True, capture_output=True, timeout=20)
    if result.returncode:
        raise AssertionError(result.stderr + result.stdout)
    return result.stdout.strip()


class Session:
    def __init__(self, name):
        self.name = name
        self.proc = subprocess.Popen(PSQL, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.lines = queue.Queue()
        self.number = 0
        def read():
            for line in self.proc.stdout:
                self.lines.put(line.rstrip())
            self.lines.put(None)
        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()
        self.run(f"SET application_name='{name}'; SET statement_timeout='12s';")

    def start(self, statement):
        self.number += 1
        self.marker = f"__done_{self.number}__"
        self.proc.stdin.write(statement + f"\nSELECT '{self.marker}';\n")
        self.proc.stdin.flush()

    def finish(self):
        result = []
        deadline = time.monotonic() + 15
        while True:
            line = self.lines.get(timeout=max(0.1, deadline - time.monotonic()))
            if line is None:
                raise AssertionError(f"{self.name}: psql exited: {result}")
            if line == self.marker:
                return "\n".join(result)
            result.append(line)

    def run(self, statement):
        self.start(statement)
        return self.finish()

    def close(self):
        if self.proc.poll() is None:
            self.proc.stdin.close()  # disconnect rolls back any outstanding tx
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)


def wait_for_lock(name):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if sql(f"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='{name}' AND wait_event_type='Lock');") == "t":
            return
        time.sleep(0.03)
    raise AssertionError(f"{name}: expected real lock wait")


def checkpoint(expected, offset, tick, owner=NEW, epoch=2, output=True):
    outputs = f"jsonb_build_array(p01_test.message('group_result',1,'{tick}'))" if output else "'[]'::jsonb"
    return f"""SELECT cad_v2.commit_checkpoint('{EXEC}','a01','{owner}',{epoch},
      {expected},{offset},'{tick}',1,'opaque-v1','\\x010203',
      encode(sha256('\\x010203'::bytea),'hex'),false,{outputs});"""


def main():
    if os.environ.get("P01_TEST_ALLOW_RESET") != "1" or not sql("SELECT current_database();").startswith("p01_"):
        raise SystemExit("Dedicated p01_* database and P01_TEST_ALLOW_RESET=1 required")
    sql(Path(__file__).with_name("fixtures.sql").read_text())
    sessions = []
    try:
        a, b = Session("p01-old-owner"), Session("p01-new-owner")
        sessions += [a, b]
        assert sql(f"SELECT cad_v2.acquire_partition('{EXEC}','a01','{OLD}',interval '2 seconds');") == "1"
        a.run("BEGIN;" + checkpoint(0, 1501, "2022-01-18T08:00:00Z", OLD, 1))
        assert sql("SELECT count(*) FROM cad_v2.group_checkpoints;") == "0"
        assert sql("SELECT count(*) FROM cad_v2.outbox_messages;") == "0"
        b.start(f"SELECT cad_v2.acquire_partition('{EXEC}','a01','{NEW}',interval '5 minutes');")
        wait_for_lock(b.name)
        # Let the lease expire while the old transaction still owns its row lock.
        deadline = time.monotonic() + 5
        while sql("SELECT lease_until <= clock_timestamp() FROM cad_v2.partition_ownership WHERE group_id='a01';") != "t":
            assert time.monotonic() < deadline
            time.sleep(0.03)
        a.run("COMMIT;")
        assert b.finish() == "2"
        assert sql("SELECT next_offset FROM cad_v2.group_checkpoints;") == "1501"
        assert sql("SELECT count(*) FROM cad_v2.outbox_messages;") == "1"
        sql(checkpoint(1, 1503, "2022-01-18T08:00:10Z"))
        sql("SELECT p01_test.expect_state($q$" + checkpoint(2, 1505, "2022-01-18T08:00:20Z", OLD, 1) + "$q$,'55000');")
        sql(f"SELECT p01_test.expect_state($q$SELECT cad_v2.ack_outbox('{EXEC}','a01','{OLD}',1,'x')$q$,'55000');")
        print("PASS concurrent fencing: old commit precedes takeover; stale state/outbox/ack rejected")

        c = Session("p01-crash-before-commit")
        sessions.append(c)
        c.run("BEGIN;" + checkpoint(2, 1505, "2022-01-18T08:00:20Z"))
        assert sql("SELECT next_offset FROM cad_v2.group_checkpoints;") == "1503"
        assert sql("SELECT count(*) FROM cad_v2.outbox_messages;") == "2"
        assert sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='p01-crash-before-commit';") == "t"
        assert sql("SELECT next_offset FROM cad_v2.group_checkpoints;") == "1503"
        assert sql("SELECT count(*) FROM cad_v2.outbox_messages;") == "2"
        print("PASS backend termination: snapshot/offset/outbox roll back together")

        message = "p01_test.message()"
        a.run(f"BEGIN; SELECT cad_v2.accept_result('{EXEC}',{message});")
        b.start(f"SELECT cad_v2.accept_result('{EXEC}',{message});")
        wait_for_lock(b.name)
        assert sql("SELECT count(*) FROM cad_v2.master_inbox;") == "0"
        a.run("ROLLBACK;")
        assert b.finish() == "t"
        assert sql(f"SELECT cad_v2.accept_result('{EXEC}',{message});") == "f"
        assert sql("SELECT count(*) FROM cad_v2.master_results;") == "1"
        print("PASS concurrent Master delivery: rollback, retry and one final result")

        ev = "p01_test.message('rbi2_event',1,'2022-01-18T08:00:00Z',false)"
        ev_other = "p01_test.message('rbi2_event',2,'2022-01-18T08:00:10Z',false,'a02')"
        a.run(f"BEGIN; SELECT cad_v2.accept_result('{EXEC}',{ev});")
        b.start(f"SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('{EXEC}',{ev_other})$q$,'55000');")
        wait_for_lock(b.name)
        a.run("COMMIT;")
        b.finish()
        assert sql("SELECT count(*) FROM cad_v2.master_events;") == "1"
        print("PASS cross-group EventID collision: serialized and rejected")
    finally:
        for session in sessions:
            session.close()
        sql("DROP SCHEMA p01_test CASCADE;")


if __name__ == "__main__":
    main()
