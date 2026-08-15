#!/usr/bin/env python3
"""Локальный интерактор для задачи A. Edge-Cloud Collaborative Scheduling.

Модель мира повторяет условие (task.md): один локальный сервер E, K удалённых
C0..C_{K-1}, две независимые FIFO-очереди переводов UP/DOWN, стоимость планирования S
за каждую задачу. Интерактор шлёт фреймы решению, читает ответы, ВАЛИДИРУЕТ их
легальность и в конце печатает скоринг.

Формат файла теста (комментарии '#' и пустые строки игнорируются):

    K S latency_in_ms bandwidth_gbps bytes_per_token num_layers
    SLO1 SLO2 tp_UB tp_base dist_base w_tp w_c
    N
    <N строк: batch_size prefill_pre prefill_proc prefill_post decode_pre decode_proc decode_post>
    R
    <R строк: arrival_time Lin Lout>

Запуск:  python3 harness/interactor.py tests/example1.txt -- ./target/release/huawei-sched
"""

import argparse
import heapq
import math
import subprocess
import sys
import threading

FMT = "%.9f"


def die(msg, code=1):
    sys.stderr.write("VIOLATION: %s\n" % msg)
    sys.stdout.write("verdict violation\nscore 0.000\n")
    sys.stdout.flush()
    sys.exit(code)


# ---------------------------------------------------------------- таблица времён


class TaskTable:
    """Кусочно-линейная интерполяция с плоскими хвостами; -1 = значение отсутствует."""

    def __init__(self, rows):
        self.cols = [[] for _ in range(6)]
        for bs, vals in rows:
            for c, v in enumerate(vals):
                if v >= 0:
                    self.cols[c].append((bs, v))
        for c in self.cols:
            c.sort()

    def lookup(self, col, size):
        c = self.cols[col]
        if not c:
            raise ValueError("empty column %d" % col)
        if size <= c[0][0]:
            return c[0][1]
        if size >= c[-1][0]:
            return c[-1][1]
        lo, hi = 0, len(c) - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if c[mid][0] <= size:
                lo = mid
            else:
                hi = mid
        x0, y0 = c[lo]
        x1, y1 = c[hi]
        if x1 == x0:
            return y0
        return y0 + (y1 - y0) * (size - x0) / (x1 - x0)


PREFILL_PRE, PREFILL_PROC, PREFILL_POST = 0, 1, 2
DECODE_PRE, DECODE_PROC, DECODE_POST = 3, 4, 5


# ---------------------------------------------------------------- разбор теста


def read_test(path):
    with open(path) as f:
        toks = []
        for line in f:
            line = line.split("#", 1)[0]
            toks.extend(line.split())
    it = iter(toks)

    def nxt():
        return next(it)

    cfg = {
        "K": int(nxt()),
        "S": float(nxt()),
        "latency": float(nxt()),
        "bw": float(nxt()),
        "bpt": float(nxt()),
        "num_layers": int(nxt()),
        "SLO1": float(nxt()),
        "SLO2": float(nxt()),
        "tp_UB": float(nxt()),
        "tp_base": float(nxt()),
        "dist_base": float(nxt()),
        "w_tp": float(nxt()),
        "w_c": float(nxt()),
    }
    n = int(nxt())
    rows = []
    for _ in range(n):
        bs = int(nxt())
        rows.append((bs, [float(nxt()) for _ in range(6)]))
    r = int(nxt())
    reqs = []
    for _ in range(r):
        reqs.append((float(nxt()), int(nxt()), int(nxt())))
    reqs.sort(key=lambda x: x[0])
    return cfg, TaskTable(rows), reqs


# ---------------------------------------------------------------- состояние запроса

(
    ARRIVED,
    PPRE_RUN,
    UP_PRE,
    PPROC_READY,
    PPROC_RUN,
    DOWN_PRE,
    PPOST_READY,
    PPOST_RUN,
    READY_D,
    DPRE_RUN,
    UP_DEC,
    DPROC_READY,
    DPROC_RUN,
    DOWN_DEC,
    DPOST_READY,
    DPOST_RUN,
    FINISHED,
) = range(17)


class Req:
    __slots__ = (
        "rid",
        "arrival",
        "lin",
        "lout",
        "state",
        "remote",
        "cursor",
        "tokens",
        "tdr_end",
        "token_times",
    )

    def __init__(self, rid, arrival, lin, lout):
        self.rid = rid
        self.arrival = arrival
        self.lin = lin
        self.lout = lout
        self.state = ARRIVED
        self.remote = -1
        self.cursor = 0
        self.tokens = 0
        self.tdr_end = None
        self.token_times = []


# ---------------------------------------------------------------- интерактор


class Interactor:
    def __init__(self, cfg, table, reqs, proc, trace=False, timeout=30.0):
        self.cfg = cfg
        self.K = cfg["K"]
        self.S = cfg["S"]
        self.NL = cfg["num_layers"]
        self.table = table
        self.proc = proc
        self.trace = trace
        self.timeout = timeout

        self.reqs = [Req(i, a, lin, lout) for i, (a, lin, lout) in enumerate(reqs)]
        self.arrived = [False] * len(self.reqs)

        self.busy = {}  # server -> занят до момента t (сервер свободен при t >= busy_until)
        self.busy["E"] = 0.0
        for k in range(self.K):
            self.busy["C%d" % k] = 0.0

        self.link_free = {"UP": 0.0, "DOWN": 0.0}
        self.heap = []
        self.seq = 0
        self.now = 0.0
        self.finished = 0
        self.killed = False
        self.pending = []

        for r in self.reqs:
            self.push(r.arrival, ("ARR", r.rid))

    # ---------------- очередь событий

    def push(self, t, ev):
        heapq.heappush(self.heap, (t, self.seq, ev))
        self.seq += 1

    def transfer_ms(self, length):
        size = length * self.cfg["bpt"]
        return self.cfg["latency"] + 8.0 * size / (self.cfg["bw"] * 1e6)

    def queue_transfer(self, t, direction, remote, length, kind, rids):
        """Перевод встаёт в FIFO своего направления; направления независимы."""
        start = max(t, self.link_free[direction])
        fin = start + self.transfer_ms(length)
        self.link_free[direction] = fin
        size = length * self.cfg["bpt"]
        self.push(fin, ("XDN", direction, remote, size, kind, list(rids)))

    # ---------------- обмен с решением

    def send(self, text):
        if self.trace:
            sys.stderr.write(">>> " + text)
        try:
            self.proc.stdin.write(text)
            self.proc.stdin.flush()
        except (BrokenPipeError, ValueError):
            die("решение закрыло stdin (упало?)")

    def _kill(self):
        self.killed = True
        try:
            self.proc.kill()
        except OSError:
            pass

    def read_line(self):
        # select() здесь непригоден: readline() у TextIOWrapper уже мог вычитать
        # следующие строки во внутренний буфер, и fd выглядел бы «пустым».
        # Поэтому сторожевой таймер, который просто убивает зависшее решение.
        timer = threading.Timer(self.timeout, self._kill)
        timer.start()
        try:
            line = self.proc.stdout.readline()
        finally:
            timer.cancel()
        if self.killed:
            die("таймаут ожидания ответа (%.1f с)" % self.timeout)
        if line == "":
            die("EOF от решения — оно завершилось до END")
        if self.trace:
            sys.stderr.write("<<< " + line)
        return line

    def next_tok(self):
        """Ответ — поток токенов: условие разрешает произвольные пробелы между ними."""
        while not self.pending:
            self.pending = self.read_line().split()
        return self.pending.pop(0)

    def read_tokens(self, count):
        return [self.next_tok() for _ in range(count)]

    # ---------------- главный цикл

    def run(self):
        while True:
            if not self.heap:
                if self.finished < len(self.reqs):
                    die("stuck state: незавершённые запросы, но событий больше нет")
                break
            t = self.heap[0][0]
            frame = []
            while self.heap and self.heap[0][0] == t:
                frame.append(heapq.heappop(self.heap)[2])
            self.now = t

            lines = []
            for ev in frame:
                lines.extend(self.apply_event(t, ev))

            out = [FMT % t, str(len(lines))] + lines
            self.send("\n".join(out) + "\n")
            self.handle_response(t)

            if self.finished == len(self.reqs):
                self.send("END\n")
                break
        return self.score()

    def apply_event(self, t, ev):
        """Применить событие к состоянию мира; вернуть строки для фрейма."""
        kind = ev[0]
        if kind == "ARR":
            rid = ev[1]
            self.arrived[rid] = True
            return ["ARR %d %d" % (rid, self.reqs[rid].lin)]

        if kind == "XDN":
            _, direction, remote, size, sub, rids = ev
            for rid in rids:
                r = self.reqs[rid]
                if direction == "UP" and sub == "PRE":
                    r.state = PPROC_READY
                elif direction == "DOWN" and sub == "PRE":
                    r.state = PPOST_READY
                elif direction == "UP":
                    r.state = DPROC_READY
                else:
                    r.state = DPOST_READY
            return [
                "XDN %s %d %d %s %d %s"
                % (direction, remote, int(round(size)), sub, len(rids), " ".join(map(str, rids)))
            ]

        # TDN
        _, server, spec, dur, meta = ev
        self.busy[server] = 0.0
        out = ["TDN %s %s %s" % (server, spec, FMT % dur)]
        typ = meta[0]

        if typ == "PPRE":
            rid = meta[1]
            r = self.reqs[rid]
            r.state = UP_PRE
            self.queue_transfer(t, "UP", r.remote, r.lin, "PRE", [rid])
        elif typ == "PPROC":
            rid, le = meta[1], meta[2]
            r = self.reqs[rid]
            r.cursor = le
            if le >= self.NL:
                r.state = DOWN_PRE
                self.queue_transfer(t, "DOWN", r.remote, r.lin, "PRE", [rid])
            else:
                r.state = PPROC_READY
        elif typ == "PPOST":
            rid = meta[1]
            r = self.reqs[rid]
            r.state = READY_D
            r.tdr_end = t
        elif typ == "DPRE":
            rids = meta[1]
            for rid in rids:
                self.reqs[rid].state = UP_DEC
            # По одному переводу на каждый задействованный удалённый сервер,
            # в порядке возрастания его индекса.
            by_remote = {}
            for rid in rids:
                by_remote.setdefault(self.reqs[rid].remote, []).append(rid)
            for remote in sorted(by_remote):
                members = by_remote[remote]
                self.queue_transfer(t, "UP", remote, len(members), "DEC", members)
        elif typ == "DPROC":
            rids, remote = meta[1], meta[2]
            for rid in rids:
                self.reqs[rid].state = DOWN_DEC
            self.queue_transfer(t, "DOWN", remote, len(rids), "DEC", rids)
        elif typ == "DPOST":
            rids = meta[1]
            for rid in rids:
                r = self.reqs[rid]
                r.tokens += 1
                r.token_times.append(t)
                if r.tokens >= r.lout:
                    r.state = FINISHED
                    self.finished += 1
                    out.append("FIN %d" % rid)
                else:
                    r.state = READY_D
        return out

    # ---------------- разбор и валидация ответа

    def handle_response(self, t):
        tok = self.next_tok()
        try:
            n = int(tok)
        except ValueError:
            die("нечисловое n: %r" % tok)
        if n < 0 or n > self.K + 1:
            die("n=%d вне [0, K+1]" % n)

        used = set()
        for _ in range(n):
            self.parse_assignment(t, used)
        if self.pending:
            die("лишние токены после %d назначений: %r" % (n, self.pending))

    def parse_assignment(self, t, used):
        toks = self.read_tokens(2)
        server, kind = toks[0], toks[1]
        if server not in self.busy:
            die("неизвестный сервер %r" % server)
        if server in used:
            die("две задачи на %s в одном ответе" % server)
        if self.busy[server] > t:
            die("%s занят до %.9f, назначение в %.9f" % (server, self.busy[server], t))

        if kind == "P":
            step = self.read_tokens(1)[0]
            if step == "PRE":
                self.do_ppre(t, server, used)
            elif step == "PROC":
                self.do_pproc(t, server, used)
            elif step == "POST":
                self.do_ppost(t, server, used)
            else:
                die("неизвестный шаг P %r" % step)
        elif kind == "D":
            step = self.read_tokens(1)[0]
            if step == "PRE":
                self.do_dgroup(t, server, used, "DPRE")
            elif step == "PROC":
                self.do_dproc(t, server, used)
            elif step == "POST":
                self.do_dgroup(t, server, used, "DPOST")
            else:
                die("неизвестный шаг D %r" % step)
        else:
            die("неизвестный тип задачи %r" % kind)

    def occupy(self, server, t, dur, spec, meta, used):
        used.add(server)
        self.busy[server] = t + self.S + dur
        self.push(t + self.S + dur, ("TDN", server, spec, dur, meta))

    def check_rid(self, tok):
        try:
            rid = int(tok)
        except ValueError:
            die("нечисловой rid %r" % tok)
        if rid < 0 or rid >= len(self.reqs):
            die("rid %d вне диапазона" % rid)
        if not self.arrived[rid]:
            die("rid %d ещё не прибыл (не было ARR)" % rid)
        return rid

    def do_ppre(self, t, server, used):
        remote, rid = self.read_tokens(2)
        remote = int(remote)
        rid = self.check_rid(rid)
        if server != "E":
            die("P PRE должен идти на E, а не на %s" % server)
        if not (0 <= remote < self.K):
            die("P PRE remote=%d вне [0,K)" % remote)
        r = self.reqs[rid]
        if r.state != ARRIVED:
            die("P PRE для rid=%d в состоянии %d" % (rid, r.state))
        r.remote = remote
        r.state = PPRE_RUN
        dur = self.table.lookup(PREFILL_PRE, r.lin)
        self.occupy(server, t, dur, "P PRE %d %d" % (remote, rid), ("PPRE", rid), used)

    def do_pproc(self, t, server, used):
        ls, le, remote, rid = self.read_tokens(4)
        ls, le, remote = int(ls), int(le), int(remote)
        rid = self.check_rid(rid)
        r = self.reqs[rid]
        if server != "C%d" % remote:
            die("P PROC на %s, но remote=%d" % (server, remote))
        if remote != r.remote:
            die("P PROC remote=%d, а назначен %d" % (remote, r.remote))
        if r.state != PPROC_READY:
            die("P PROC для rid=%d в состоянии %d" % (rid, r.state))
        if not (0 <= ls < le <= self.NL):
            die("некорректный кусок [%d,%d) при num_layers=%d" % (ls, le, self.NL))
        if ls != r.cursor:
            die("кусок начинается с %d, ожидалось %d (дыра/пересечение)" % (ls, r.cursor))
        r.state = PPROC_RUN
        dur = (le - ls) / float(self.NL) * self.table.lookup(PREFILL_PROC, r.lin)
        self.occupy(
            server,
            t,
            dur,
            "P PROC %d %d %d %d" % (ls, le, remote, rid),
            ("PPROC", rid, le),
            used,
        )

    def do_ppost(self, t, server, used):
        remote, rid = self.read_tokens(2)
        remote = int(remote)
        rid = self.check_rid(rid)
        r = self.reqs[rid]
        if server != "E":
            die("P POST должен идти на E, а не на %s" % server)
        if remote != r.remote:
            die("P POST remote=%d, а назначен %d" % (remote, r.remote))
        if r.state != PPOST_READY:
            die("P POST для rid=%d в состоянии %d" % (rid, r.state))
        r.state = PPOST_RUN
        dur = self.table.lookup(PREFILL_POST, r.lin)
        self.occupy(server, t, dur, "P POST %d %d" % (remote, rid), ("PPOST", rid), used)

    def read_group(self, expect_marker):
        head = self.read_tokens(2)
        marker, m = head[0], head[1]
        if expect_marker is not None and marker != str(expect_marker):
            die("ожидался маркер %s, получен %r" % (expect_marker, marker))
        m = int(m)
        if m < 1:
            die("m=%d < 1" % m)
        rids = [self.check_rid(x) for x in self.read_tokens(m)]
        if len(set(rids)) != m:
            die("дубликаты rid в группе: %r" % (rids,))
        return marker, rids

    def do_dgroup(self, t, server, used, which):
        _, rids = self.read_group(-1)
        if server != "E":
            die("%s должен идти на E, а не на %s" % (which, server))
        want = READY_D if which == "DPRE" else DPOST_READY
        for rid in rids:
            if self.reqs[rid].state != want:
                die("%s: rid=%d в состоянии %d" % (which, rid, self.reqs[rid].state))
        for rid in rids:
            self.reqs[rid].state = DPRE_RUN if which == "DPRE" else DPOST_RUN
        col = DECODE_PRE if which == "DPRE" else DECODE_POST
        dur = self.table.lookup(col, len(rids))
        word = "PRE" if which == "DPRE" else "POST"
        spec = "D %s -1 %d %s" % (word, len(rids), " ".join(map(str, rids)))
        self.occupy(server, t, dur, spec, (which, rids), used)

    def do_dproc(self, t, server, used):
        remote, rids = self.read_group(None)
        remote = int(remote)
        if server != "C%d" % remote:
            die("D PROC на %s, но remote=%d" % (server, remote))
        for rid in rids:
            r = self.reqs[rid]
            if r.remote != remote:
                die("D PROC: rid=%d назначен на %d, а не на %d" % (rid, r.remote, remote))
            if r.state != DPROC_READY:
                die("D PROC: rid=%d в состоянии %d" % (rid, r.state))
        for rid in rids:
            self.reqs[rid].state = DPROC_RUN
        dur = self.table.lookup(DECODE_PROC, len(rids))
        spec = "D PROC %d %d %s" % (remote, len(rids), " ".join(map(str, rids)))
        self.occupy(server, t, dur, spec, ("DPROC", rids, remote), used)

    # ---------------- скоринг

    def score(self):
        cfg = self.cfg
        total_tokens = sum(r.tokens for r in self.reqs)
        first_arrival = min(r.arrival for r in self.reqs)
        last_token = max((r.token_times[-1] for r in self.reqs if r.token_times), default=0.0)
        elapsed = last_token - first_arrival
        tp = total_tokens / elapsed if elapsed > 0 else 0.0

        tdr = sum(r.tdr_end - r.arrival for r in self.reqs) / len(self.reqs)
        gaps = []
        for r in self.reqs:
            for i in range(1, len(r.token_times)):
                gaps.append(r.token_times[i] - r.token_times[i - 1])
        tpot = sum(gaps) / len(gaps) if gaps else 0.0

        def clamp01(x):
            return max(0.0, min(1.0, x))

        tp_comp = clamp01((tp - cfg["tp_base"]) / (cfg["tp_UB"] - cfg["tp_base"]))
        ex_tdr = max(0.0, (tdr - cfg["SLO1"]) / cfg["SLO1"])
        ex_tpot = max(0.0, (tpot - cfg["SLO2"]) / cfg["SLO2"])
        dist = math.sqrt(ex_tdr * ex_tdr + ex_tpot * ex_tpot)
        if cfg["dist_base"] > 0:
            wait_comp = clamp01(1.0 - dist / cfg["dist_base"])
        else:
            wait_comp = 1.0 if dist == 0.0 else 0.0
        score = 1000.0 * (cfg["w_tp"] * tp_comp + cfg["w_c"] * wait_comp)

        return {
            "requests": len(self.reqs),
            "tokens": total_tokens,
            "first_arrival": first_arrival,
            "last_token": last_token,
            "tp": tp,
            "tdr": tdr,
            "tpot": tpot,
            "tp_comp": tp_comp,
            "wait_comp": wait_comp,
            "score": score,
        }


def main():
    # Всё после первого '--' — команда запуска решения; остальное разбирает argparse.
    argv = sys.argv[1:]
    if "--" in argv:
        split = argv.index("--")
        own, cmd = argv[:split], argv[split + 1 :]
    else:
        own, cmd = argv, []

    ap = argparse.ArgumentParser()
    ap.add_argument("test")
    ap.add_argument("--trace", action="store_true", help="дамп протокола в stderr")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args(own)

    if not cmd:
        sys.exit("укажите команду решения после --")

    cfg, table, reqs = read_test(args.test)
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
    )

    head = "%d %s %s %s %d %d\n%s %s %s %s %s %s %s\n" % (
        cfg["K"],
        FMT % cfg["S"],
        FMT % cfg["latency"],
        FMT % cfg["bw"],
        int(cfg["bpt"]),
        cfg["num_layers"],
        FMT % cfg["SLO1"],
        FMT % cfg["SLO2"],
        FMT % cfg["tp_UB"],
        FMT % cfg["tp_base"],
        FMT % cfg["dist_base"],
        FMT % cfg["w_tp"],
        FMT % cfg["w_c"],
    )
    inter = Interactor(cfg, table, reqs, proc, trace=args.trace, timeout=args.timeout)
    inter.send(head)
    inter.send(dump_table(args.test))

    res = inter.run()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    out = sys.stdout
    out.write("verdict ok\n")
    out.write("requests %d\n" % res["requests"])
    out.write("tokens %d\n" % res["tokens"])
    out.write("first_arrival %.6f\n" % res["first_arrival"])
    out.write("last_token %.6f\n" % res["last_token"])
    out.write("tp %.6f\n" % res["tp"])
    out.write("tdr %.6f\n" % res["tdr"])
    out.write("tpot %.6f\n" % res["tpot"])
    out.write("tp_comp %.6f\n" % res["tp_comp"])
    out.write("wait_comp %.6f\n" % res["wait_comp"])
    out.write("score %.3f\n" % res["score"])
    out.flush()


def dump_table(path):
    """N и N строк таблицы — в том же порядке, что в файле теста."""
    with open(path) as f:
        toks = []
        for line in f:
            line = line.split("#", 1)[0]
            toks.extend(line.split())
    # пропускаем 6 + 7 параметров
    idx = 13
    n = int(toks[idx])
    idx += 1
    out = [str(n)]
    for _ in range(n):
        bs = toks[idx]
        vals = toks[idx + 1 : idx + 7]
        idx += 7
        out.append(" ".join([bs] + [FMT % float(v) for v in vals]))
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    main()
