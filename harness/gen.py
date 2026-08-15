#!/usr/bin/env python3
"""Генератор локальных тестов, покрывающих режимы из лога судьи.

Судейские тесты недоступны (итерация = сабмит), поэтому воспроизводим их режимы:
  burst_*     — всплеск прибытий, система перегружена (ср. #16/#21 в логе)
  fast_*      — суб-миллисекундные длительности при большом S: узкое место — накладные
                расходы на задачу (ср. #5/#6/#22, где norm_tp низкий при высоком tp)
  slowlink_*  — медленный канал, узкое место — FIFO UP/DOWN (ср. #11/#15/#17/#18,
                где mean_tdr исчисляется миллионами)
  spread_*    — недогруженная система, прибытия растянуты (ср. #2/#20)
  holes       — таблица, где большинство ячеек -1
  k1nl64      — K=1 и 64 слоя: единственный remote, есть что резать в P PROC
  stress      — потолок ограничений (R=2000, сумма Lout ~2e5) для проверки TL/ML

Параметры скоринга (SLO1/SLO2/tp_UB/tp_base/dist_base) считает model.py по
эталонному последовательному расписанию — так локальные norm_tp/norm_c сопоставимы
с теми, что печатает судья.

    python3 harness/gen.py            # записать всё в tests/gen/
    python3 harness/gen.py --stress   # включить и тяжёлый тест
"""

import argparse
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from interactor import TaskTable  # noqa: E402
from model import scoring_line  # noqa: E402

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests", "gen")

SIZES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]


def clamp_val(v):
    return max(0.001, min(10000.0, v))


def make_table(cols, sizes=SIZES, holes=None):
    """cols — 6 функций size -> ms. holes[c] — множество индексов размеров, где ставим -1."""
    holes = holes or {}
    rows = []
    for i, bs in enumerate(sizes):
        vals = []
        for c in range(6):
            if i in holes.get(c, ()):
                vals.append(-1.0)
            else:
                vals.append(clamp_val(cols[c](bs)))
        rows.append((bs, vals))
    return rows


def power(a, alpha):
    return lambda x: a * (x ** alpha)


def write_test(name, cfg_sys, rows, reqs, f1, f2, w_tp, comment=""):
    table = TaskTable(rows)
    cfg = dict(cfg_sys)
    sc = scoring_line(cfg, table, reqs, f1, f2, w_tp)
    ref = sc.pop("_ref")

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name + ".txt")
    with open(path, "w") as f:
        if comment:
            f.write("# %s\n" % comment)
        f.write(
            "# эталон: tp=%.6g tdr=%.6g tpot=%.6g tokens=%d\n"
            % (ref["tp"], ref["tdr"], ref["tpot"], ref["tokens"])
        )
        f.write(
            "%d %.9f %.9f %.9f %d %d\n"
            % (cfg["K"], cfg["S"], cfg["latency"], cfg["bw"], int(cfg["bpt"]), cfg["num_layers"])
        )
        f.write(
            "%.9f %.9f %.9f %.9f %.9f %.9f %.9f\n"
            % (sc["SLO1"], sc["SLO2"], sc["tp_UB"], sc["tp_base"], sc["dist_base"],
               sc["w_tp"], sc["w_c"])
        )
        f.write("%d\n" % len(rows))
        for bs, vals in rows:
            f.write("%d %s\n" % (bs, " ".join("%.9f" % v for v in vals)))
        f.write("%d\n" % len(reqs))
        f.write("# arrival Lin Lout\n")
        for a, lin, lout in reqs:
            f.write("%.9f %d %d\n" % (a, lin, lout))
    return path, ref


# ------------------------------------------------------------------ семейства


def burst(rng, n, lin_range, lout_range):
    return [(0.0, rng.randint(*lin_range), rng.randint(*lout_range)) for _ in range(n)]


def poisson_arrivals(rng, n, rate_ms, lin_range, lout_range):
    reqs, t = [], 0.0
    for _ in range(n):
        t += rng.expovariate(rate_ms)
        reqs.append((t, rng.randint(*lin_range), rng.randint(*lout_range)))
    return reqs


def build(args):
    rng = random.Random(20260815)
    made = []

    sublinear = [
        power(0.6, 0.55), power(4.0, 0.85), power(0.5, 0.55),   # prefill pre/proc/post
        power(0.8, 0.35), power(3.0, 0.45), power(0.8, 0.35),   # decode pre/proc/post
    ]
    superlinear = [
        power(0.6, 0.55), power(4.0, 0.85), power(0.5, 0.55),
        power(0.9, 1.25), power(3.0, 1.15), power(0.9, 1.25),
    ]
    tiny = [
        power(0.02, 0.5), power(0.08, 0.7), power(0.02, 0.5),
        power(0.01, 0.3), power(0.03, 0.4), power(0.01, 0.3),
    ]

    # --- всплеск, сублинейная таблица: группировка выгодна, конвейер обязателен
    for w in (1.0, 0.5, 0.0):
        made.append(write_test(
            "burst_sub_w%02d" % int(w * 10),
            {"K": 8, "S": 2.0, "latency": 1.0, "bw": 4.0, "bpt": 65536, "num_layers": 8},
            make_table(sublinear),
            burst(rng, 300, (64, 1024), (1, 40)),
            f1=0.15, f2=0.6, w_tp=w,
            comment="всплеск 300 запросов, сублинейный декод, K=8",
        ))

    # --- всплеск, суперлинейная таблица: большие группы вредны
    made.append(write_test(
        "burst_super_w05",
        {"K": 8, "S": 2.0, "latency": 1.0, "bw": 4.0, "bpt": 65536, "num_layers": 8},
        make_table(superlinear),
        burst(rng, 200, (64, 1024), (1, 30)),
        f1=0.15, f2=0.6, w_tp=0.5,
        comment="всплеск, СУПЕРлинейный декод: размер группы должен схлопнуться к 1",
    ))

    # --- крошечные длительности при большом S: правит накладная стоимость задачи
    for w in (0.9, 0.5):
        made.append(write_test(
            "fast_w%02d" % int(w * 10),
            {"K": 4, "S": 10.0, "latency": 0.5, "bw": 10.0, "bpt": 4096, "num_layers": 4},
            make_table(tiny),
            burst(rng, 250, (32, 512), (2, 60)),
            f1=0.2, f2=0.5, w_tp=w,
            comment="S=10 при суб-миллисекундных задачах: узкое место — сама задача",
        ))

    # --- декод-режим: префилл дёшев (Lin мал), почти всё время — оборот декода.
    # Именно здесь работает конвейер волн; без таких тестов набор слеп к нему.
    decode_bound = [
        power(0.3, 0.5), power(0.5, 0.7), power(0.3, 0.5),      # дешёвый префилл
        power(0.8, 0.35), power(3.0, 0.45), power(0.8, 0.35),
    ]
    for w, k in ((1.0, 8), (0.5, 8), (0.0, 8), (0.5, 2)):
        made.append(write_test(
            "decode_w%02d_k%d" % (int(w * 10), k),
            {"K": k, "S": 2.0, "latency": 1.0, "bw": 8.0, "bpt": 16384, "num_layers": 8},
            make_table(decode_bound),
            burst(rng, 120, (16, 64), (100, 512)),
            f1=0.4, f2=0.5, w_tp=w,
            comment="Lin мал, Lout велик: узкое место — сам декод-оборот, K=%d" % k,
        ))

    # --- медленный канал: узкое место — FIFO переводы
    for w in (0.5, 0.0):
        made.append(write_test(
            "slowlink_w%02d" % int(w * 10),
            {"K": 4, "S": 1.0, "latency": 20.0, "bw": 0.02, "bpt": 65536, "num_layers": 8},
            make_table(sublinear),
            burst(rng, 40, (512, 4096), (2, 20)),
            f1=0.1, f2=0.4, w_tp=w,
            comment="bw=0.02 Gb/s: один prefill-перевод дольше всех вычислений",
        ))

    # --- недогруженная система, редкие прибытия
    made.append(write_test(
        "spread_single_w05",
        {"K": 2, "S": 3.0, "latency": 2.0, "bw": 2.0, "bpt": 32768, "num_layers": 4},
        make_table(sublinear),
        poisson_arrivals(rng, 60, 1 / 400.0, (128, 1024), (1, 1)),
        f1=0.9, f2=1.0, w_tp=0.5,
        comment="прибытия раз в ~400 мс, Lout=1 у всех: tpot=0, группировать нечего",
    ))
    made.append(write_test(
        "spread_multi_w03",
        {"K": 4, "S": 2.0, "latency": 2.0, "bw": 2.0, "bpt": 32768, "num_layers": 8},
        make_table(sublinear),
        poisson_arrivals(rng, 80, 1 / 120.0, (128, 2048), (4, 60)),
        f1=0.5, f2=0.7, w_tp=0.3,
        comment="умеренная нагрузка, вес на ожиданиях",
    ))

    # --- dist_base = 0: компонента ожиданий строго 0 или 1
    made.append(write_test(
        "distbase0",
        {"K": 4, "S": 2.0, "latency": 1.0, "bw": 4.0, "bpt": 32768, "num_layers": 4},
        make_table(sublinear),
        burst(rng, 30, (64, 512), (1, 10)),
        f1=1.0, f2=1.0, w_tp=0.5,
        comment="f1=f2=1 => dist_base=0, ожидания дают 0 или 1",
    ))

    # --- дырявая таблица: почти везде -1
    holes = {c: set(range(len(SIZES))) - {c % 3, (c * 3 + 5) % len(SIZES), len(SIZES) - 1}
             for c in range(6)}
    made.append(write_test(
        "holes",
        {"K": 4, "S": 2.0, "latency": 1.0, "bw": 4.0, "bpt": 65536, "num_layers": 8},
        make_table(sublinear, holes=holes),
        burst(rng, 150, (64, 2048), (2, 30)),
        f1=0.2, f2=0.6, w_tp=0.5,
        comment="в каждой колонке всего 3 непропущенных значения, остальное -1",
    ))

    # --- K=1, 64 слоя: единственный remote, длинный P PROC
    made.append(write_test(
        "k1nl64",
        {"K": 1, "S": 1.0, "latency": 1.0, "bw": 8.0, "bpt": 16384, "num_layers": 64},
        make_table([
            power(0.5, 0.5), power(30.0, 0.9), power(0.5, 0.5),
            power(0.5, 0.4), power(2.0, 0.5), power(0.5, 0.4),
        ]),
        burst(rng, 60, (256, 4096), (2, 25)),
        f1=0.2, f2=0.5, w_tp=0.6,
        comment="K=1, num_layers=64, огромный prefill_proc: есть смысл резать P PROC",
    ))

    if args.stress:
        reqs = []
        for _ in range(2000):
            reqs.append((rng.uniform(0, 5000.0), rng.randint(64, 4096), rng.randint(1, 200)))
        made.append(write_test(
            "stress",
            {"K": 8, "S": 1.0, "latency": 0.5, "bw": 20.0, "bpt": 8192, "num_layers": 16},
            make_table(sublinear),
            reqs,
            f1=0.2, f2=0.6, w_tp=0.5,
            comment="потолок ограничений: R=2000, сумма Lout ~2e5 — проверка TL/ML",
        ))

    return made


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stress", action="store_true", help="добавить тяжёлый тест на TL/ML")
    args = ap.parse_args()
    for path, ref in build(args):
        print("%-24s эталон tp=%.6g tdr=%.6g tpot=%.6g tokens=%d"
              % (os.path.basename(path), ref["tp"], ref["tdr"], ref["tpot"], ref["tokens"]))


if __name__ == "__main__":
    main()
