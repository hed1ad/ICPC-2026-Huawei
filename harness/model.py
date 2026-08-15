#!/usr/bin/env python3
"""Аналитическая модель системы: эталонный последовательный планировщик и потолок темпа.

Нужна для калибровки сгенерированных тестов. Судья определяет
    tp_base   — темп «фиксированного эталона по одному запросу за раз» (task.md:265),
    dist_base — насколько этот же эталон промахивается мимо SLO,
    tp_UB     — «оценка высокого темпа»,
и без этих чисел локальный балл несопоставим с логом судьи: norm_tp/norm_c
считаются именно относительно них.

Последовательный эталон детерминирован, поэтому считается в замкнутой форме, без
симуляции: ref_sched.py прогоняет тот же план через интерактор и служит проверкой,
что формулы здесь совпадают с моделью мира.
"""

import math

PREFILL_PRE, PREFILL_PROC, PREFILL_POST = 0, 1, 2
DECODE_PRE, DECODE_PROC, DECODE_POST = 3, 4, 5


def transfer_ms(cfg, length):
    return cfg["latency"] + 8.0 * length * cfg["bpt"] / (cfg["bw"] * 1e6)


def sequential_reference(cfg, table, reqs):
    """Эталон: один запрос за раз, без группировки, без перекрытий.

    Возвращает tp/tdr/tpot такого расписания. Запросы обрабатываются в порядке
    прибытия; следующий начинается только после полного завершения предыдущего.
    """
    S = cfg["S"]
    now = 0.0
    tdr_sum = 0.0
    token_times_last = 0.0
    total_tokens = 0
    gaps = []
    first_arrival = min(r[0] for r in reqs)

    for arrival, lin, lout in sorted(reqs, key=lambda x: x[0]):
        t = max(now, arrival)
        t += S + table.lookup(PREFILL_PRE, lin)
        t += transfer_ms(cfg, lin)
        t += S + table.lookup(PREFILL_PROC, lin)
        t += transfer_ms(cfg, lin)
        t += S + table.lookup(PREFILL_POST, lin)
        tdr_sum += t - arrival

        cycle = (
            S + table.lookup(DECODE_PRE, 1)
            + transfer_ms(cfg, 1)
            + S + table.lookup(DECODE_PROC, 1)
            + transfer_ms(cfg, 1)
            + S + table.lookup(DECODE_POST, 1)
        )
        for _ in range(lout):
            t += cycle
            total_tokens += 1
        if lout > 1:
            gaps.extend([cycle] * (lout - 1))
        token_times_last = t
        now = t

    elapsed = token_times_last - first_arrival
    return {
        "tp": total_tokens / elapsed if elapsed > 0 else 0.0,
        "tdr": tdr_sum / len(reqs),
        "tpot": sum(gaps) / len(gaps) if gaps else 0.0,
        "tokens": total_tokens,
        "elapsed": elapsed,
    }


def throughput_ceiling(cfg, table, reqs):
    """Оценка «высокого темпа»: обратное к загрузке узкого места в установившемся режиме.

    Для размера группы m на один токен приходится:
      E      (2S + decode_pre(m) + decode_post(m)) / m
      облако (S + decode_proc(m)) / (m*K)      — K серверов работают параллельно
      канал  (latency + 8*m*bpt/(bw*1e6)) / m  — UP и DOWN независимы, берём худший
    Плюс амортизированная по всем токенам стоимость префилла на тех же ресурсах.
    Берём m, минимизирующее узкое место.
    """
    S, K = cfg["S"], cfg["K"]
    total_tokens = sum(r[2] for r in reqs)
    if total_tokens == 0:
        return 0.0

    pre_e = sum(
        2 * S + table.lookup(PREFILL_PRE, lin) + table.lookup(PREFILL_POST, lin)
        for _, lin, _ in reqs
    )
    pre_c = sum(S + table.lookup(PREFILL_PROC, lin) for _, lin, _ in reqs) / K
    pre_link = sum(transfer_ms(cfg, lin) for _, lin, _ in reqs)

    best = float("inf")
    m = 1
    while m <= 4096:
        e = (2 * S + table.lookup(DECODE_PRE, m) + table.lookup(DECODE_POST, m)) / m
        c = (S + table.lookup(DECODE_PROC, m)) / (m * K)
        link = transfer_ms(cfg, m) / m
        bottleneck = max(
            e + pre_e / total_tokens,
            c + pre_c / total_tokens,
            link + pre_link / total_tokens,
        )
        best = min(best, bottleneck)
        m = m + 1 if m < 16 else m * 2
    return 1.0 / best


def scoring_line(cfg, table, reqs, f1, f2, w_tp, ub_slack=1.0):
    """Подобрать SLO1/SLO2/tp_UB/tp_base/dist_base под сгенерированный тест.

    f1/f2 — во сколько раз цель жёстче, чем у эталона: SLO1 = tdr_ref * f1.
    f1 = f2 = 1 даёт dist_base = 0 (особый случай: компонента ожиданий 0 или 1).
    """
    ref = sequential_reference(cfg, table, reqs)
    slo1 = max(ref["tdr"] * f1, 1e-9)
    slo2 = max(ref["tpot"] * f2, 1e-9)
    ex_tdr = max(0.0, (ref["tdr"] - slo1) / slo1)
    ex_tpot = max(0.0, (ref["tpot"] - slo2) / slo2)
    dist_base = math.sqrt(ex_tdr * ex_tdr + ex_tpot * ex_tpot)

    tp_ub = throughput_ceiling(cfg, table, reqs) * ub_slack
    tp_base = ref["tp"]
    if tp_ub <= tp_base:
        tp_ub = tp_base * 1.5 + 1e-9
    return {
        "SLO1": slo1,
        "SLO2": slo2,
        "tp_UB": tp_ub,
        "tp_base": tp_base,
        "dist_base": dist_base,
        "w_tp": w_tp,
        "w_c": 1.0 - w_tp,
        "_ref": ref,
    }
