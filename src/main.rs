//! A. Edge-Cloud Collaborative Scheduling — интерактивный планировщик.
//!
//! Весь solution живёт в одном файле (ограничение сабмита), только std.
//! Разбиение на слои:
//!   Scanner   — потоковый парсер токенов из stdin (не блокирует лишнего, EOF => exit 0).
//!   TaskTable — кусочно-линейная интерполяция таблицы времён.
//!   Sched     — состояние мира: запросы, занятость серверов, очереди готовности.
//!   Sched::decide — ЕДИНСТВЕННОЕ место принятия решений. Меняя стратегию, трогаем только его.
//!
//! Инвариант, нарушение которого = 0 баллов: сервер помечается занятым в момент вывода
//! задачи и освобождается ТОЛЬКО по её TDN. Состояние запроса меняется только по событию
//! из фрейма (или в момент нашего собственного назначения), но никогда по предсказанию времени.

use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::process::exit;

// ---------------------------------------------------------------- I/O

/// Потоковый разборщик токенов. Все поля протокола разделены пробелами/переводами строк,
/// а число токенов каждого события выводимо из его префикса, поэтому построчный разбор не нужен.
struct Scanner<R: BufRead> {
    r: R,
    buf: Vec<u8>,
}

impl<R: BufRead> Scanner<R> {
    fn new(r: R) -> Self {
        Scanner {
            r,
            buf: Vec::with_capacity(64),
        }
    }

    /// Следующий токен или None при EOF.
    fn token(&mut self) -> Option<&[u8]> {
        self.buf.clear();
        // Пропустить пробельные символы.
        loop {
            let n = {
                let av = match self.r.fill_buf() {
                    Ok(b) => b,
                    Err(_) => return None,
                };
                if av.is_empty() {
                    return None; // EOF
                }
                let mut i = 0;
                while i < av.len() && av[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i == av.len() {
                    i // весь буфер — пробелы, надо дочитать
                } else {
                    self.r.consume(i);
                    break;
                }
            };
            self.r.consume(n);
        }
        // Собрать сам токен.
        loop {
            let (took, done) = {
                let av = match self.r.fill_buf() {
                    Ok(b) => b,
                    Err(_) => break,
                };
                if av.is_empty() {
                    break;
                }
                let mut i = 0;
                while i < av.len() && !av[i].is_ascii_whitespace() {
                    i += 1;
                }
                self.buf.extend_from_slice(&av[..i]);
                (i, i < av.len())
            };
            self.r.consume(took);
            if done {
                break;
            }
        }
        Some(&self.buf)
    }

    /// Токен или немедленный выход с кодом 0 (потеря потока — не наша ошибка).
    fn tok(&mut self) -> &[u8] {
        match self.token() {
            Some(t) => t,
            None => exit(0),
        }
    }

    fn f64(&mut self) -> f64 {
        let t = self.tok();
        parse_f64(t)
    }

    fn i64(&mut self) -> i64 {
        let t = self.tok();
        parse_i64(t)
    }

    fn usize(&mut self) -> usize {
        let v = self.i64();
        if v < 0 {
            exit(0);
        }
        v as usize
    }
}

fn parse_f64(b: &[u8]) -> f64 {
    match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => exit(0),
    }
}

fn parse_i64(b: &[u8]) -> i64 {
    match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => exit(0),
    }
}

// ---------------------------------------------------------------- параметры и таблица времён

#[allow(dead_code)]
struct Config {
    k: usize,
    s: f64,
    latency_ms: f64,
    bandwidth_gbps: f64,
    bytes_per_token: f64,
    num_layers: i64,
    slo1: f64,
    slo2: f64,
    tp_ub: f64,
    tp_base: f64,
    dist_base: f64,
    w_tp: f64,
    w_c: f64,
}

impl Config {
    /// Время перевода len токенов, мс. Формула из условия; 8 переводит байты в биты.
    #[allow(dead_code)]
    fn transfer_ms(&self, len: f64) -> f64 {
        self.latency_ms + 8.0 * (len * self.bytes_per_token) / (self.bandwidth_gbps * 1e6)
    }
}

#[allow(dead_code)]
const COL_PREFILL_PRE: usize = 0;
#[allow(dead_code)]
const COL_PREFILL_PROC: usize = 1;
#[allow(dead_code)]
const COL_PREFILL_POST: usize = 2;
const COL_DECODE_PRE: usize = 3;
#[allow(dead_code)]
const COL_DECODE_PROC: usize = 4;
const COL_DECODE_POST: usize = 5;

/// Шесть колонок таблицы. В каждой — только непропущенные (-1) значения, отсортированные
/// по batch_size. Гарантия условия: каждая колонка непуста.
struct TaskTable {
    cols: [Vec<(f64, f64)>; 6],
}

impl TaskTable {
    /// Кусочно-линейная интерполяция с плоскими хвостами:
    /// ниже минимума — первое значение, выше максимума — последнее.
    fn lookup(&self, col: usize, size: f64) -> f64 {
        let c = &self.cols[col];
        if c.is_empty() {
            return 0.0;
        }
        if size <= c[0].0 {
            return c[0].1;
        }
        let last = c.len() - 1;
        if size >= c[last].0 {
            return c[last].1;
        }
        // c.len() >= 2 здесь гарантировано.
        let mut lo = 0usize;
        let mut hi = last;
        while hi - lo > 1 {
            let mid = (lo + hi) / 2;
            if c[mid].0 <= size {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let (x0, y0) = c[lo];
        let (x1, y1) = c[hi];
        if x1 == x0 {
            return y0;
        }
        y0 + (y1 - y0) * (size - x0) / (x1 - x0)
    }
}

// ---------------------------------------------------------------- состояние запроса

/// Полный жизненный цикл. Переход выполняется только по событию фрейма
/// либо в момент нашего собственного назначения задачи.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    /// ARR получен, ждём E для P PRE.
    Arrived,
    PPreRunning,
    /// P PRE завершён, данные в UP-очереди.
    UpPre,
    /// Можно запускать следующий кусок P PROC (пришёл XDN UP PRE либо TDN предыдущего куска).
    PProcReady,
    PProcRunning,
    /// Последний кусок завершён, данные в DOWN-очереди.
    DownPre,
    /// Пришёл XDN DOWN PRE, можно P POST.
    PPostReady,
    PPostRunning,
    /// Готов к следующему output-шагу (после P POST или после D POST).
    ReadyForDPre,
    DPreRunning,
    UpDec,
    DProcReady,
    DProcRunning,
    DownDec,
    DPostReady,
    DPostRunning,
    /// Пришёл FIN. Больше нигде не используется — иначе 0 баллов.
    Finished,
}

struct Req {
    #[allow(dead_code)]
    lin: f64,
    #[allow(dead_code)]
    arrival: f64,
    state: State,
    /// Назначается в P PRE и после этого неизменен.
    remote: usize,
    /// Сколько слоёв P PROC уже сделано: ls следующего куска.
    layer_cursor: i64,
    #[allow(dead_code)]
    tokens: u32,
    /// Время завершения P POST (конец TDR); -1 пока не завершён.
    #[allow(dead_code)]
    tdr_end: f64,
}

// ---------------------------------------------------------------- планировщик

struct Sched {
    cfg: Config,
    table: TaskTable,
    reqs: Vec<Req>,
    e_busy: bool,
    c_busy: Vec<bool>,
    // Очереди готовности. Хранят rid; актуальность проверяется по State при извлечении
    // (ленивое удаление), поэтому FIN в любом порядке внутри фрейма безопасен.
    q_ppre: VecDeque<usize>,
    q_pproc: Vec<VecDeque<usize>>,
    q_ppost: VecDeque<usize>,
    q_dpre: VecDeque<usize>,
    q_dproc: Vec<VecDeque<usize>>,
    q_dpost: VecDeque<usize>,
    next_remote: usize,
    /// pref_best[col][m-1] — размер группы, минимизирующий стоимость одного запроса
    /// среди всех размеров 1..=m. Строится лениво, амортизированно O(1) на новый размер:
    /// наивный перебор 1..=avail на каждом фрейме дал бы до 2·10^6 · 2000 операций.
    pref_best: Vec<Vec<u32>>,
    pref_cost: Vec<f64>,
    /// Буфер под кандидатов в группу; переиспользуется, чтобы не аллоцировать на кадр.
    scratch: Vec<usize>,
}

impl Sched {
    fn new(cfg: Config, table: TaskTable) -> Self {
        let k = cfg.k;
        Sched {
            cfg,
            table,
            reqs: Vec::new(),
            e_busy: false,
            c_busy: vec![false; k],
            q_ppre: VecDeque::new(),
            q_pproc: (0..k).map(|_| VecDeque::new()).collect(),
            q_ppost: VecDeque::new(),
            q_dpre: VecDeque::new(),
            q_dproc: (0..k).map(|_| VecDeque::new()).collect(),
            q_dpost: VecDeque::new(),
            next_remote: 0,
            pref_best: vec![Vec::new(); 6],
            pref_cost: vec![f64::INFINITY; 6],
            scratch: Vec::new(),
        }
    }

    // ---------------- обработка событий

    fn on_arr(&mut self, rid: usize, lin: f64, t: f64) {
        // rid выдаются по порядку прибытия, поэтому просто дописываем.
        while self.reqs.len() <= rid {
            self.reqs.push(Req {
                lin: 0.0,
                arrival: 0.0,
                state: State::Arrived,
                remote: usize::MAX,
                layer_cursor: 0,
                tokens: 0,
                tdr_end: -1.0,
            });
        }
        let r = &mut self.reqs[rid];
        r.lin = lin;
        r.arrival = t;
        r.state = State::Arrived;
        self.q_ppre.push_back(rid);
    }

    fn on_fin(&mut self, rid: usize) {
        self.reqs[rid].state = State::Finished;
    }

    /// TDN: освободить сервер и продвинуть состояние.
    fn on_tdn(&mut self, sc: &mut Scanner<impl BufRead>, t: f64) {
        let server = self.tok_server(sc);
        let kind = self.tok_word(sc); // P | D
        let step = self.tok_word(sc); // PRE | PROC | POST

        match server {
            Server::Local => self.e_busy = false,
            Server::Remote(k) => self.c_busy[k] = false,
        }

        match (kind, step) {
            (Word::P, Word::Pre) => {
                let _remote = sc.i64();
                let rid = sc.usize();
                let _dur = sc.f64();
                self.reqs[rid].state = State::UpPre;
            }
            (Word::P, Word::Proc) => {
                let _ls = sc.i64();
                let le = sc.i64();
                let remote = sc.usize();
                let rid = sc.usize();
                let _dur = sc.f64();
                self.reqs[rid].layer_cursor = le;
                if le >= self.cfg.num_layers {
                    // Только последний кусок поднимает DOWN-перевод.
                    self.reqs[rid].state = State::DownPre;
                } else {
                    self.reqs[rid].state = State::PProcReady;
                    self.q_pproc[remote].push_back(rid);
                }
            }
            (Word::P, Word::Post) => {
                let _remote = sc.i64();
                let rid = sc.usize();
                let _dur = sc.f64();
                let r = &mut self.reqs[rid];
                r.tdr_end = t;
                r.state = State::ReadyForDPre;
                self.q_dpre.push_back(rid);
            }
            (Word::D, Word::Pre) => {
                let _marker = sc.i64();
                let m = sc.usize();
                for _ in 0..m {
                    let rid = sc.usize();
                    self.reqs[rid].state = State::UpDec;
                }
                let _dur = sc.f64();
            }
            (Word::D, Word::Proc) => {
                let _remote = sc.usize();
                let m = sc.usize();
                for _ in 0..m {
                    let rid = sc.usize();
                    self.reqs[rid].state = State::DownDec;
                }
                let _dur = sc.f64();
            }
            (Word::D, Word::Post) => {
                let _marker = sc.i64();
                let m = sc.usize();
                for _ in 0..m {
                    let rid = sc.usize();
                    let r = &mut self.reqs[rid];
                    r.tokens += 1;
                    // FIN может прийти раньше TDN внутри одного фрейма — не воскрешаем запрос.
                    if r.state != State::Finished {
                        r.state = State::ReadyForDPre;
                        self.q_dpre.push_back(rid);
                    }
                }
                let _dur = sc.f64();
            }
            _ => exit(0),
        }
    }

    /// XDN: перевод доставлен, участники переходят в «готов».
    fn on_xdn(&mut self, sc: &mut Scanner<impl BufRead>) {
        let up = {
            let t = sc.tok();
            t == b"UP"
        };
        let remote = sc.usize();
        let _size = sc.f64();
        let pre = {
            let t = sc.tok();
            t == b"PRE"
        };
        let m = sc.usize();
        for _ in 0..m {
            let rid = sc.usize();
            match (up, pre) {
                (true, true) => {
                    self.reqs[rid].state = State::PProcReady;
                    self.q_pproc[remote].push_back(rid);
                }
                (false, true) => {
                    self.reqs[rid].state = State::PPostReady;
                    self.q_ppost.push_back(rid);
                }
                (true, false) => {
                    if self.reqs[rid].state != State::Finished {
                        self.reqs[rid].state = State::DProcReady;
                        self.q_dproc[remote].push_back(rid);
                    }
                }
                (false, false) => {
                    if self.reqs[rid].state != State::Finished {
                        self.reqs[rid].state = State::DPostReady;
                        self.q_dpost.push_back(rid);
                    }
                }
            }
        }
    }

    // ---------------- решения

    /// Достать из очереди первый rid, реально находящийся в ожидаемом состоянии.
    fn pop_ready(q: &mut VecDeque<usize>, reqs: &[Req], want: State) -> Option<usize> {
        while let Some(rid) = q.pop_front() {
            if reqs[rid].state == want {
                return Some(rid);
            }
        }
        None
    }

    /// Слить из очереди всех, кто реально находится в ожидаемом состоянии.
    /// Протухшие записи (FIN, уже запущенные) отбрасываются.
    fn drain_ready(q: &mut VecDeque<usize>, reqs: &[Req], want: State, dst: &mut Vec<usize>) {
        dst.clear();
        while let Some(rid) = q.pop_front() {
            if reqs[rid].state == want {
                dst.push(rid);
            }
        }
    }

    /// Вернуть хвост кандидатов в начало очереди, сохранив исходный порядок.
    fn put_back(q: &mut VecDeque<usize>, rest: &[usize]) {
        for &rid in rest.iter().rev() {
            q.push_front(rid);
        }
    }

    /// Размер группы, минимизирующий время локального сервера НА ОДИН запрос:
    /// (S + dur(m)) / m. Ровно тот критерий, что описан в разделе 9 конспекта:
    /// если dur(m)/m убывает — группируем агрессивно, если таблица суперлинейна — не группируем.
    /// Ждать «ещё немного, вдруг подойдут другие» нельзя: R не объявлен, затишье
    /// неотличимо от конца потока. Поэтому группируем только тех, кто готов ПРЯМО СЕЙЧАС.
    fn best_group_size(&mut self, col: usize, avail: usize) -> usize {
        while self.pref_best[col].len() < avail {
            let m = self.pref_best[col].len() + 1;
            let cost = (self.cfg.s + self.table.lookup(col, m as f64)) / m as f64;
            let best = if cost < self.pref_cost[col] - 1e-12 {
                self.pref_cost[col] = cost;
                m as u32
            } else {
                *self.pref_best[col].last().unwrap_or(&1)
            };
            self.pref_best[col].push(best);
        }
        self.pref_best[col][avail - 1] as usize
    }

    /// Собрать группу для D POST (is_post) или D PRE из готовых прямо сейчас запросов.
    /// Невзятый хвост возвращается в очередь и будет рассмотрен на следующем освобождении E.
    fn take_group(&mut self, is_post: bool) -> Option<Vec<usize>> {
        let (want, col) = if is_post {
            (State::DPostReady, COL_DECODE_POST)
        } else {
            (State::ReadyForDPre, COL_DECODE_PRE)
        };
        let mut scratch = std::mem::take(&mut self.scratch);
        {
            let q = if is_post {
                &mut self.q_dpost
            } else {
                &mut self.q_dpre
            };
            Self::drain_ready(q, &self.reqs, want, &mut scratch);
        }
        if scratch.is_empty() {
            self.scratch = scratch;
            return None;
        }
        let m = self.best_group_size(col, scratch.len());
        let group = scratch[..m].to_vec();
        {
            let q = if is_post {
                &mut self.q_dpost
            } else {
                &mut self.q_dpre
            };
            Self::put_back(q, &scratch[m..]);
        }
        self.scratch = scratch;
        Some(group)
    }

    /// Единственное место, где выбирается, что запускать.
    ///
    /// v0: группы размера 1, один полный кусок P PROC, round-robin по удалённым серверам.
    /// Приоритет на E: доделать начатое (D POST -> P POST) прежде, чем начинать новое
    /// (D PRE -> P PRE). Это главный тюнинг-ручка для TDR/TPOT.
    fn decide(&mut self, out: &mut Vec<String>) {
        // Удалённые серверы независимы: каждому можно дать по задаче в одном ответе.
        for k in 0..self.cfg.k {
            if self.c_busy[k] {
                continue;
            }
            if let Some(rid) = Self::pop_ready(&mut self.q_dproc[k], &self.reqs, State::DProcReady) {
                self.c_busy[k] = true;
                self.reqs[rid].state = State::DProcRunning;
                out.push(format!("C{} D PROC {} 1 {}", k, k, rid));
                continue;
            }
            if let Some(rid) = Self::pop_ready(&mut self.q_pproc[k], &self.reqs, State::PProcReady) {
                let ls = self.reqs[rid].layer_cursor;
                let le = self.cfg.num_layers; // v0: одним куском до конца
                self.c_busy[k] = true;
                self.reqs[rid].state = State::PProcRunning;
                out.push(format!("C{} P PROC {} {} {} {}", k, ls, le, k, rid));
            }
        }

        // Локальный сервер один — не больше одной задачи за ответ.
        if self.e_busy {
            return;
        }
        if let Some(g) = self.take_group(true) {
            self.e_busy = true;
            for &rid in &g {
                self.reqs[rid].state = State::DPostRunning;
            }
            out.push(format_group("E D POST -1", &g));
            return;
        }
        if let Some(rid) = Self::pop_ready(&mut self.q_ppost, &self.reqs, State::PPostReady) {
            let remote = self.reqs[rid].remote;
            self.e_busy = true;
            self.reqs[rid].state = State::PPostRunning;
            out.push(format!("E P POST {} {}", remote, rid));
            return;
        }
        if let Some(g) = self.take_group(false) {
            self.e_busy = true;
            for &rid in &g {
                self.reqs[rid].state = State::DPreRunning;
            }
            out.push(format_group("E D PRE -1", &g));
            return;
        }
        if let Some(rid) = Self::pop_ready(&mut self.q_ppre, &self.reqs, State::Arrived) {
            let remote = self.next_remote;
            self.next_remote = (self.next_remote + 1) % self.cfg.k;
            self.e_busy = true;
            self.reqs[rid].remote = remote;
            self.reqs[rid].state = State::PPreRunning;
            out.push(format!("E P PRE {} {}", remote, rid));
        }
    }

    // ---------------- мелкие помощники разбора

    fn tok_server(&self, sc: &mut Scanner<impl BufRead>) -> Server {
        let t = sc.tok();
        if t == b"E" {
            Server::Local
        } else if t.len() >= 2 && t[0] == b'C' {
            Server::Remote(parse_i64(&t[1..]) as usize)
        } else {
            exit(0)
        }
    }

    fn tok_word(&self, sc: &mut Scanner<impl BufRead>) -> Word {
        let t = sc.tok();
        match t {
            b"P" => Word::P,
            b"D" => Word::D,
            b"PRE" => Word::Pre,
            b"PROC" => Word::Proc,
            b"POST" => Word::Post,
            _ => exit(0),
        }
    }
}

/// "E D PRE -1" + [4,7] -> "E D PRE -1 2 4 7"
fn format_group(prefix: &str, g: &[usize]) -> String {
    let mut s = String::with_capacity(prefix.len() + 8 * (g.len() + 1));
    s.push_str(prefix);
    s.push(' ');
    s.push_str(&g.len().to_string());
    for rid in g {
        s.push(' ');
        s.push_str(&rid.to_string());
    }
    s
}

#[derive(Clone, Copy)]
enum Server {
    Local,
    Remote(usize),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Word {
    P,
    D,
    Pre,
    Proc,
    Post,
}

// ---------------------------------------------------------------- main

fn main() {
    let stdin = io::stdin();
    let mut sc = Scanner::new(BufReader::with_capacity(1 << 16, stdin.lock()));
    let stdout = io::stdout();
    let mut out = BufWriter::with_capacity(1 << 16, stdout.lock());

    // Строка 1: системные параметры.
    let cfg = Config {
        k: sc.usize(),
        s: sc.f64(),
        latency_ms: sc.f64(),
        bandwidth_gbps: sc.f64(),
        bytes_per_token: sc.f64(),
        num_layers: sc.i64(),
        // Строка 2: параметры скоринга.
        slo1: sc.f64(),
        slo2: sc.f64(),
        tp_ub: sc.f64(),
        tp_base: sc.f64(),
        dist_base: sc.f64(),
        w_tp: sc.f64(),
        w_c: sc.f64(),
    };

    // Таблица времён: N строк по 7 значений, -1 = отсутствует.
    let n = sc.usize();
    let mut cols: [Vec<(f64, f64)>; 6] = Default::default();
    for _ in 0..n {
        let bs = sc.f64();
        for c in 0..6 {
            let v = sc.f64();
            if v >= 0.0 {
                cols[c].push((bs, v));
            }
        }
    }
    for c in cols.iter_mut() {
        c.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }
    let table = TaskTable { cols };
    let _ = (
        COL_PREFILL_PRE,
        COL_PREFILL_PROC,
        COL_PREFILL_POST,
        COL_DECODE_PRE,
        COL_DECODE_PROC,
        COL_DECODE_POST,
    );

    let mut sched = Sched::new(cfg, table);
    let mut asg: Vec<String> = Vec::with_capacity(16);

    loop {
        // Заголовок фрейма: либо END, либо временная метка.
        let t = {
            let head = sc.tok();
            if head == b"END" {
                break;
            }
            parse_f64(head)
        };
        let e = sc.usize();
        for _ in 0..e {
            let ev = {
                let w = sc.tok();
                if w == b"ARR" {
                    0
                } else if w == b"TDN" {
                    1
                } else if w == b"XDN" {
                    2
                } else if w == b"FIN" {
                    3
                } else {
                    exit(0)
                }
            };
            match ev {
                0 => {
                    let rid = sc.usize();
                    let lin = sc.f64();
                    sched.on_arr(rid, lin, t);
                }
                1 => sched.on_tdn(&mut sc, t),
                2 => sched.on_xdn(&mut sc),
                _ => {
                    let rid = sc.usize();
                    sched.on_fin(rid);
                }
            }
        }

        asg.clear();
        sched.decide(&mut asg);

        let mut buf = String::with_capacity(64 * (asg.len() + 1));
        buf.push_str(&asg.len().to_string());
        buf.push('\n');
        for a in &asg {
            buf.push_str(a);
            buf.push('\n');
        }
        if out.write_all(buf.as_bytes()).is_err() {
            exit(0);
        }
        if out.flush().is_err() {
            exit(0);
        }
    }

    exit(0);
}
