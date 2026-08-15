// A. Edge-Cloud Collaborative Scheduling — планировщик v7.
//
// Что здесь важно понимать про устройство задачи (всё замерено на harness/bench.py,
// не выведено на бумаге — бумажные выкладки в этой задаче систематически врут):
//
// 1. Узкое место бывает трёх видов, и они требуют разного: E (локальный сервер),
//    облако (P PROC там же, где D PROC) и FIFO-каналы UP/DOWN. Интерактор печатает
//    util_E / util_C_max / util_UP — с них и надо начинать разбор любого теста.
// 2. Стоимость на токен на E — это (S + dpre(a))/a + (S + dpost(b))/b, где a и b —
//    размеры групп. Значит, всё решают размеры групп, а не «умный» порядок.
// 3. Группы НЕЛЬЗЯ увеличивать ожиданием: волны расслаиваются в конвейер сами, только
//    если запускать готовое немедленно. Пока E ждёт недостающих, они физически не
//    могут прийти раньше полного оборота (замер: 6164 -> 5799).
// 4. Волна с одного сервера даёт неразрезанный D POST, но она размером pool/K, и это
//    перевешивает (замер: 6300 -> 5684). Концентрировать пул на части серверов тоже
//    нельзя: на них лежит и P PROC, а он на многих тестах и есть узкое место.
//
// Поэтому от v6 отличают три вещи, каждая с замером:
//   -1 в таблице = «значения нет для ЭТОЙ колонки» и не участвует в интерполяции
//      (v6 интерполировал сквозь пропуски; на оценках это давало мусор);
//   размер группы для D PRE/D POST/D PROC берётся по критерию (S + dur(g))/g, поэтому
//      на суперлинейной таблице группы схлопываются к 1 (+94 на burst_super);
//   порядок на E — по заполненности пула и градиенту dist, а не FCFS (+112).

#include <bits/stdc++.h>
using namespace std;

enum {
    NEED_PPRE = 0,
    RUN_PPRE,
    WAIT_UP_PRE,
    NEED_PPROC,
    RUN_PPROC,
    WAIT_DOWN_PRE,
    NEED_PPOST,
    RUN_PPOST,
    NEED_DPRE,
    RUN_DPRE,
    WAIT_UP_DEC,
    NEED_DPROC,
    RUN_DPROC,
    WAIT_DOWN_DEC,
    NEED_DPOST,
    RUN_DPOST,
    DONE
};

// ------------------------------------------------------------------ ввод

static inline bool nextTok(const string &s, size_t &p, size_t &b, size_t &e) {
    while (p < s.size() && (s[p] == ' ' || s[p] == '\t' || s[p] == '\r' || s[p] == '\n')) p++;
    if (p >= s.size()) return false;
    b = p;
    while (p < s.size() && s[p] != ' ' && s[p] != '\t' && s[p] != '\r' && s[p] != '\n') p++;
    e = p;
    return true;
}

static inline long long toInt(const string &s, size_t b, size_t e) {
    bool neg = false;
    if (b < e && (s[b] == '-' || s[b] == '+')) { neg = (s[b] == '-'); b++; }
    long long v = 0;
    for (size_t i = b; i < e; i++) {
        char c = s[i];
        if (c < '0' || c > '9') break;
        v = v * 10 + (c - '0');
    }
    return neg ? -v : v;
}

static inline bool eqTok(const string &s, size_t b, size_t e, const char *w) {
    size_t n = e - b;
    for (size_t i = 0; i < n; i++) {
        if (w[i] == '\0') return false;
        if (s[b + i] != w[i]) return false;
    }
    return w[n] == '\0';
}

static void appendInt(string &s, long long v) {
    char buf[24];
    int len = 0;
    if (v == 0) buf[len++] = '0';
    bool neg = v < 0;
    unsigned long long u = neg ? (unsigned long long)(-v) : (unsigned long long)v;
    while (u) { buf[len++] = char('0' + (u % 10)); u /= 10; }
    if (neg) buf[len++] = '-';
    while (len) s.push_back(buf[--len]);
}

// ------------------------------------------------------------------ таблица времён

// -1 означает «значения нет ДЛЯ ЭТОЙ КОЛОНКИ» (task.md), поэтому колонки хранятся
// отдельно: пропуски нельзя тащить в интерполяцию.
enum { C_PPRE = 0, C_PPROC, C_PPOST, C_DPRE, C_DPROC, C_DPOST };

struct Column {
    vector<double> xs, ys;

    double at(double x) const {
        if (xs.empty()) return 0.0;
        if (x <= xs.front()) return ys.front();
        if (x >= xs.back()) return ys.back();
        int lo = 0, hi = (int)xs.size() - 1;
        while (hi - lo > 1) {
            int md = (lo + hi) >> 1;
            if (xs[md] <= x) lo = md; else hi = md;
        }
        double x0 = xs[lo], x1 = xs[hi];
        if (x1 <= x0) return ys[lo];
        return ys[lo] + (ys[hi] - ys[lo]) * (x - x0) / (x1 - x0);
    }
};

// ------------------------------------------------------------------ параметры и модель

int K, numLayers;
double S, lat, bw, bpt;
double slo1, slo2, tpUB, tpBase, distBase, wtp, wc;
Column col[6];

static inline double xferMs(double len) { return lat + 8.0 * len * bpt / (bw * 1e6); }

// Занятость E волной размера m: две задачи (D PRE и D POST) вместе со стоимостью S.
static inline double eWave(double m) { return 2.0 * S + col[C_DPRE].at(m) + col[C_DPOST].at(m); }

// Наблюдаемая стоимость префилла на токен. Считается по факту, а не по догадке о
// будущем: без неё модель видит только декод и concentrates пул на паре серверов
// даже там, где узкое место — P PROC на облаке.
double prefE = 0.0, prefC = 0.0, prefL = 0.0;

static inline double perTokenAt(double p, int ruse) {
    double e = eWave(p) / p + prefE;
    double link = (lat + 8.0 * p * bpt / (bw * 1e6)) / p + prefL;
    double cloud = ((S + col[C_DPROC].at(p)) / p + prefC) / ruse;
    return max(e, max(link, cloud));
}

static inline double rttWave(double p) {
    double tr = lat + 8.0 * p * bpt / (bw * 1e6);
    return (S + col[C_DPRE].at(p)) + tr + (S + col[C_DPROC].at(p)) + tr
           + (S + col[C_DPOST].at(p));
}

static inline double perToken(double m) { return perTokenAt(m, K); }

struct Wave { int m; int ruse; double T; double per; };


// Оценка установившегося оборота: T = max(pool * стоимость_на_токен, латентность
// круга). Даёт цель по размеру группы (chunkTargetMs) и оценку TPOT для политики.
static Wave computeWave(int avail) {
    if (avail < 1) avail = 1;
    Wave best{avail, K, 1e300, 1e300};
    for (int m = 1; m <= avail; m = (m < 16 ? m + 1 : (int)(m * 1.35) + 1)) {
        int mm = min(m, 4096);
        double per = perToken(mm);
        double T = max(avail * per, rttWave(mm));
        if (T < best.T) best = Wave{mm, K, T, per};
        if (mm >= 4096) break;
    }
    return best;
}

// Сколько запросов нужно держать в декод-обороте. Размер волны ограничен пулом, а
// стоимость на токен — (2S + dpre(m) + dpost(m))/m, поэтому пул растит tp до тех пор,
// пока стоимость на токен падает. Целимся в m_flat (где она перестаёт падать) и в
// насыщение конвейера rtt/perToken; дальше пул только раздувает TPOT.
static int computePoolTarget() {
    double bestPer = 1e300;
    int bestM = 1;
    for (int m = 1; m <= 4096; m = (m < 16 ? m + 1 : (int)(m * 1.35) + 1)) {
        double per = perToken(m);
        if (per < bestPer * 0.999) { bestPer = per; bestM = m; }
    }
    double sat = bestPer > 0 ? rttWave(bestM) / bestPer : bestM;
    return max(bestM, (int)ceil(sat));
}

static Wave cachedWave{1, 1, 0, 0};
static int cachedFor = -1;

static Wave waveFor(int avail) {
    if (avail < 0) avail = 0;
    if (avail != cachedFor) { cachedWave = computeWave(avail); cachedFor = avail; }
    return cachedWave;
}

// ------------------------------------------------------------------ состояние

struct Req {
    int stage = DONE;
    int remote = -1;
    int Lin = 1;
    int cursor = 0;          // сколько слоёв P PROC уже сделано
    long long seq = 0;
    double arrival = 0.0;
    double lastToken = -1.0;
    bool alive = false;
    bool prefilled = false;
};

vector<Req> req;
long long seqCounter = 0;

deque<int> qPPRE, qPPOST, qDPOST;
vector<deque<int>> qPPROC, qDPRE, qDPROC;   // per-remote

vector<char> cloudBusy;
bool edgeBusy = false;
vector<int> nDpre;       // готовых к D PRE на этом remote (счётчик, а не проход по деку)
int nDpost = 0;          // готовых к D POST (тот же счётчик вместо прохода по деку)
vector<int> assignedOn;  // сколько всего живых запросов назначено на remote
vector<double> cloudWork; // суммарная занятость remote — оценка его загрузки

int pool = 0;            // запросов в декод-обороте (после P POST, ещё не FIN)
int liveCount = 0;       // прибыли и ещё не финишировали — горизонт планирования
int inFlight = 0;        // задач/переводов в полёте: гарантия будущего события
int pendPrefill = 0;     // прибыли, но ещё не дошли до конца P POST
double pendArrivalSum = 0.0;
double tdrSum = 0.0;
long long tdrCount = 0;
double tpotSum = 0.0;
long long tpotCount = 0;
double now = 0.0;

static void ensureReq(int rid) {
    if ((int)req.size() <= rid) req.resize(rid + 1);
}

static inline void clean(deque<int> &q, int expected) {
    while (!q.empty()) {
        int r = q.front();
        if (r >= 0 && r < (int)req.size() && req[r].alive && req[r].stage == expected) break;
        q.pop_front();
    }
}

static inline void push(deque<int> &q, int rid, int stage) {
    req[rid].stage = stage;
    req[rid].seq = ++seqCounter;
    q.push_back(rid);
}

// Снять с очереди готовых запросов, пока в out не станет cap штук (ленивое удаление
// по stage/alive). counter — счётчик готовых, если очередь его ведёт.
static void take(deque<int> &q, int expected, int cap, vector<int> &out, int *counter = nullptr) {
    while ((int)out.size() < cap) {
        clean(q, expected);
        if (q.empty()) break;
        out.push_back(q.front());
        q.pop_front();
        if (counter) (*counter)--;
    }
}

// ------------------------------------------------------------------ политика

// Оценка средней TDR с учётом ещё не префилленных запросов: без этого слагаемого
// показатель не растёт, пока префилл голодает, и обратная связь не работает.
static double tdrEstimate() {
    double pending = pendPrefill * now - pendArrivalSum;
    long long n = tdrCount + pendPrefill;
    if (n <= 0) return 0.0;
    return (tdrSum + max(0.0, pending)) / (double)n;
}

static double tpotEstimate() {
    return tpotCount ? tpotSum / (double)tpotCount : 0.0;
}

int poolTarget = 1;
int chunkDpre = 4096, chunkDpost = 4096, chunkDproc = 4096;
double chunkTargetMs = 0.0;   // цель длительности куска P PROC; 0 = не резать
double prefWorkE = 0.0, prefWorkC = 0.0, prefWorkL = 0.0;  // фактическая работа префилла
long long tokensDone = 0;

// Размер волны решает, СКОЛЬКО запросов запускать в оборот (D PRE). Возвраты
// (D POST, D PROC) — это уже проделанная работа, её надо лишь разложить по задачам
// оптимально: минимизируем (S + dur(g))/g. На сублинейной колонке ответ — «всё
// сразу», на суперлинейной — мелкие куски.
static int bestChunk(int c) {
    double best = 1e300;
    int bestG = 1;
    for (int g = 1; g <= 4096; g = (g < 16 ? g + 1 : (int)(g * 1.35) + 1)) {
        double v = (S + col[c].at(g)) / g;
        if (v < best * 0.999) { best = v; bestG = g; }
    }
    return bestG;
}

// Префилл вперёд декода? Пока пул меньше целевого — да: пул кормит волны, и пока
// стоимость на токен падает с ростом m, префилл работает и на tp, и на TDR.
// Дальше решает градиент по dist: подтягиваем ту компоненту, что хуже.
int policy = 1;   // 0 = FCFS по seq (как v6), 1 = приоритет по пулу и градиенту dist

static bool prefillFirst() {
    if (policy == 0) return false;
    if (pool < poolTarget) return true;
    if (wc <= 0.0) return false;
    double exTdr = max(0.0, (tdrEstimate() - slo1) / slo1);
    double exTpot = max(0.0, (tpotEstimate() - slo2) / slo2);
    return exTdr > exTpot;
}

// ------------------------------------------------------------------ main

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    // Диагностика только в stderr: любой лишний байт в stdout = 0 баллов.
    // Ожидание ради полной волны ЗАМЕРЕНО КАК ВРЕДНОЕ (6164 -> 5799 на локальном
    // наборе) и по умолчанию выключено: волны расслаиваются в конвейер сами, только
    // если запускать готовое сразу. Пока E ждёт недостающих, они физически не могут
    // прийти раньше полного оборота — ожидание схлопывает конвейер в одну волну.
    // Переключатели оставлены, чтобы перепроверить на других семействах.
    const char *envPre = getenv("SCHED_WAIT_PRE");
    const char *envPost = getenv("SCHED_WAIT_POST");
    int waitPre = envPre ? atoi(envPre) : 0;
    int waitPost = envPost ? atoi(envPost) : 0;
    const char *envPol = getenv("SCHED_POLICY");
    policy = envPol ? atoi(envPol) : 1;
    const char *dbg = getenv("SCHED_DEBUG");
    long long dbgFrame = 0;
    long long dbgEvery = dbg ? max(1L, atol(dbg)) : 1;

    if (!(cin >> K >> S >> lat >> bw >> bpt >> numLayers)) return 0;
    cin >> slo1 >> slo2 >> tpUB >> tpBase >> distBase >> wtp >> wc;
    if (slo1 <= 0) slo1 = 1e-9;
    if (slo2 <= 0) slo2 = 1e-9;

    int N;
    if (!(cin >> N)) return 0;
    {
        vector<pair<double, array<double, 6>>> rows(N);
        for (int i = 0; i < N; i++) {
            cin >> rows[i].first;
            for (int c = 0; c < 6; c++) cin >> rows[i].second[c];
        }
        sort(rows.begin(), rows.end(),
             [](const pair<double, array<double, 6>> &a, const pair<double, array<double, 6>> &b) {
                 return a.first < b.first;
             });
        for (auto &r : rows)
            for (int c = 0; c < 6; c++)
                if (r.second[c] >= 0.0) {          // -1 = значения для этой колонки нет
                    col[c].xs.push_back(r.first);
                    col[c].ys.push_back(r.second[c]);
                }
    }

    string line;
    getline(cin, line);

    qPPROC.assign(K, {});
    qDPRE.assign(K, {});
    qDPROC.assign(K, {});
    cloudBusy.assign(K, 0);
    nDpre.assign(K, 0);
    assignedOn.assign(K, 0);
    cloudWork.assign(K, 0.0);
    poolTarget = computePoolTarget();
    chunkDpre = bestChunk(C_DPRE);
    chunkDpost = bestChunk(C_DPOST);
    chunkDproc = bestChunk(C_DPROC);
    req.reserve(4096);

    vector<int> group;
    vector<string> cmds;
    string out;
    out.reserve(1 << 16);

    long long frameNo = 0;
    while (getline(cin, line)) {
        frameNo++;
        {
            size_t p = 0, b, e;
            if (!nextTok(line, p, b, e)) continue;
            if (eqTok(line, b, e, "END")) break;
            now = atof(line.c_str() + b);
        }

        if (!getline(cin, line)) break;
        long long ecount;
        {
            size_t p = 0, b, e;
            if (!nextTok(line, p, b, e)) break;
            ecount = toInt(line, b, e);
        }

        for (long long ie = 0; ie < ecount; ie++) {
            if (!getline(cin, line)) return 0;
            size_t p = 0, b, e;
            if (!nextTok(line, p, b, e)) { ie--; continue; }

            if (eqTok(line, b, e, "ARR")) {
                nextTok(line, p, b, e);
                int rid = (int)toInt(line, b, e);
                nextTok(line, p, b, e);
                int lin = (int)toInt(line, b, e);
                ensureReq(rid);
                req[rid].alive = true;
                req[rid].remote = -1;
                req[rid].Lin = lin;
                req[rid].arrival = now;
                req[rid].lastToken = -1.0;
                req[rid].prefilled = false;
                pendPrefill++;
                liveCount++;
                pendArrivalSum += now;
                push(qPPRE, rid, NEED_PPRE);
            } else if (eqTok(line, b, e, "FIN")) {
                nextTok(line, p, b, e);
                int rid = (int)toInt(line, b, e);
                if (rid >= 0 && rid < (int)req.size() && req[rid].alive) {
                    if (req[rid].remote >= 0) {
                        if (req[rid].stage == NEED_DPRE) nDpre[req[rid].remote]--;
                        assignedOn[req[rid].remote]--;
                    }
                    pool--;
                    liveCount--;
                    req[rid].stage = DONE;
                    req[rid].alive = false;
                }
            } else if (eqTok(line, b, e, "TDN")) {
                if (inFlight > 0) inFlight--;
                nextTok(line, p, b, e);
                bool isEdge = eqTok(line, b, e, "E");
                int ck = -1;
                if (!isEdge) ck = (int)toInt(line, b + 1, e);
                if (isEdge) edgeBusy = false; else if (ck >= 0 && ck < K) cloudBusy[ck] = 0;

                nextTok(line, p, b, e);
                bool isP = eqTok(line, b, e, "P");
                nextTok(line, p, b, e);
                bool isPRE = eqTok(line, b, e, "PRE");
                bool isPROC = eqTok(line, b, e, "PROC");

                if (isP) {
                    if (isPRE) {
                        nextTok(line, p, b, e);
                        int rem = (int)toInt(line, b, e);
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < (int)req.size() && req[rid].alive) {
                            req[rid].remote = rem;
                            req[rid].stage = WAIT_UP_PRE;
                            inFlight++;                     // поехал UP-перевод
                        }
                    } else if (isPROC) {
                        nextTok(line, p, b, e);                       // ls
                        nextTok(line, p, b, e);
                        int le = (int)toInt(line, b, e);
                        nextTok(line, p, b, e);
                        int rem = (int)toInt(line, b, e);
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < (int)req.size() && req[rid].alive) {
                            req[rid].cursor = le;
                            if (le >= numLayers) {
                                req[rid].stage = WAIT_DOWN_PRE;       // поехал DOWN
                                inFlight++;
                            } else {
                                // Кусок не последний: перевода нет, следующий кусок
                                // ждёт только этот TDN — возвращаем в очередь remote.
                                push(qPPROC[max(0, rem)], rid, NEED_PPROC);
                            }
                        }
                    } else {
                        nextTok(line, p, b, e);
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < (int)req.size() && req[rid].alive) {
                            // Конец P POST — это конец TDR.
                            req[rid].prefilled = true;
                            tdrSum += now - req[rid].arrival;
                            tdrCount++;
                            pendPrefill--;
                            pendArrivalSum -= req[rid].arrival;
                            pool++;
                            int k = max(0, req[rid].remote);
                            nDpre[k]++;
                            push(qDPRE[k], rid, NEED_DPRE);
                        }
                    }
                } else {
                    nextTok(line, p, b, e);
                    nextTok(line, p, b, e);
                    int m = (int)toInt(line, b, e);
                    unsigned remoteMask = 0;
                    for (int i = 0; i < m; i++) {
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < 0 || rid >= (int)req.size() || !req[rid].alive) continue;
                        if (isPRE) {
                            req[rid].stage = WAIT_UP_DEC;
                            if (req[rid].remote >= 0) remoteMask |= 1u << req[rid].remote;
                        } else if (isPROC) {
                            req[rid].stage = WAIT_DOWN_DEC;
                        } else {
                            if (req[rid].lastToken >= 0.0) {
                                tpotSum += now - req[rid].lastToken;
                                tpotCount++;
                            }
                            req[rid].lastToken = now;
                            tokensDone++;
                            int k = max(0, req[rid].remote);
                            nDpre[k]++;
                            push(qDPRE[k], rid, NEED_DPRE);
                        }
                    }
                    // Группа D PRE поднимает по переводу на КАЖДЫЙ задействованный
                    // remote, D PROC — ровно один. Иначе счётчик в полёте уплывёт.
                    if (isPRE) inFlight += __builtin_popcount(remoteMask);
                    else if (isPROC) inFlight++;
                }
            } else if (eqTok(line, b, e, "XDN")) {
                if (inFlight > 0) inFlight--;
                nextTok(line, p, b, e);
                bool up = eqTok(line, b, e, "UP");
                nextTok(line, p, b, e);
                int rem = (int)toInt(line, b, e);
                nextTok(line, p, b, e);
                nextTok(line, p, b, e);
                bool isPre = eqTok(line, b, e, "PRE");
                nextTok(line, p, b, e);
                int m = (int)toInt(line, b, e);
                for (int i = 0; i < m; i++) {
                    nextTok(line, p, b, e);
                    int rid = (int)toInt(line, b, e);
                    if (rid < 0 || rid >= (int)req.size() || !req[rid].alive) continue;
                    if (isPre) {
                        if (up) push(qPPROC[rem], rid, NEED_PPROC);
                        else push(qPPOST, rid, NEED_PPOST);
                    } else {
                        if (up) {
                            push(qDPROC[rem], rid, NEED_DPROC);
                        } else {
                            nDpost++;
                            push(qDPOST, rid, NEED_DPOST);
                        }
                    }
                }
            }
        }

        cmds.clear();
        // Пока токенов почти нет, отношение «префилл на токен» шумит — держим все
        // серверы в работе (поведение v6) и только потом сжимаем пул.
        if (tokensDone >= 32) {
            prefE = prefWorkE / tokensDone;
            prefC = prefWorkC / tokensDone;
            prefL = prefWorkL / tokensDone;
        } else {
            prefE = prefC = prefL = 1e9;
        }
        // Пересчёт модели стоит ~30 бинпоисков; на 2*10^6 фреймов это секунды, поэтому
        // обновляем при смене пула и изредка — чтобы подхватить дрейф prefE/prefC/prefL.
        if ((frameNo & 255) == 0) cachedFor = -1;
        Wave wave = waveFor(pool);
        bool preFirst = prefillFirst();
        // Пока декода нет, резать нечего — куски только добавят S на каждый.
        chunkTargetMs = pool > 0 ? S + col[C_DPROC].at(max(1.0, (double)wave.m)) : 0.0;

        // ---------------- локальный сервер: ровно одна задача за фрейм
        if (!edgeBusy) {
            clean(qPPRE, NEED_PPRE);
            clean(qPPOST, NEED_PPOST);
            clean(qDPOST, NEED_DPOST);
            int haveDpre = 0;
            for (int k = 0; k < K; k++) haveDpre += nDpre[k];
            // Копить ли D POST: дробление возвратов — главная статья расхода на E
            // (каждая задача стоит свой S). Ждём, пока наберётся волна, но только
            // пока событие гарантировано.
            bool canDpost = nDpost > 0 && (waitPost == 0 || nDpost >= wave.m || inFlight <= 0);


            bool havePpost = !qPPOST.empty();
            bool havePpre = !qPPRE.empty();

            // Ждать ради полной волны. Запускать D PRE, едва хоть кто-то готов, —
            // это волны по 20 вместо 60 и лишний 2S на каждую. Условие пропустить
            // кадр: набралось меньше целевого И что-то в полёте, то есть событие
            // ГАРАНТИРОВАНО придёт (иначе это stuck state = 0 за тест). Если
            // остальные уже финишировали, pool падает, цель снижается — и волна
            // уходит. Пока ждём, E занимается возвратами и префиллом.
            bool canDpre = haveDpre > 0 && (waitPre == 0 || haveDpre >= wave.m || inFlight <= 0);

            // 0 = D POST, 1 = P POST, 2 = P PRE, 3 = D PRE.
            // P POST раньше P PRE: он закрывает TDR уже запущенному запросу.
            // D POST раньше D PRE: он закрывает токен уже проделанной работы.
            int choice = -1;
            if (policy == 0) {
                long long best = LLONG_MAX;
                if (canDpost) { best = req[qDPOST.front()].seq; choice = 0; }
                if (havePpost && req[qPPOST.front()].seq < best) {
                    best = req[qPPOST.front()].seq; choice = 1;
                }
                if (havePpre && req[qPPRE.front()].seq < best) {
                    best = req[qPPRE.front()].seq; choice = 2;
                }
                if (canDpre) {
                    long long s2 = LLONG_MAX;
                    for (int k = 0; k < K; k++) {
                        clean(qDPRE[k], NEED_DPRE);
                        if (!qDPRE[k].empty()) s2 = min(s2, req[qDPRE[k].front()].seq);
                    }
                    if (s2 < best) choice = 3;
                }
            } else if (preFirst) {
                if (havePpost) choice = 1;
                else if (havePpre) choice = 2;
                else if (canDpost) choice = 0;
                else if (canDpre) choice = 3;
            } else {
                if (canDpost) choice = 0;
                else if (canDpre) choice = 3;
                else if (havePpost) choice = 1;
                else if (havePpre) choice = 2;
            }

            if (choice == 0) {
                group.clear();
                take(qDPOST, NEED_DPOST, chunkDpost, group, &nDpost);
                if (!group.empty()) {
                    string c = "E D POST -1 ";
                    appendInt(c, (long long)group.size());
                    for (int rid : group) {
                        c.push_back(' ');
                        appendInt(c, rid);
                        req[rid].stage = RUN_DPOST;
                    }
                    cmds.push_back(c);
                    edgeBusy = true;
                    inFlight++;
                }
            } else if (choice == 1) {
                int rid = qPPOST.front();
                qPPOST.pop_front();
                req[rid].stage = RUN_PPOST;
                prefWorkE += S + col[C_PPOST].at(req[rid].Lin);
                string c = "E P POST ";
                appendInt(c, req[rid].remote);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                edgeBusy = true;
                inFlight++;
            } else if (choice == 2) {
                int rid = qPPRE.front();
                qPPRE.pop_front();
                // Раскладываем по всем серверам: облако несёт не только декод, но и
                // P PROC, который на многих тестах и есть узкое место.
                int rem = 0;
                for (int k = 1; k < K; k++)
                    if (assignedOn[k] < assignedOn[rem]) rem = k;
                req[rid].remote = rem;
                assignedOn[rem]++;
                req[rid].stage = RUN_PPRE;
                prefWorkE += S + col[C_PPRE].at(req[rid].Lin);
                prefWorkL += xferMs(req[rid].Lin);        // UP, а позже столько же DOWN
                string c = "E P PRE ";
                appendInt(c, rem);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                edgeBusy = true;
                inFlight++;
            } else if (choice == 3) {
                // Волна: m участников, размазанных по j удалённым серверам. Берём j
                // самых наполненных очередей — так волна набирается за меньшее число
                // переводов, а D PROC получаются одинакового размера.
                // Волна собирается со всех серверов: одна задача на E вместо K, а
                // переводы всё равно идут по одному на сервер независимо от того,
                // одной группой мы их запустили или K отдельными.
                group.clear();
                // Собираем со всех серверов. Волна с ОДНОГО сервера даёт неразрезанный
                // D POST (один DOWN вместо K) и на бумаге дешевле, но она размером
                // pool/K, и это перевешивает: замер 6300 -> 5684. Концентрировать же
                // сам пул на части серверов нельзя — на них лежит и P PROC.
                for (int k = 0; k < K && (int)group.size() < chunkDpre; k++)
                    take(qDPRE[k], NEED_DPRE, chunkDpre, group, &nDpre[k]);
                if (!group.empty()) {
                    string c = "E D PRE -1 ";
                    appendInt(c, (long long)group.size());
                    for (int rid : group) {
                        c.push_back(' ');
                        appendInt(c, rid);
                        req[rid].stage = RUN_DPRE;
                    }
                    cmds.push_back(c);
                    edgeBusy = true;
                    inFlight++;
                }
            }
        }

        // ---------------- удалённые серверы
        for (int k = 0; k < K; k++) {
            if (cloudBusy[k]) continue;
            clean(qPPROC[k], NEED_PPROC);
            clean(qDPROC[k], NEED_DPROC);
            bool haveP = !qPPROC[k].empty();
            bool haveD = !qDPROC[k].empty();
            if (!haveP && !haveD) continue;

            // P PROC бывает в разы длиннее всего декод-оборота (в таблице встречаются
            // тысячи мс), и он держит remote целиком. Пропускать его вперёд, пока пул
            // добирается до целевого, дешевле, чем потом вклинивать между волнами:
            // так весь префилл уходит одной фазой и не рвёт TPOT в середине прогона.
            bool doPrefill = haveP && (!haveD || preFirst);
            if (doPrefill) {
                int rid = qPPROC[k].front();
                qPPROC[k].pop_front();
                req[rid].stage = RUN_PPROC;
                int ls = req[rid].cursor;
                int le = numLayers;
                // P PROC занимает remote целиком и бывает в разы длиннее декод-задачи;
                // пока он идёт, готовая волна на этом сервере стоит, и это прямо
                // раздувает TPOT. Режем его на куски, сопоставимые с декод-задачей.
                // Каждый кусок стоит лишний S, поэтому куски не мельче цели.
                // Режем, только если на этом сервере прямо сейчас стоит волна декода:
                // иначе разбиение — чистая переплата S на узком месте, а префилл
                // растягивается и портит TDR.
                // Режем только когда на сервере ждёт волна декода И у сервера есть
                // запас ёмкости: если облако само по себе узкое место, лишний S на
                // каждый кусок отнимает именно ту ёмкость, которой не хватает.
                double util = now > 1e-9 ? cloudWork[k] / now : 1.0;
                if (numLayers > 1 && chunkTargetMs > 0 && haveD && wc > 0.0 && util < 0.7) {
                    double full = col[C_PPROC].at(req[rid].Lin);
                    int want = (int)ceil(full / chunkTargetMs);
                    if (want < 1) want = 1;
                    if (want < numLayers) {
                        int per = (numLayers + want - 1) / want;
                        if (per < 1) per = 1;
                        le = min(numLayers, ls + per);
                    } else {
                        le = min(numLayers, ls + 1);
                    }
                }
                string c = "C";
                appendInt(c, k);
                c += " P PROC ";
                appendInt(c, ls);
                c.push_back(' ');
                appendInt(c, le);
                c.push_back(' ');
                appendInt(c, k);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                cloudBusy[k] = 1;
                {
                    double d = S + (double)(le - ls) / numLayers * col[C_PPROC].at(req[rid].Lin);
                    cloudWork[k] += d;
                    prefWorkC += d;
                }
                inFlight++;
            } else {
                group.clear();
                take(qDPROC[k], NEED_DPROC, chunkDproc, group);
                if (group.empty()) continue;
                string c = "C";
                appendInt(c, k);
                c += " D PROC ";
                appendInt(c, k);
                c.push_back(' ');
                appendInt(c, (long long)group.size());
                for (int rid : group) {
                    c.push_back(' ');
                    appendInt(c, rid);
                    req[rid].stage = RUN_DPROC;
                }
                cmds.push_back(c);
                cloudBusy[k] = 1;
                cloudWork[k] += S + col[C_DPROC].at((double)group.size());
                inFlight++;
            }
        }

        if (dbg && ++dbgFrame % dbgEvery == 0) {
            int dpre = 0;
            for (int k = 0; k < K; k++) dpre += nDpre[k];
            fprintf(stderr,
                    "t=%.1f pool=%d/%d m=%d ruse=%d T=%.1f pre=%d | dpre=%d dpost=%zu "
                    "ppre=%zu ppost=%zu | tdr=%.1f tpot=%.1f n=%zu\n",
                    now, pool, poolTarget, wave.m, wave.ruse, wave.T, (int)preFirst, dpre,
                    qDPOST.size(), qPPRE.size(), qPPOST.size(), tdrEstimate(),
                    tpotEstimate(), cmds.size());
        }

        out.clear();
        appendInt(out, (long long)cmds.size());
        out.push_back('\n');
        for (const string &c : cmds) {
            out += c;
            out.push_back('\n');
        }
        cout << out << flush;
    }

    return 0;
}
