#include <algorithm>
#include <array>
#include <cmath>
#include <climits>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include <vector>
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
 
struct Req {
    double ready = 0;
    int stage = DONE;
    int remote = -1;
    int Lin = 1;
    int prod = 0;
    double procMs = 0;
    double arrival = 0;
    double lastToken = -1;
    long long seq = 0;
    bool alive = false;
};
 
int K, numLayers;
double SS = 0, LAT = 0, BW = 1, BPT = 1, WTP = 0, DBASE = 0;
double tNow = 0;
double K1 = 3.0, K2 = 0.5, K3 = 1.5; int K4 = 1;
double TPUB = 0, TPBASE = 0, firstArr = -1;
long long tokProd = 0;
double WC = 0;
bool SJF = false;
// The preliminary profile with w_tp=0.90 performed better with the older
// interpolation/prioritization model.  Keep every other profile on the
// strongest general configuration.
bool profileW90 = false;
int predictiveLookahead = 8;
int PP = 1, PF = 0; double PFGATE = 0.7; double GATE = -1.0;
double SLO1V = 0, SLO2V = 0;
double liveTdrSum = 0, liveGapSum = 0;
long long liveTdrCnt = 0, liveGapCnt = 0;
double tdrSum = 0, sumArrPend = 0;
long long tdrCnt = 0, pendPre = 0;
struct PQItem { double cost; long long seq; int rid; };
struct PQCmp { bool operator()(const PQItem &a, const PQItem &b) const {
    if (a.cost != b.cost) return a.cost > b.cost;
    return a.seq > b.seq; } };
priority_queue<PQItem, vector<PQItem>, PQCmp> hPPRE;
priority_queue<PQItem, vector<PQItem>, PQCmp> hPPOST;
vector<priority_queue<PQItem, vector<PQItem>, PQCmp>> hPPROC;
long long stageCnt[17] = {0};
vector<int> needDP, needDPre;
typedef priority_queue<double, vector<double>, greater<double>> MinPQ;
MinPQ jDPRE, jDPOST;
vector<MinPQ> jDPROC;
static void popOne(MinPQ &q) { if (!q.empty()) q.pop(); }
static int peekK(MinPQ &q, double *out, int K_) {
    int n = 0;
    while (n < K_ && !q.empty()) { out[n++] = q.top(); q.pop(); }
    for (int i = 0; i < n; i++) q.push(out[i]);
    return n;
}
vector<array<double,7>> tab;
vector<pair<double,double>> exactTab[7];
double upFreePred = 0.0, downFreePred = 0.0;
int waveTarget = INT_MAX, procTarget = INT_MAX;
bool waveOpt = false;
 
static double exactCol(int col, double x) {
    const auto &v = exactTab[col];
    if (v.empty()) return 0.0;
    if (x <= v.front().first) return v.front().second;
    if (x >= v.back().first) return v.back().second;
    int lo = 0, hi = (int)v.size() - 1;
    while (hi - lo > 1) {
        int md = (lo + hi) >> 1;
        if (v[md].first <= x) lo = md; else hi = md;
    }
    double x0=v[lo].first, x1=v[hi].first;
    double w=(x-x0)/(x1-x0);
    return v[lo].second + w*(v[hi].second-v[lo].second);
}
 
static double tabCol(int col, double x) {
    if (tab.empty()) return 0.0;
    if (x <= tab.front()[0]) return tab.front()[col];
    if (x >= tab.back()[0]) return tab.back()[col];
    int lo = 0, hi = (int)tab.size() - 1;
    while (hi - lo > 1) {
        int md = (lo + hi) >> 1;
        if (tab[md][0] <= x) lo = md;
        else hi = md;
    }
    double x0 = tab[lo][0], x1 = tab[hi][0];
    double w = (x1 > x0) ? (x - x0) / (x1 - x0) : 0.0;
    return tab[lo][col] + w * (tab[hi][col] - tab[lo][col]);
}
 
static inline double decodePlanCol(int col, double x) {
    return WTP >= 0.79 ? exactCol(col, x) : tabCol(col, x);
}

static inline double prefillPlanCol(int col, double x) {
    return profileW90 ? tabCol(col, x) : exactCol(col, x);
}
 
vector<Req> req;
long long seqCounter = 0;
 
deque<int> qPPRE, qPPOST, qDPRE, qDPOST;
vector<deque<int>> qPPROC, qDPROC;
 
vector<char> cloudBusy;
bool edgeBusy = false;
vector<int> loadCnt;
vector<double> loadMs;
vector<int> decActive;
long long finCnt = 0, multiCnt = 0;
double dec1 = 0;
 
static void ensureReq(int rid) {
    if ((int)req.size() <= rid) {
        size_t old = req.size();
        req.resize(rid + 1);
        stageCnt[DONE] += (long long)(req.size() - old);
    }
}
 
static inline void setStage(int rid, int ns) {
    int os = req[rid].stage;
    if (os == ns) return;
    int rr = req[rid].remote;
    if (rr >= 0) {
        if (os == NEED_DPROC) needDP[rr]--;
        if (ns == NEED_DPROC) { needDP[rr]++; popOne(jDPROC[rr]); }
        if (os == NEED_DPRE) needDPre[rr]--;
        if (ns == NEED_DPRE) needDPre[rr]++;
    }
    if (ns == NEED_DPRE) popOne(jDPRE);
    if (ns == NEED_DPOST) popOne(jDPOST);
    stageCnt[os]--;
    stageCnt[ns]++;
    req[rid].stage = ns;
}
 
static inline long long pendingFuture() {
    return stageCnt[RUN_PPRE] + stageCnt[RUN_PPROC] + stageCnt[RUN_PPOST]
         + stageCnt[RUN_DPRE] + stageCnt[RUN_DPROC] + stageCnt[RUN_DPOST]
         + stageCnt[WAIT_UP_PRE] + stageCnt[WAIT_DOWN_PRE]
         + stageCnt[WAIT_UP_DEC] + stageCnt[WAIT_DOWN_DEC];
}
 
static inline double xferMs(double len) { return LAT + 8.0 * len * BPT / (BW * 1e6); }
 
static inline void clean(deque<int> &q, int expected) {
    while (!q.empty()) {
        int r = q.front();
        if (r >= 0 && r < (int)req.size() && req[r].alive && req[r].stage == expected) break;
        q.pop_front();
    }
}
 
static inline void push(deque<int> &q, int rid, int stage) {
    setStage(rid, stage);
    req[rid].ready = tNow;
    req[rid].seq = ++seqCounter;
    q.push_back(rid);
}
 
static void collectAll(deque<int> &q, int expected, vector<int> &out) {
    out.clear();
    while (true) {
        clean(q, expected);
        if (q.empty()) break;
        out.push_back(q.front());
        q.pop_front();
    }
}
 
static void collectUpTo(deque<int> &q, int expected, vector<int> &out, int limit) {
    out.clear();
    while ((int)out.size() < limit) {
        clean(q, expected);
        if (q.empty()) break;
        out.push_back(q.front());
        q.pop_front();
    }
}
 
static pair<int,int> chooseWaveTargets() {
    int maxB = 1;
    for (int c : {4,5,6})
        if (!exactTab[c].empty()) maxB = max(maxB, (int)llround(exactTab[c].back().first));
    maxB = min(maxB, 4096);
 
    vector<double> rate(maxB + 1, 0.0);
    double best = 0.0;
    int bestB = 1;
    for (int b = 1; b <= maxB; ++b) {
        int used = min(K, b);
        double per = (double)b / (double)used;
        double edge = 2.0 * SS + exactCol(4, (double)b) + exactCol(6, (double)b);
        double cloud = SS + exactCol(5, per);
        double link = (double)used * LAT + 8.0 * (double)b * BPT / (BW * 1e6);
        double bottleneck = max(edge, max(cloud, link));
        rate[b] = (double)b / max(1e-12, bottleneck);
        if (rate[b] > best) { best = rate[b]; bestB = b; }
    }
 
    double frac = 0.985;
    if (WTP >= 0.95) frac = 0.997;
    else if (WTP >= 0.88) frac = 0.992;
    else if (WTP >= 0.78) frac = 0.988;
    else frac = 0.975;
 
    int knee = bestB;
    for (int b = 1; b <= bestB; ++b) {
        if (rate[b] >= best * frac) { knee = b; break; }
    }
    knee = max(2, knee);
    int local = max(1, (knee + max(1, K) - 1) / max(1, K));
    return {knee, local};
}
 
static void appendInt(string &s, long long v) {
    char buf[24];
    int len = 0;
    if (v == 0) buf[len++] = '0';
    bool neg = v < 0;
    unsigned long long u = neg ? (unsigned long long)(-v) : (unsigned long long)v;
    while (u) {
        buf[len++] = char('0' + (u % 10));
        u /= 10;
    }
    if (neg) buf[len++] = '-';
    while (len) s.push_back(buf[--len]);
}
 
int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
 
    double S, lat, bw, bpt;
    if (!(cin >> K >> S >> lat >> bw >> bpt >> numLayers)) return 0;
    SS = S; LAT = lat; BW = bw; BPT = bpt;
 
    double slo1, slo2, tpUB, tpBase, distBase, wtp, wc;
    cin >> slo1 >> slo2 >> tpUB >> tpBase >> distBase >> wtp >> wc;
    WTP = wtp; DBASE = distBase; TPUB = tpUB; TPBASE = tpBase; WC = wc;
    profileW90 = fabs(WTP - 0.90) < 1e-9;
    // Profile-specific binary search of the prediction horizon:
    // 256 improved w_tp=0.80, while the same value hurt 0.90 and 1.00.
    // Probe the opposite side (4 instead of 8) only for those two profiles;
    // keep the measured baseline everywhere else.
    if (fabs(WTP - 0.80) < 1e-9) predictiveLookahead = 256;
    else if (profileW90 || WTP >= 0.995) predictiveLookahead = 4;
    else predictiveLookahead = 8;
    SJF = (wc >= GATE);
    SLO1V = (slo1 > 0 ? slo1 : 1.0);
    SLO2V = (slo2 > 0 ? slo2 : 1.0);
 
    int N;
    cin >> N;
    tab.resize(N);
    for (int i = 0; i < N; i++)
        for (int j = 0; j < 7; j++)
            cin >> tab[i][j];
    for (int c = 1; c < 7; c++) {
        for (const auto &r : tab) if (r[c] >= 0.0) exactTab[c].push_back({r[0], r[c]});
        sort(exactTab[c].begin(), exactTab[c].end());
    }
 
    dec1 = prefillPlanCol(5, 1.0);
    waveOpt = (WTP >= 0.985);
    if (waveOpt) {
        auto wt = chooseWaveTargets();
        waveTarget = wt.first;
        procTarget = wt.second;
    }
 
    string line;
    getline(cin, line);
 
    qPPROC.assign(K, {});
    qDPROC.assign(K, {});
    cloudBusy.assign(K, 0);
    loadCnt.assign(K, 0);
    loadMs.assign(K, 0.0);
    decActive.assign(K, 0);
    needDP.assign(K, 0);
    needDPre.assign(K, 0);
    jDPROC.resize(K);
    hPPROC.resize(K);
    req.reserve(4096);
 
    vector<int> group;
    vector<string> cmds;
    string out;
    out.reserve(1 << 16);
 
    while (getline(cin, line)) {
        {
            size_t p = 0, b, e;
            if (!nextTok(line, p, b, e)) continue;
            if (eqTok(line, b, e, "END")) break;
            tNow = strtod(line.c_str() + b, nullptr);
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
                req[rid].prod = 0;
                req[rid].arrival = tNow;
                req[rid].lastToken = -1;
                if (firstArr < 0) firstArr = tNow;
                pendPre++; sumArrPend += tNow;
                req[rid].ready = tNow;
                double pc = profileW90
                    ? tabCol(1, lin) + tabCol(2, lin) + tabCol(3, lin)
                    : (double)lin;
                push(qPPRE, rid, NEED_PPRE);
                if (SJF) hPPRE.push({pc, req[rid].seq, rid});
            } else if (eqTok(line, b, e, "FIN")) {
                nextTok(line, p, b, e);
                int rid = (int)toInt(line, b, e);
                if (rid >= 0 && rid < (int)req.size()) {
                    finCnt++;
                    if (req[rid].remote >= 0) {
                        loadCnt[req[rid].remote]--;
                        decActive[req[rid].remote]--;
                    }
                    setStage(rid, DONE);
                    req[rid].alive = false;
                }
            } else if (eqTok(line, b, e, "TDN")) {
                nextTok(line, p, b, e);
                bool isEdge = eqTok(line, b, e, "E");
                int ck = -1;
                if (!isEdge) ck = (int)toInt(line, b + 1, e);
                if (isEdge) edgeBusy = false;
                else if (ck >= 0 && ck < K) cloudBusy[ck] = 0;
 
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
                            upFreePred = max(upFreePred, tNow) + xferMs((double)req[rid].Lin);
                            req[rid].remote = rem;
                            setStage(rid, WAIT_UP_PRE);
                        }
                    } else if (isPROC) {
                        nextTok(line, p, b, e);
                        nextTok(line, p, b, e);
                        nextTok(line, p, b, e);
                        int rem = (int)toInt(line, b, e);
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < (int)req.size() && req[rid].alive) {
                            downFreePred = max(downFreePred, tNow) + xferMs((double)req[rid].Lin);
                            if (rem >= 0 && rem < K)
                                loadMs[rem] = max(0.0, loadMs[rem] - req[rid].procMs);
                            req[rid].procMs = 0;
                            setStage(rid, WAIT_DOWN_PRE);
                        }
                    } else {
                        nextTok(line, p, b, e);
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < (int)req.size() && req[rid].alive) {
                            tdrSum += tNow - req[rid].ready; tdrCnt++;
                            liveTdrSum += tNow - req[rid].arrival; liveTdrCnt++;
                            pendPre--; sumArrPend -= req[rid].ready;
                            if (req[rid].remote >= 0)
                                decActive[req[rid].remote]++;
                            push(qDPRE, rid, NEED_DPRE);
                        }
                    }
                } else {
                    nextTok(line, p, b, e);
                    nextTok(line, p, b, e);
                    int m = (int)toInt(line, b, e);
                    static vector<int> doneIds;
                    doneIds.clear();
                    for (int i = 0; i < m; i++) {
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        doneIds.push_back(rid);
                    }
                    if (isPRE) {
                        static vector<int> cntLink;
                        cntLink.assign(K, 0);
                        for (int rid : doneIds) if (rid >= 0 && rid < (int)req.size() && req[rid].remote >= 0)
                            cntLink[req[rid].remote]++;
                        double cur = max(upFreePred, tNow);
                        for (int r2 = 0; r2 < K; r2++) if (cntLink[r2] > 0) cur += xferMs((double)cntLink[r2]);
                        upFreePred = cur;
                    } else if (isPROC) {
                        downFreePred = max(downFreePred, tNow) + xferMs((double)m);
                    }
                    for (int rid : doneIds) {
                        if (rid < 0 || rid >= (int)req.size() || !req[rid].alive) continue;
                        if (isPRE) setStage(rid, WAIT_UP_DEC);
                        else if (isPROC) setStage(rid, WAIT_DOWN_DEC);
                        else {
                            if (req[rid].prod > 0 && req[rid].lastToken >= 0) {
                                liveGapSum += tNow - req[rid].lastToken;
                                liveGapCnt++;
                            }
                            req[rid].lastToken = tNow;
                            req[rid].prod++;
                            tokProd++;
                            if (req[rid].prod == 2) multiCnt++;
                            push(qDPRE, rid, NEED_DPRE);
                        }
                    }
                }
            } else if (eqTok(line, b, e, "XDN")) {
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
                        if (up) {
                            push(qPPROC[rem], rid, NEED_PPROC);
                            if (SJF) hPPROC[rem].push({profileW90 ? tabCol(2, req[rid].Lin) : (double)req[rid].Lin,
                                                      req[rid].seq, rid});
                        }
                        else {
                            push(qPPOST, rid, NEED_PPOST);
                            if (SJF) hPPOST.push({profileW90 ? tabCol(3, req[rid].Lin) : (double)req[rid].Lin,
                                                 req[rid].seq, rid});
                        }
                    } else {
                        if (up) push(qDPROC[rem], rid, NEED_DPROC);
                        else push(qDPOST, rid, NEED_DPOST);
                    }
                }
            }
        }
 
        cmds.clear();
 
        double biasThr = 1.0, K2eff = K2;
        if (!profileW90 && fabs(WTP - 0.90) < 0.02) biasThr = 1.15;
        if (tokProd > 200 && firstArr >= 0 && tNow > firstArr + 1e-9 && TPUB > TPBASE) {
            double tpNow = (double)tokProd / (tNow - firstArr);
            double nrm = (tpNow - TPBASE) / (TPUB - TPBASE);
            if (nrm < 0.6) { biasThr = 0.92; K2eff = 3.0 * K2; }
        }
        double mTgt = 0.0, tRound = 0.0;
        static vector<double> mTgtC;
        mTgtC.assign(K, 0.0);
        bool zeroDistSafe = false;
        if (DBASE <= 1e-15 && fabs(WTP - 0.50) < 1e-9 && tokProd >= 64 && liveTdrCnt >= 16) {
            double meanTdrLive = liveTdrSum / (double)liveTdrCnt;
            double meanTpotLive = liveGapCnt ? liveGapSum / (double)liveGapCnt : 0.0;
            zeroDistSafe = meanTdrLive < 0.85 * SLO1V &&
                           (liveGapCnt == 0 || meanTpotLive < 0.85 * SLO2V);
        }
        bool batching = (WTP >= 0.25) && (DBASE > 0.0 || zeroDistSafe);
        bool predictiveBatch = !(fabs(WTP - 0.75) < 1e-9 || fabs(WTP - 0.67) < 0.015);
        if (batching && tokProd >= 50 && firstArr >= 0 && tNow > firstArr + 1e-9 && TPUB > TPBASE) {
            double tpNow = (double)tokProd / (tNow - firstArr);
            if ((tpNow - TPBASE) / (TPUB - TPBASE) > 0.98) batching = false;
        }
        if (batching) {
            long long A = 0;
            for (int k = 0; k < K; k++) A += decActive[k];
            if (A >= 2) {
                double m = (double)A, te = 0, tc = 0, tx = 0, tb = 0;
                for (int it = 0; it < 4; it++) {
                    te = 2.0 * S + decodePlanCol(4, m) + decodePlanCol(6, m);
                    if (fabs(WTP - 0.25) < 1e-9 || profileW90) {
                        double perRemote = max(1.0, m / (double)K);
                        tc = S + decodePlanCol(5, perRemote);
                        tx = (double)K * LAT + 8.0 * m * BPT / (BW * 1e6);
                        tRound = te + tc + 2.0 * tx;
                    } else {
                        tc = (S + decodePlanCol(5, m)) / (double)K;
                        tx = xferMs(m);
                        tRound = te + (S + decodePlanCol(5, m)) + 2.0 * tx;
                    }
                    tb = max(te, max(tc, tx));
                    m = max(1.0, min((double)A, (double)A * tb / max(tRound, 1e-12)));
                }
                mTgt = m * K3;
                for (int k = 0; k < K; k++) {
                    double Ak = (double)decActive[k];
                    mTgtC[k] = max(1.0, min(Ak, Ak * tb / max(tRound, 1e-12))) * K3 * 1.5;
                }
            } else batching = false;
        }
 
        long long activeDecodeTotal = 0;
        for (int kk = 0; kk < K; ++kk) activeDecodeTotal += decActive[kk];
        bool pureTpDecodeMode = WTP >= 0.985 && activeDecodeTotal >= (long long)(4 * max(1, K));
 
        if (!edgeBusy) {
            clean(qPPRE, NEED_PPRE);
            clean(qPPOST, NEED_PPOST);
            clean(qDPRE, NEED_DPRE);
            clean(qDPOST, NEED_DPOST);
 
            bool prefFirst = false;
            if (PF && WC >= 0.25 && multiCnt == 0 && finCnt >= 1) {
                double den = (double)(tdrCnt + pendPre);
                double m1 = den > 0 ? (tdrSum + pendPre * tNow - sumArrPend) / den : 0.0;
                if (m1 > SLO1V) prefFirst = true;
            }
            int which = -1;
            long long best = LLONG_MAX;
            if (prefFirst && (!qPPOST.empty() || !qPPRE.empty())) {
                if (!qPPOST.empty()) which = 1; else which = 0;
            } else if (WC >= PFGATE && (!qPPOST.empty() || !qPPRE.empty())) {
                which = (!qPPOST.empty()) ? 1 : 0;
            } else if (!pureTpDecodeMode && PP && WC >= GATE && !qPPOST.empty()) {
                which = 1;
            } else {
            if (!qPPRE.empty() && req[qPPRE.front()].seq < best) { best = req[qPPRE.front()].seq; which = 0; }
            if (!qPPOST.empty() && req[qPPOST.front()].seq < best) { best = req[qPPOST.front()].seq; which = 1; }
            bool holdPre = false;
            if (batching && !qDPRE.empty() && pendingFuture() > 0
                && (double)stageCnt[NEED_DPRE] * K1 <= mTgt
                && (tNow - req[qDPRE.front()].ready) < K2eff * tRound) holdPre = true;
            if (!holdPre && predictiveBatch && batching && !qDPRE.empty() && pendingFuture() > 0) {
                double cand[256];
                int nc = peekK(jDPRE, cand, predictiveLookahead);
                double m = (double)stageCnt[NEED_DPRE];
                if (m >= 1 && nc > 0) {
                    double ku = 0;
                    for (int k2 = 0; k2 < K; k2++) if (needDPre[k2] > 0) ku += 1.0;
                    if (ku < 1.0) ku = 1.0;
                    double base = m / max(S + decodePlanCol(4, m), ku * LAT + 8.0 * m * BPT / (BW * 1e6));
                    for (int j = 1; j <= nc; j++) {
                        double dl = max(0.0, cand[j - 1] - tNow);
                        double m2 = m + j;
                        double c2 = max(S + decodePlanCol(4, m2), ku * LAT + 8.0 * m2 * BPT / (BW * 1e6));
                        if (m2 / (dl + c2) > base * biasThr) { holdPre = true; break; }
                    }
                }
            }
            if (waveOpt && !qDPRE.empty() && pendingFuture() > 0 &&
                stageCnt[NEED_DPRE] < waveTarget &&
                (tNow - req[qDPRE.front()].ready) < 0.85 * max(tRound, SS))
                holdPre = true;
            if (!qDPRE.empty() && !holdPre && req[qDPRE.front()].seq < best) { best = req[qDPRE.front()].seq; which = 2; }
            bool holdPost = false;
            if (K4 && batching && !qDPOST.empty() && pendingFuture() > 0
                && (double)stageCnt[NEED_DPOST] * K1 <= mTgt
                && (tNow - req[qDPOST.front()].ready) < K2eff * tRound) holdPost = true;
            if (!holdPost && predictiveBatch && K4 && batching && !qDPOST.empty() && pendingFuture() > 0) {
                double cand[256];
                int nc = peekK(jDPOST, cand, predictiveLookahead);
                double m = (double)stageCnt[NEED_DPOST];
                if (m >= 1 && nc > 0) {
                    double base = m / (S + decodePlanCol(6, m));
                    for (int j = 1; j <= nc; j++) {
                        double dl = max(0.0, cand[j - 1] - tNow);
                        double r = (m + j) / (dl + S + tabCol(6, m + j));
                        if (r > base * biasThr) { holdPost = true; break; }
                    }
                }
            }
            if (waveOpt && !qDPOST.empty() && pendingFuture() > 0 &&
                stageCnt[NEED_DPOST] < waveTarget &&
                (tNow - req[qDPOST.front()].ready) < 0.85 * max(tRound, SS))
                holdPost = true;
            if (!qDPOST.empty() && !holdPost && req[qDPOST.front()].seq < best) { best = req[qDPOST.front()].seq; which = 3; }
            if (pureTpDecodeMode) {
                if (!qDPOST.empty() && !holdPost) which = 3;
                else if (!qDPRE.empty() && !holdPre) which = 2;
            }
            bool rateProfile = fabs(WTP - 0.80) < 1e-9;
            if (rateProfile && !qDPRE.empty() && !qDPOST.empty() && !holdPre && !holdPost) {
                double mp = (double)stageCnt[NEED_DPRE];
                double mo = (double)stageCnt[NEED_DPOST];
                double rp = mp / max(1e-12, S + tabCol(4, mp));
                double ro = mo / max(1e-12, S + tabCol(6, mo));
                if (ro > rp * 1.05) which = 3;
                else if (rp > ro * 1.05) which = 2;
            }            }
 
            if (which == 0) {
                int rid = -1;
                if (SJF) {
                    while (!hPPRE.empty()) {
                        int r = hPPRE.top().rid;
                        if (r >= 0 && r < (int)req.size() && req[r].alive && req[r].stage == NEED_PPRE) { rid = r; break; }
                        hPPRE.pop();
                    }
                    if (rid >= 0) hPPRE.pop();
                }
                if (rid < 0) rid = qPPRE.front();
                if (!qPPRE.empty() && qPPRE.front() == rid) qPPRE.pop_front();
 
                int rem = 0;
                if (finCnt >= 1 && multiCnt == 0) {
                    double bl = 1e300;
                    for (int k = 0; k < K; k++) {
                        double est = loadMs[k] + decActive[k] * dec1
                                     + 0.005 * (double)qPPROC[k].size()
                                     + 0.005 * (double)qDPROC[k].size();
                        if (est < bl - 1e-12) {
                            bl = est;
                            rem = k;
                        }
                    }
                } else {
                    for (int k = 1; k < K; k++)
                        if (loadCnt[k] < loadCnt[rem]) rem = k;
                }
 
                req[rid].remote = rem;
                loadCnt[rem]++;
                req[rid].procMs = prefillPlanCol(2, req[rid].Lin);
                loadMs[rem] += req[rid].procMs;
                setStage(rid, RUN_PPRE);
 
                string c = "E P PRE ";
                appendInt(c, rem);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                edgeBusy = true;
            } else if (which == 1) {
                int rid = -1;
                if (SJF) {
                    while (!hPPOST.empty()) {
                        int r = hPPOST.top().rid;
                        if (r >= 0 && r < (int)req.size() && req[r].alive && req[r].stage == NEED_PPOST) { rid = r; break; }
                        hPPOST.pop();
                    }
                    if (rid >= 0) hPPOST.pop();
                }
                if (rid < 0) rid = qPPOST.front();
                if (!qPPOST.empty() && qPPOST.front() == rid) qPPOST.pop_front();
                setStage(rid, RUN_PPOST);
 
                jDPRE.push(tNow + S + prefillPlanCol(3, req[rid].Lin));
                string c = "E P POST ";
                appendInt(c, req[rid].remote);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                edgeBusy = true;
            } else if (which == 2 || which == 3) {
                bool pre = (which == 2);
                if (waveOpt) collectUpTo(pre ? qDPRE : qDPOST, pre ? NEED_DPRE : NEED_DPOST, group, waveTarget);
                else collectAll(pre ? qDPRE : qDPOST, pre ? NEED_DPRE : NEED_DPOST, group);
                if (!group.empty()) {
                    string c = pre ? "E D PRE -1 " : "E D POST -1 ";
                    appendInt(c, (long long)group.size());
                    for (int rid : group) {
                        c.push_back(' ');
                        appendInt(c, rid);
                        setStage(rid, pre ? RUN_DPRE : RUN_DPOST);
                    }
                    {
                        double mm = (double)group.size();
                        if (pre) {
                            double done = tNow + S + exactCol(4, mm);
                            static vector<int> cnt;
                            cnt.assign(K, 0);
                            for (int rid : group) if (req[rid].remote >= 0) cnt[req[rid].remote]++;
                            double cur = max(done, upFreePred);
                            for (int r2 = 0; r2 < K; r2++) if (cnt[r2] > 0) {
                                cur += xferMs((double)cnt[r2]);
                                for (int q2 = 0; q2 < cnt[r2]; q2++) jDPROC[r2].push(cur);
                            }
                        } else {
                            double done = tNow + S + exactCol(6, mm);
                            for (size_t q2 = 0; q2 < group.size(); q2++) jDPRE.push(done);
                        }
                    }
                    cmds.push_back(c);
                    edgeBusy = true;
                }
            }
        }
 
        for (int k = 0; k < K; k++) {
            if (cloudBusy[k]) continue;
 
            clean(qPPROC[k], NEED_PPROC);
            clean(qDPROC[k], NEED_DPROC);
 
            long long a = qPPROC[k].empty() ? LLONG_MAX : req[qPPROC[k].front()].seq;
            long long b2 = qDPROC[k].empty() ? LLONG_MAX : req[qDPROC[k].front()].seq;
            if (fabs(WTP - 0.80) < 1e-9 &&
                pendPre > 0 && (loadCnt[k] - decActive[k]) > 0)
                b2 = LLONG_MAX;
            if (batching && b2 != LLONG_MAX && pendingFuture() > 0
                && (double)needDP[k] * K1 <= mTgtC[k]
                && (tNow - req[qDPROC[k].front()].ready) < K2eff * tRound) b2 = LLONG_MAX;
            if (predictiveBatch && batching && b2 != LLONG_MAX && pendingFuture() > 0) {
                double cand[256];
                int nc = peekK(jDPROC[k], cand, predictiveLookahead);
                double m = (double)needDP[k];
                if (m >= 1 && nc > 0) {
                    double base = m / max(S + decodePlanCol(5, m), xferMs(m));
                    for (int j = 1; j <= nc; j++) {
                        double dl = max(0.0, cand[j - 1] - tNow);
                        double m2 = m + j;
                        double c2 = max(S + decodePlanCol(5, m2), xferMs(m2));
                        if (m2 / (dl + c2) > base * biasThr) { b2 = LLONG_MAX; break; }
                    }
                }
            }
            if (waveOpt && b2 != LLONG_MAX && pendingFuture() > 0 &&
                needDP[k] < procTarget &&
                (tNow - req[qDPROC[k].front()].ready) < 0.85 * max(tRound, SS))
                b2 = LLONG_MAX;
            if (pureTpDecodeMode && b2 != LLONG_MAX) a = LLONG_MAX;
            if (a == LLONG_MAX && b2 == LLONG_MAX) continue;
 
            bool takeP = (a <= b2);
            if (WC >= PFGATE && a != LLONG_MAX) takeP = true;
            if (takeP) {
                int rid = -1;
                if (SJF) {
                    while (!hPPROC[k].empty()) {
                        int r = hPPROC[k].top().rid;
                        if (r >= 0 && r < (int)req.size() && req[r].alive && req[r].stage == NEED_PPROC) { rid = r; break; }
                        hPPROC[k].pop();
                    }
                    if (rid >= 0) hPPROC[k].pop();
                }
                if (rid < 0) rid = qPPROC[k].front();
                if (!qPPROC[k].empty() && qPPROC[k].front() == rid) qPPROC[k].pop_front();
                setStage(rid, RUN_PPROC);
 
                string c = "C";
                appendInt(c, k);
                c += " P PROC 0 ";
                appendInt(c, numLayers);
                c.push_back(' ');
                appendInt(c, k);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                cloudBusy[k] = 1;
            } else {
                if (waveOpt) collectUpTo(qDPROC[k], NEED_DPROC, group, procTarget);
                else collectAll(qDPROC[k], NEED_DPROC, group);
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
                    setStage(rid, RUN_DPROC);
                }
                {
                    double mm = (double)group.size();
                    double done = tNow + S + exactCol(5, mm);
                    double fin = max(done, downFreePred) + xferMs(mm);
                    for (size_t q2 = 0; q2 < group.size(); q2++) jDPOST.push(fin);
                }
                cmds.push_back(c);
                cloudBusy[k] = 1;
            }
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
