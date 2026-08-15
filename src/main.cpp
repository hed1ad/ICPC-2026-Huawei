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
    long long seq = 0;
    bool alive = false;
};

int K, numLayers;
double SS = 0, LAT = 0, BW = 1, BPT = 1, WTP = 0, DBASE = 0;
double tNow = 0;
double K1 = 2.5, K2 = 0.5, K3 = 1.2; int K4 = 1;
double TPUB = 0, TPBASE = 0, firstArr = -1;
long long tokProd = 0;
long long stageCnt[17] = {0};
vector<int> needDP;
vector<array<double,7>> tab;

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
        if (ns == NEED_DPROC) needDP[rr]++;
    }
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
    WTP = wtp; DBASE = distBase; TPUB = tpUB; TPBASE = tpBase;

    int N;
    cin >> N;
    tab.resize(N);
    for (int i = 0; i < N; i++)
        for (int j = 0; j < 7; j++)
            cin >> tab[i][j];

    dec1 = tabCol(5, 1.0);

    string line;
    getline(cin, line);

    qPPROC.assign(K, {});
    qDPROC.assign(K, {});
    cloudBusy.assign(K, 0);
    loadCnt.assign(K, 0);
    loadMs.assign(K, 0.0);
    decActive.assign(K, 0);
    needDP.assign(K, 0);
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
                if (firstArr < 0) firstArr = tNow;
                push(qPPRE, rid, NEED_PPRE);
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
                            if (req[rid].remote >= 0)
                                decActive[req[rid].remote]++;
                            push(qDPRE, rid, NEED_DPRE);
                        }
                    }
                } else {
                    nextTok(line, p, b, e);
                    nextTok(line, p, b, e);
                    int m = (int)toInt(line, b, e);
                    for (int i = 0; i < m; i++) {
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < 0 || rid >= (int)req.size() || !req[rid].alive) continue;
                        if (isPRE) setStage(rid, WAIT_UP_DEC);
                        else if (isPROC) setStage(rid, WAIT_DOWN_DEC);
                        else {
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
                        if (up) push(qPPROC[rem], rid, NEED_PPROC);
                        else push(qPPOST, rid, NEED_PPOST);
                    } else {
                        if (up) push(qDPROC[rem], rid, NEED_DPROC);
                        else push(qDPOST, rid, NEED_DPOST);
                    }
                }
            }
        }

        cmds.clear();

        double mTgt = 0.0, tRound = 0.0;
        static vector<double> mTgtC;
        mTgtC.assign(K, 0.0);
        bool batching = (WTP >= 0.25) && (DBASE > 0.0);
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
                    te = 2.0 * S + tabCol(4, m) + tabCol(6, m);
                    tc = (S + tabCol(5, m)) / (double)K;
                    tx = xferMs(m);
                    tRound = te + (S + tabCol(5, m)) + 2.0 * tx;
                    tb = max(te, max(tc, tx));
                    m = max(1.0, min((double)A, (double)A * tb / max(tRound, 1e-12)));
                }
                mTgt = m * K3;
                for (int k = 0; k < K; k++) {
                    double Ak = (double)decActive[k];
                    mTgtC[k] = max(1.0, min(Ak, Ak * tb / max(tRound, 1e-12))) * K3;
                }
            } else batching = false;
        }

        if (!edgeBusy) {
            clean(qPPRE, NEED_PPRE);
            clean(qPPOST, NEED_PPOST);
            clean(qDPRE, NEED_DPRE);
            clean(qDPOST, NEED_DPOST);

            int which = -1;
            long long best = LLONG_MAX;
            if (!qPPRE.empty() && req[qPPRE.front()].seq < best) { best = req[qPPRE.front()].seq; which = 0; }
            if (!qPPOST.empty() && req[qPPOST.front()].seq < best) { best = req[qPPOST.front()].seq; which = 1; }
            bool holdPre = false;
            if (batching && !qDPRE.empty() && pendingFuture() > 0
                && (double)stageCnt[NEED_DPRE] * K1 <= mTgt
                && (tNow - req[qDPRE.front()].ready) < K2 * tRound) holdPre = true;
            if (!qDPRE.empty() && !holdPre && req[qDPRE.front()].seq < best) { best = req[qDPRE.front()].seq; which = 2; }
            bool holdPost = false;
            if (K4 && batching && !qDPOST.empty() && pendingFuture() > 0
                && (double)stageCnt[NEED_DPOST] * K1 <= mTgt
                && (tNow - req[qDPOST.front()].ready) < K2 * tRound) holdPost = true;
            if (!qDPOST.empty() && !holdPost && req[qDPOST.front()].seq < best) { best = req[qDPOST.front()].seq; which = 3; }

            if (which == 0) {
                int rid = qPPRE.front();
                qPPRE.pop_front();

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
                req[rid].procMs = tabCol(2, req[rid].Lin);
                loadMs[rem] += req[rid].procMs;
                setStage(rid, RUN_PPRE);

                string c = "E P PRE ";
                appendInt(c, rem);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                edgeBusy = true;
            } else if (which == 1) {
                int rid = qPPOST.front();
                qPPOST.pop_front();
                setStage(rid, RUN_PPOST);

                string c = "E P POST ";
                appendInt(c, req[rid].remote);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                edgeBusy = true;
            } else if (which == 2 || which == 3) {
                bool pre = (which == 2);
                collectAll(pre ? qDPRE : qDPOST, pre ? NEED_DPRE : NEED_DPOST, group);
                if (!group.empty()) {
                    string c = pre ? "E D PRE -1 " : "E D POST -1 ";
                    appendInt(c, (long long)group.size());
                    for (int rid : group) {
                        c.push_back(' ');
                        appendInt(c, rid);
                        setStage(rid, pre ? RUN_DPRE : RUN_DPOST);
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
            if (batching && b2 != LLONG_MAX && pendingFuture() > 0
                && (double)needDP[k] * K1 <= mTgtC[k]
                && (tNow - req[qDPROC[k].front()].ready) < K2 * tRound) b2 = LLONG_MAX;
            if (a == LLONG_MAX && b2 == LLONG_MAX) continue;

            if (a <= b2) {
                int rid = qPPROC[k].front();
                qPPROC[k].pop_front();
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
                collectAll(qDPROC[k], NEED_DPROC, group);
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
