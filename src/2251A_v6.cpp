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
    int stage = DONE;
    int remote = -1;
    int Lin = 1;
    int prod = 0;
    double procMs = 0;
    long long seq = 0;
    bool alive = false;
};

int K, numLayers;
vector<array<double,7>> tab;
static double tabCol(int col, double x) {
    if (tab.empty()) return 0.0;
    if (x <= tab.front()[0]) return tab.front()[col];
    if (x >= tab.back()[0]) return tab.back()[col];
    int lo = 0, hi = (int)tab.size() - 1;
    while (hi - lo > 1) { int md = (lo + hi) >> 1; if (tab[md][0] <= x) lo = md; else hi = md; }
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
    while (u) { buf[len++] = char('0' + (u % 10)); u /= 10; }
    if (neg) buf[len++] = '-';
    while (len) s.push_back(buf[--len]);
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    double S, lat, bw, bpt;
    if (!(cin >> K >> S >> lat >> bw >> bpt >> numLayers)) return 0;

    double slo1, slo2, tpUB, tpBase, distBase, wtp, wc;
    cin >> slo1 >> slo2 >> tpUB >> tpBase >> distBase >> wtp >> wc;

    int N;
    cin >> N;
    tab.resize(N);
    for (int i = 0; i < N; i++) for (int j = 0; j < 7; j++) cin >> tab[i][j];
    dec1 = tabCol(5, 1.0);

    string line;
    getline(cin, line);

    qPPROC.assign(K, {});
    qDPROC.assign(K, {});
    cloudBusy.assign(K, 0);
    loadCnt.assign(K, 0);
    loadMs.assign(K, 0.0);
    decActive.assign(K, 0);
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
                push(qPPRE, rid, NEED_PPRE);
            } else if (eqTok(line, b, e, "FIN")) {
                nextTok(line, p, b, e);
                int rid = (int)toInt(line, b, e);
                if (rid >= 0 && rid < (int)req.size()) {
                    finCnt++;
                    if (req[rid].remote >= 0) { loadCnt[req[rid].remote]--; decActive[req[rid].remote]--; }
                    req[rid].stage = DONE;
                    req[rid].alive = false;
                }
            } else if (eqTok(line, b, e, "TDN")) {
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
                        }
                    } else if (isPROC) {
                        nextTok(line, p, b, e);
                        nextTok(line, p, b, e);
                        nextTok(line, p, b, e);
                        int rem = (int)toInt(line, b, e);
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < (int)req.size() && req[rid].alive) {
                            if (rem >= 0 && rem < K) loadMs[rem] = max(0.0, loadMs[rem] - req[rid].procMs);
                            req[rid].procMs = 0;
                            req[rid].stage = WAIT_DOWN_PRE;
                        }
                    } else {
                        nextTok(line, p, b, e);
                        nextTok(line, p, b, e);
                        int rid = (int)toInt(line, b, e);
                        if (rid < (int)req.size() && req[rid].alive) {
                            if (req[rid].remote >= 0) decActive[req[rid].remote]++;
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
                        if (isPRE) req[rid].stage = WAIT_UP_DEC;
                        else if (isPROC) req[rid].stage = WAIT_DOWN_DEC;
                        else {
                            req[rid].prod++;
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

        if (!edgeBusy) {
            clean(qPPRE, NEED_PPRE);
            clean(qPPOST, NEED_PPOST);
            clean(qDPRE, NEED_DPRE);
            clean(qDPOST, NEED_DPOST);

            int which = -1;
            long long best = LLONG_MAX;
            if (!qPPRE.empty() && req[qPPRE.front()].seq < best) { best = req[qPPRE.front()].seq; which = 0; }
            if (!qPPOST.empty() && req[qPPOST.front()].seq < best) { best = req[qPPOST.front()].seq; which = 1; }
            if (!qDPRE.empty() && req[qDPRE.front()].seq < best) { best = req[qDPRE.front()].seq; which = 2; }
            if (!qDPOST.empty() && req[qDPOST.front()].seq < best) { best = req[qDPOST.front()].seq; which = 3; }

            if (which == 0) {
                int rid = qPPRE.front();
                qPPRE.pop_front();
                int rem = 0;
                if (finCnt >= 1 && multiCnt == 0) {
                    double bl = 1e300;
                    for (int k = 0; k < K; k++) {
                        double est = loadMs[k] + decActive[k] * dec1;
                        if (est < bl - 1e-12) { bl = est; rem = k; }
                    }
                } else {
                    for (int k = 1; k < K; k++) if (loadCnt[k] < loadCnt[rem]) rem = k;
                }
                req[rid].remote = rem;
                loadCnt[rem]++;
                req[rid].procMs = tabCol(2, req[rid].Lin);
                loadMs[rem] += req[rid].procMs;
                req[rid].stage = RUN_PPRE;
                string c = "E P PRE ";
                appendInt(c, rem);
                c.push_back(' ');
                appendInt(c, rid);
                cmds.push_back(c);
                edgeBusy = true;
            } else if (which == 1) {
                int rid = qPPOST.front();
                qPPOST.pop_front();
                req[rid].stage = RUN_PPOST;
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
                        req[rid].stage = pre ? RUN_DPRE : RUN_DPOST;
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
            if (a == LLONG_MAX && b2 == LLONG_MAX) continue;

            if (a <= b2) {
                int rid = qPPROC[k].front();
                qPPROC[k].pop_front();
                req[rid].stage = RUN_PPROC;
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
                    req[rid].stage = RUN_DPROC;
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
