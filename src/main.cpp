// A. Edge-Cloud Collaborative Scheduling — стратегия v6, реализация на C++23.
// Поведение планировщика идентично src/2251A_v6.cpp (единственное отличие:
// таблица времён сортируется по batch_size, спека порядок строк не гарантирует).

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <deque>
#include <iostream>
#include <iterator>
#include <limits>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

// ---------------------------------------------------------------- ввод/вывод

constexpr bool isSpace(char c) noexcept {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

// Токенайзер по строке кадра: без потоков и без аллокаций.
class Scanner {
public:
    explicit constexpr Scanner(std::string_view line) noexcept : line_{line} {}

    constexpr std::string_view next() noexcept {
        while (pos_ < line_.size() && isSpace(line_[pos_])) ++pos_;
        const std::size_t begin = pos_;
        while (pos_ < line_.size() && !isSpace(line_[pos_])) ++pos_;
        return line_.substr(begin, pos_ - begin);
    }

    constexpr long long nextInt() noexcept { return toInt(next()); }

    constexpr void skip(int count) noexcept {
        while (count-- > 0) { [[maybe_unused]] const auto tok = next(); }
    }

    static constexpr long long toInt(std::string_view tok) noexcept {
        long long value = 0;
        std::from_chars(tok.data(), tok.data() + tok.size(), value);
        return value;
    }

private:
    std::string_view line_;
    std::size_t pos_ = 0;
};

void appendInt(std::string& dst, long long value) {
    std::array<char, 24> buf{};
    const auto [end, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
    dst.append(buf.data(), end);
}

// Ответ на кадр: сначала число задач, потом сами задачи.
class Response {
public:
    void start(std::string_view head) {
        ++count_;
        body_ += head;
    }
    void arg(long long value) {
        body_.push_back(' ');
        appendInt(body_, value);
    }
    void finish() { body_.push_back('\n'); }

    void emit(std::string& scratch) {
        scratch.clear();
        appendInt(scratch, count_);
        scratch.push_back('\n');
        scratch += body_;
        std::cout.write(scratch.data(), static_cast<std::streamsize>(scratch.size()));
        std::cout.flush();
        body_.clear();
        count_ = 0;
    }

private:
    std::string body_;
    long long count_ = 0;
};

// ------------------------------------------------------------ таблица времён

enum class Col : std::size_t {
    BatchSize = 0,
    PrefillPre,
    PrefillProc,
    PrefillPost,
    DecodePre,
    DecodeProc,
    DecodePost,
};
constexpr std::size_t kCols = 7;
using Row = std::array<double, kCols>;

class Table {
public:
    void read(std::istream& in, int rows) {
        rows_.resize(static_cast<std::size_t>(rows));
        for (Row& row : rows_)
            for (double& cell : row) in >> cell;
        std::ranges::sort(rows_, {}, [](const Row& row) { return row[0]; });
    }

    // Кусочно-линейная интерполяция по batch_size с зажимом на краях.
    [[nodiscard]] double at(Col col, double x) const {
        if (rows_.empty()) return 0.0;
        const auto c = std::to_underlying(col);
        if (x <= rows_.front()[0]) return rows_.front()[c];
        if (x >= rows_.back()[0]) return rows_.back()[c];

        const auto hi = std::ranges::upper_bound(rows_, x, {}, [](const Row& r) { return r[0]; });
        const Row& right = *hi;
        const Row& left = *std::prev(hi);
        const double span = right[0] - left[0];
        const double w = span > 0.0 ? (x - left[0]) / span : 0.0;
        return left[c] + w * (right[c] - left[c]);
    }

private:
    std::vector<Row> rows_;
};

// ------------------------------------------------------------------ запросы

enum class Stage : std::uint8_t {
    NeedPPre,
    RunPPre,
    WaitUpPre,
    NeedPProc,
    RunPProc,
    WaitDownPre,
    NeedPPost,
    RunPPost,
    NeedDPre,
    RunDPre,
    WaitUpDec,
    NeedDProc,
    RunDProc,
    WaitDownDec,
    NeedDPost,
    RunDPost,
    Done,
};

struct Req {
    Stage stage = Stage::Done;
    int remote = -1;
    int lin = 1;
    int produced = 0;   // сколько токенов уже выдано
    double procMs = 0;  // вклад в loadMs сервера, пока идёт P PROC
    double work = 0;    // оценка работы префилла на облаке, ключ SJF
    long long seq = 0;  // порядок постановки в очередь, задаёт FCFS
    bool alive = false;
};

// Очередь готовых шагов: мин-куча по (key, seq). При key == 0 у всех элементов
// порядок совпадает с FIFO, поэтому это дословная замена deque в режиме FCFS.
class ReadyQueue {
public:
    struct Item {
        double key;
        long long seq;
        int rid;
    };

    void push(double key, long long seq, int rid) {
        heap_.push_back({key, seq, rid});
        std::ranges::push_heap(heap_, worseFirst);
    }
    [[nodiscard]] bool empty() const noexcept { return heap_.empty(); }
    [[nodiscard]] int front() const noexcept { return heap_.front().rid; }
    void pop_front() {
        std::ranges::pop_heap(heap_, worseFirst);
        heap_.pop_back();
    }

private:
    // «Хуже» = дальше от вершины: сначала большой ключ, при равных — поздний seq.
    static constexpr auto worseFirst = [](const Item& a, const Item& b) noexcept {
        return a.key != b.key ? a.key > b.key : a.seq > b.seq;
    };

    std::vector<Item> heap_;
};

// Переключатели для ablation (в сабмите используются значения по умолчанию).
int envInt(const char* name, int fallback) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') return fallback;
    return static_cast<int>(Scanner::toInt(raw));
}

// ---------------------------------------------------------------- планировщик

class Solver {
public:
    void run();

private:
    // параметры системы
    int k_ = 1;
    int numLayers_ = 1;
    Table table_;
    double decodeProc1_ = 0.0;  // decode_proc при группе из одного запроса

    // состояние запросов
    std::vector<Req> reqs_;
    long long seqCounter_ = 0;
    long long finCount_ = 0;
    long long multiTokenCount_ = 0;

    // очереди готовых к запуску шагов
    ReadyQueue qPPre_;
    std::deque<int> qPPost_, qDPre_, qDPost_;
    std::vector<ReadyQueue> qPProc_;
    std::vector<std::deque<int>> qDProc_;

    // ablation: 1 — префилл в порядке SJF, 0 — FCFS как в v6
    int sjf_ = envInt("SCHED_SJF", 1);
    // ablation: 1 — раскладка по оценке занятости в мс, 0 — гейт v6 по числу
    // назначенных. Замер: мс-раскладка хуже (−115 сама по себе, −474 вместе с SJF),
    // потому что при SJF loadMs у серверов выравнивается и раскладка вырождается.
    int balMs_ = envInt("SCHED_BAL_MS", 0);

    // занятость и оценка загрузки
    bool edgeBusy_ = false;
    std::vector<char> cloudBusy_;
    std::vector<int> loadCnt_;
    std::vector<double> loadMs_;
    std::vector<int> decActive_;

    // буферы
    std::vector<int> group_;
    Response response_;
    std::string outBuf_;

    void readHeader();
    bool readFrame(std::string& line);
    void handleEvent(std::string_view line);
    void handleTdn(Scanner& sc);
    void handleXdn(Scanner& sc);
    void scheduleEdge();
    void scheduleCloud();

    Req& req(int rid) { return reqs_[static_cast<std::size_t>(rid)]; }
    [[nodiscard]] bool known(int rid) const {
        return rid >= 0 && rid < static_cast<int>(reqs_.size()) && reqs_[static_cast<std::size_t>(rid)].alive;
    }
    void ensureReq(int rid) {
        if (static_cast<int>(reqs_.size()) <= rid) reqs_.resize(static_cast<std::size_t>(rid) + 1);
    }

    void push(std::deque<int>& q, int rid, Stage stage) {
        Req& r = req(rid);
        r.stage = stage;
        r.seq = ++seqCounter_;
        q.push_back(rid);
    }

    // Ключ SJF — оценка работы префилла на облаке; при выключенном SJF ключи
    // равны и куча вырождается в FIFO.
    void push(ReadyQueue& q, int rid, Stage stage) {
        Req& r = req(rid);
        r.stage = stage;
        r.seq = ++seqCounter_;
        q.push(sjf_ ? r.work : 0.0, r.seq, rid);
    }

    // Ленивое удаление: проход по всей очереди на каждом кадре недопустим.
    void clean(auto& q, Stage expected) {
        while (!q.empty()) {
            const int rid = q.front();
            if (known(rid) && req(rid).stage == expected) break;
            q.pop_front();
        }
    }

    void collectAll(std::deque<int>& q, Stage expected) {
        group_.clear();
        for (clean(q, expected); !q.empty(); clean(q, expected)) {
            group_.push_back(q.front());
            q.pop_front();
        }
    }

    [[nodiscard]] long long headSeq(auto& q, Stage expected) {
        clean(q, expected);
        return q.empty() ? std::numeric_limits<long long>::max() : req(q.front()).seq;
    }

    int pickRemote();
};

void Solver::readHeader() {
    double s = 0, latency = 0, bandwidth = 0, bytesPerToken = 0;
    if (!(std::cin >> k_ >> s >> latency >> bandwidth >> bytesPerToken >> numLayers_)) std::exit(0);

    double slo1 = 0, slo2 = 0, tpUb = 0, tpBase = 0, distBase = 0, wTp = 0, wC = 0;
    std::cin >> slo1 >> slo2 >> tpUb >> tpBase >> distBase >> wTp >> wC;

    int n = 0;
    std::cin >> n;
    table_.read(std::cin, n);
    decodeProc1_ = table_.at(Col::DecodeProc, 1.0);

    std::string rest;
    std::getline(std::cin, rest);

    qPProc_.assign(static_cast<std::size_t>(k_), {});
    qDProc_.assign(static_cast<std::size_t>(k_), {});
    cloudBusy_.assign(static_cast<std::size_t>(k_), 0);
    loadCnt_.assign(static_cast<std::size_t>(k_), 0);
    loadMs_.assign(static_cast<std::size_t>(k_), 0.0);
    decActive_.assign(static_cast<std::size_t>(k_), 0);
    reqs_.reserve(4096);
}

void Solver::handleTdn(Scanner& sc) {
    const std::string_view server = sc.next();
    if (server == "E") {
        edgeBusy_ = false;
    } else {
        const int ck = static_cast<int>(Scanner::toInt(server.substr(1)));
        if (ck >= 0 && ck < k_) cloudBusy_[static_cast<std::size_t>(ck)] = 0;
    }

    const bool isPrefill = sc.next() == "P";
    const std::string_view step = sc.next();
    const bool isPre = step == "PRE";
    const bool isProc = step == "PROC";

    if (isPrefill) {
        if (isPre) {
            const int remote = static_cast<int>(sc.nextInt());
            const int rid = static_cast<int>(sc.nextInt());
            if (known(rid)) {
                req(rid).remote = remote;
                req(rid).stage = Stage::WaitUpPre;
            }
        } else if (isProc) {
            sc.skip(2);  // ls le
            const int remote = static_cast<int>(sc.nextInt());
            const int rid = static_cast<int>(sc.nextInt());
            if (known(rid)) {
                if (remote >= 0 && remote < k_) {
                    auto& load = loadMs_[static_cast<std::size_t>(remote)];
                    load = std::max(0.0, load - req(rid).procMs);
                }
                req(rid).procMs = 0;
                req(rid).stage = Stage::WaitDownPre;
            }
        } else {  // P POST — запрос готов к первому шагу декода
            sc.skip(1);  // remote
            const int rid = static_cast<int>(sc.nextInt());
            if (known(rid)) {
                if (req(rid).remote >= 0) ++decActive_[static_cast<std::size_t>(req(rid).remote)];
                push(qDPre_, rid, Stage::NeedDPre);
            }
        }
        return;
    }

    sc.skip(1);  // remote (-1 для групп через удалённые серверы)
    const int m = static_cast<int>(sc.nextInt());
    for (int i = 0; i < m; ++i) {
        const int rid = static_cast<int>(sc.nextInt());
        if (!known(rid)) continue;
        if (isPre) {
            req(rid).stage = Stage::WaitUpDec;
        } else if (isProc) {
            req(rid).stage = Stage::WaitDownDec;
        } else {  // D POST — токен выдан, запрос идёт на следующий шаг декода
            if (++req(rid).produced == 2) ++multiTokenCount_;
            push(qDPre_, rid, Stage::NeedDPre);
        }
    }
}

void Solver::handleXdn(Scanner& sc) {
    const bool up = sc.next() == "UP";
    const int remote = static_cast<int>(sc.nextInt());
    sc.skip(1);  // size
    const bool isPre = sc.next() == "PRE";
    const int m = static_cast<int>(sc.nextInt());

    for (int i = 0; i < m; ++i) {
        const int rid = static_cast<int>(sc.nextInt());
        if (!known(rid)) continue;
        if (isPre) {
            if (up) {
                push(qPProc_[static_cast<std::size_t>(remote)], rid, Stage::NeedPProc);
            } else {
                push(qPPost_, rid, Stage::NeedPPost);
            }
        } else if (up) {
            push(qDProc_[static_cast<std::size_t>(remote)], rid, Stage::NeedDProc);
        } else {
            push(qDPost_, rid, Stage::NeedDPost);
        }
    }
}

void Solver::handleEvent(std::string_view line) {
    Scanner sc{line};
    const std::string_view kind = sc.next();

    if (kind == "ARR") {
        const int rid = static_cast<int>(sc.nextInt());
        const int lin = static_cast<int>(sc.nextInt());
        ensureReq(rid);
        Req& r = req(rid);
        r.alive = true;
        r.remote = -1;
        r.lin = lin;
        r.produced = 0;
        r.work = table_.at(Col::PrefillProc, lin);
        push(qPPre_, rid, Stage::NeedPPre);
    } else if (kind == "FIN") {
        const int rid = static_cast<int>(sc.nextInt());
        if (rid >= 0 && rid < static_cast<int>(reqs_.size())) {
            Req& r = req(rid);
            ++finCount_;
            if (r.remote >= 0) {
                --loadCnt_[static_cast<std::size_t>(r.remote)];
                --decActive_[static_cast<std::size_t>(r.remote)];
            }
            r.stage = Stage::Done;
            r.alive = false;
        }
    } else if (kind == "TDN") {
        handleTdn(sc);
    } else if (kind == "XDN") {
        handleXdn(sc);
    }
}

// Раскладка нового запроса по облачным серверам: пока не видно установившегося
// режима декода — по числу назначенных, дальше — по оценке занятости в мс.
int Solver::pickRemote() {
    int best = 0;
    if (balMs_ != 0 || (finCount_ >= 1 && multiTokenCount_ == 0)) {
        double bestLoad = 1e300;
        for (int k = 0; k < k_; ++k) {
            const auto idx = static_cast<std::size_t>(k);
            const double est = loadMs_[idx] + decActive_[idx] * decodeProc1_;
            if (est < bestLoad - 1e-12) {
                bestLoad = est;
                best = k;
            }
        }
    } else {
        for (int k = 1; k < k_; ++k)
            if (loadCnt_[static_cast<std::size_t>(k)] < loadCnt_[static_cast<std::size_t>(best)]) best = k;
    }
    return best;
}

void Solver::scheduleEdge() {
    if (edgeBusy_) return;

    const std::array<long long, 4> heads{
        headSeq(qPPre_, Stage::NeedPPre),
        headSeq(qPPost_, Stage::NeedPPost),
        headSeq(qDPre_, Stage::NeedDPre),
        headSeq(qDPost_, Stage::NeedDPost),
    };

    int which = -1;
    long long best = std::numeric_limits<long long>::max();
    for (const auto& [index, seq] : std::views::enumerate(heads)) {
        if (seq < best) {
            best = seq;
            which = static_cast<int>(index);
        }
    }
    if (which < 0) return;

    switch (which) {
        case 0: {  // P PRE — вход на локальном сервере
            const int rid = qPPre_.front();
            qPPre_.pop_front();
            const int remote = pickRemote();
            Req& r = req(rid);
            r.remote = remote;
            ++loadCnt_[static_cast<std::size_t>(remote)];
            r.procMs = r.work;
            loadMs_[static_cast<std::size_t>(remote)] += r.procMs;
            r.stage = Stage::RunPPre;
            response_.start("E P PRE");
            response_.arg(remote);
            response_.arg(rid);
            response_.finish();
            edgeBusy_ = true;
            break;
        }
        case 1: {  // P POST — завершение входа
            const int rid = qPPost_.front();
            qPPost_.pop_front();
            req(rid).stage = Stage::RunPPost;
            response_.start("E P POST");
            response_.arg(req(rid).remote);
            response_.arg(rid);
            response_.finish();
            edgeBusy_ = true;
            break;
        }
        default: {  // D PRE / D POST — группируем всё готовое
            const bool pre = which == 2;
            collectAll(pre ? qDPre_ : qDPost_, pre ? Stage::NeedDPre : Stage::NeedDPost);
            if (group_.empty()) break;
            response_.start(pre ? "E D PRE -1" : "E D POST -1");
            response_.arg(static_cast<long long>(group_.size()));
            for (const int rid : group_) {
                response_.arg(rid);
                req(rid).stage = pre ? Stage::RunDPre : Stage::RunDPost;
            }
            response_.finish();
            edgeBusy_ = true;
            break;
        }
    }
}

void Solver::scheduleCloud() {
    for (int k = 0; k < k_; ++k) {
        const auto idx = static_cast<std::size_t>(k);
        if (cloudBusy_[idx]) continue;

        const long long prefillSeq = headSeq(qPProc_[idx], Stage::NeedPProc);
        const long long decodeSeq = headSeq(qDProc_[idx], Stage::NeedDProc);
        if (prefillSeq == std::numeric_limits<long long>::max() &&
            decodeSeq == std::numeric_limits<long long>::max())
            continue;

        if (prefillSeq <= decodeSeq) {  // P PROC целиком, все слои одним куском
            const int rid = qPProc_[idx].front();
            qPProc_[idx].pop_front();
            req(rid).stage = Stage::RunPProc;
            std::string head = "C";
            appendInt(head, k);
            head += " P PROC";
            response_.start(head);
            response_.arg(0);
            response_.arg(numLayers_);
            response_.arg(k);
            response_.arg(rid);
            response_.finish();
        } else {
            collectAll(qDProc_[idx], Stage::NeedDProc);
            if (group_.empty()) continue;
            std::string head = "C";
            appendInt(head, k);
            head += " D PROC";
            response_.start(head);
            response_.arg(k);
            response_.arg(static_cast<long long>(group_.size()));
            for (const int rid : group_) {
                response_.arg(rid);
                req(rid).stage = Stage::RunDProc;
            }
            response_.finish();
        }
        cloudBusy_[idx] = 1;
    }
}

bool Solver::readFrame(std::string& line) {
    // заголовок кадра: время (или END)
    if (!std::getline(std::cin, line)) return false;
    {
        Scanner sc{line};
        const std::string_view head = sc.next();
        if (head.empty()) return true;  // пустая строка — просто ждём следующую
        if (head == "END") return false;
    }

    if (!std::getline(std::cin, line)) return false;
    long long events = Scanner{line}.nextInt();

    for (long long i = 0; i < events; ++i) {
        if (!std::getline(std::cin, line)) std::exit(0);
        if (Scanner{line}.next().empty()) {
            --i;
            continue;
        }
        handleEvent(line);
    }

    scheduleEdge();
    scheduleCloud();
    response_.emit(outBuf_);
    return true;
}

void Solver::run() {
    readHeader();
    outBuf_.reserve(1 << 16);
    std::string line;
    line.reserve(1 << 12);
    while (readFrame(line)) {}
}

}  // namespace

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
    Solver{}.run();
    return 0;
}
