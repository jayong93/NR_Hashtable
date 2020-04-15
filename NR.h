#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <optional>

using namespace std;

static thread_local unsigned thread_id;
static constexpr unsigned BACKOFF_DELAY = 500;

template <typename T>
T *NUMA_allocation(unsigned numa_id)
{
    return new T;
}

template <typename CMD, typename ARGS>
struct LogEntry
{
    CMD command;
    ARGS args;
    bool is_empty = true;
};

template <typename T, typename CMD, typename ARGS, typename Res>
class NR
{
    using Batch = vector<tuple<unsigned, CMD, ARGS>>;

public:
    NR<T, CMD, ARGS, Res>(unsigned node_num, unsigned core_num_per_node) : node_num{node_num}, core_num_per_node{core_num_per_node}, log{node_num * core_num_per_node * 10}, log_min{node_num * core_num_per_node * 10 - 1}, log_tail{0}, completed_tail{0}, max_batch{core_num_per_node}
    {
        replicas.reserve(node_num);
        local_tails.reserve(node_num);
        combiner_locks.reserve(node_num);
        for (auto i = 0; i < node_num; ++i)
        {
            replicas.emplace_back(NUMA_allocation<T>(i));
            local_tails.emplace_back(NUMA_allocation<atomic<unsigned int>>(i));
            combiner_locks.emplace_back(NUMA_allocation<mutex>(i));

            for (auto j = 0; j < core_num_per_node; ++j)
            {
                op.emplace_back(NUMA_allocation<optional<pair<CMD, ARGS>>>(i));
                response.emplace_back(NUMA_allocation<optional<Res>>(i));
            }
        }
    }

    unsigned int reserve_log(unsigned int size)
    {
        // CAS에서 실패한 경우 old_log_tail이 갱신되므로 여기서만 load하면 충분
        auto old_log_tail = log_tail.load(memory_order_relaxed);
        while (true)
        {
            // 만약 log_tail + size가 log_min 보다 크면 log_min을 다른 thread가 옮겨줄 때까지 대기
            auto target = (old_log_tail + size) % log.size();
            while (log_min.load(memory_order_relaxed) < old_log_tail + size)
            {
                back_off(BACKOFF_DELAY);
            }
            // size만큼 tail을 CAS로 전진, 만약 wrap around가 발생하면 계산해서 정확한 위치를 목표로 CAS
            if (true == log_tail.compare_exchange_strong(old_log_tail, target))
            {
                // 만약 내가 log_min을 update 해야 한다면 update
                auto low_mark = log_min.load(memory_order_relaxed) - core_num_per_node;
                if (low_mark < 0)
                {
                    low_mark = log.size() - low_mark;
                }
                auto is_wraped = is_wraped_around();
                low_mark = get_real_index(is_wraped, low_mark);
                auto real_start = get_real_index(is_wraped, old_log_tail);
                auto real_target = get_real_index(is_wraped, target);

                if (real_start <= low_mark && low_mark < real_target)
                {
                    update_log_min();
                }
                // CAS에 성공하면 할당받은 첫번째 entry index를 반환
                return old_log_tail;
            }
        }
    }

    void update_log_min()
    {
        bool is_wraped = is_wraped_around();

        unsigned min_tail = log.size() * 2;

        for (auto local_tail_ptr : local_tails)
        {
            auto local_tail = get_real_index(is_wraped, local_tail_ptr->load(memory_order_acquire));
            if (local_tail < min_tail)
            {
                min_tail = local_tail;
            }
        }

        log_min.store(min_tail % log.size(), memory_order_relaxed);
    }

    // delay 만큼 대기
    void back_off(unsigned int delay) const
    {
    }

    Res execute(const CMD &cmd, const ARGS &args)
    {
        if (T::is_read_only(cmd))
        {
            return execute_read_only(cmd, args, thread_id);
        }
        else
        {
            return combine(cmd, args, thread_id);
        }
    }

    Res execute_read_only(const CMD &cmd, const ARGS &args)
    {
        auto read_tail = completed_tail.load(memory_order_relaxed);
        auto node_id = get_node_id();
        auto& rw_lock = *rw_locks[node_id];

        while(true) {
            unique_lock<shared_mutex> lock{rw_lock, try_to_lock};
            if(lock)
            {
                update_replica(node_id, read_tail);
            }
            else
            {
                back_off(BACKOFF_DELAY);
            }
        }

        shared_lock<shared_mutex> lock{rw_lock};
        return replicas[node_id]->execute(cmd, args);
    }

    Res combine(const CMD &cmd, const ARGS &args)
    {
        auto node_id = get_node_id();
        auto &combiner_lock = *combiner_locks[node_id];
        auto &rw_lock = *rw_locks[node_id];
        auto &res = *response[thread_id];

        res.reset();
        *op[thread_id] = make_pair<CMD, ARGS>(cmd, args);

        // combiner lock 얻기를 시도한다.
        unique_lock<mutex> lock{combiner_lock, try_to_lock};
        while (true)
        {
            // lock을 얻는데에 성공했으면
            if (lock)
            {
                // 각 thread의 local op 중 등록된 것들을 모은다
                auto batch = collect_batch();
                // log entry를 등록된 op들 수 만큼 할당 받고 op들을 등록한다.
                auto start_index = update_log(batch);
                // writer lock을 얻고 node replica를 start entry까지 update 한 뒤 local tail을 update 한다.
                auto w_lock = unique_lock<shared_mutex>{rw_lock};
                update_replica(node_id, start_index);
                // completed tail이 할당받은 마지막 entry index가 되도록 CAS를 시도한다.
                update_completed_tail((start_index + batch.size()) % log.size());
                // response에 결과들을 등록 한다.
                write_responses(batch, node_id);

                return *res;
            }
            // 실패한 경우
            else
            {
                // 자신의 response가 갱신이 되거나 combiner lock이 해제될 때까지 대기한다.
                while (!res && !(lock = unique_lock<mutex>{combiner_lock, try_to_lock}))
                {
                    back_off(BACKOFF_DELAY);
                }
                // response가 갱신됐다면 그 결과를 반환한다.
                if (res)
                {
                    return *res;
                }
            }
        }
    }

    Batch collect_batch() const
    {
        Batch batch;
        for (auto i = 0; i < op.size(); ++i)
        {
            auto &o = *op[i];
            if (o)
            {
                batch.emplace_back(i, o->first, o->second);
            }
        }
        return batch;
    }

    unsigned int update_log(const Batch &batch)
    {
        const auto start_idx = reserve_log(batch.size());
        for (auto i = 0; i < batch.size(); ++i)
        {
            const auto &[_, cmd, args] = batch[i];
            auto &entry = log[(start_idx + i) % log.size()];
            entry.command = cmd;
            entry.args = args;
            entry.is_empty = false;
        }
        return start_idx;
    }

    void update_replica(unsigned node_id, unsigned start_index)
    {
        auto &replica = *replicas[node_id];
        auto &local_tail = *local_tails[node_id];

        // 이 함수를 호출하기 전에 writer lock을 걸기 때문에 local tail과 replica를 마음껏 수정해도 된다.
        unsigned l_tail = local_tail.load(memory_order_relaxed);
        while (l_tail != start_index)
        {
            while (log[l_tail].is_empty)
            {
                back_off(BACKOFF_DELAY);
            }
            replica.execute(log[l_tail].command, log[l_tail].args);

            l_tail = (l_tail + 1) % log.size();
            local_tail.store(l_tail, memory_order_release);
        }
    }

    void update_completed_tail(unsigned last_index)
    {
        unsigned local_c_tail = completed_tail.load(memory_order_relaxed);
        while (true)
        {
            if (completed_tail.compare_exchange_strong(local_c_tail, last_index))
                break;

            bool is_wraped = is_wraped_around();
            unsigned real_c_tail = get_real_index(is_wraped, local_c_tail);
            unsigned real_last_index = get_real_index(is_wraped, last_index);
            if (real_last_index < real_c_tail)
                break;
        }
    }

    void write_response(const Batch& batch, T& replica)
    {
        for(auto& [id, cmd, args] : batch)
        {
            *response[id] = replica.execute(cmd, args);
        }
    }

    bool is_wraped_around() const
    {
        return log_min.load(memory_order_relaxed) != (log.size() - 1);
    }

    unsigned int get_node_id() const
    {
        return thread_id / core_num_per_node;
    }

    // log_min의 위치에 따라서 확장된 index를 반환
    // log_min이 log.size() - 1이 아니라면 log_min보다 작거나 같은 index가 사실은 log_min보다 큰 index보다 더 크다.
    unsigned get_real_index(bool is_wraped, unsigned index)
    {
        if (index <= log_min.load(memory_order_relaxed))
        {
            return index + log.size();
        }
        return index;
    }

    void init_per_thread()
    {
        thread_id = id_counter.fetch_add(1, memory_order_relaxed);
        // 각 thread를 id에 맞춰서 NUMA Node에 pinning
    }

private:
    vector<LogEntry<CMD, ARGS>> log;
    atomic<unsigned int> log_tail;
    atomic<unsigned int> completed_tail;
    atomic<unsigned int> log_min;
    const unsigned int max_batch;
    const unsigned int node_num;
    const unsigned int core_num_per_node;
    atomic_uint id_counter{0};

    // Node local data
    vector<T *> replicas;
    vector<atomic<unsigned int> *> local_tails;
    vector<mutex *> combiner_locks;
    vector<shared_mutex *> rw_locks;

    // Thread local data
    vector<optional<pair<CMD, ARGS>> *> op;
    vector<optional<Res> *> response;
};