#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <optional>
#include <cassert>

using namespace std;

static thread_local unsigned thread_id;
static constexpr unsigned BACKOFF_DELAY = 500;

template <typename T, typename... Vals>
T *NUMA_allocation(unsigned numa_id, Vals &&... val)
{
    return new T{forward<Vals>(val)...};
}

template <typename CMD, typename ARGS>
struct LogEntry
{
    CMD command;
    ARGS args;
    volatile bool is_empty = true;
};

template <typename T, typename CMD, typename ARGS, typename Res>
class NR
{
    using Batch = vector<tuple<unsigned, CMD, ARGS>>;

public:
    NR<T, CMD, ARGS, Res>(unsigned node_num, unsigned core_num_per_node) : node_num{node_num}, core_num_per_node{core_num_per_node}, log{node_num * core_num_per_node * 10}, log_min{node_num * core_num_per_node * 10}, log_tail{0}, completed_tail{0}, max_batch{core_num_per_node}
    {
        for (auto i = 0; i < node_num; ++i)
        {
            replicas.emplace_back(NUMA_allocation<T>(i));
            local_tails.emplace_back(NUMA_allocation<atomic<uint64_t>>(i, (uint64_t)0));
            combiner_locks.emplace_back(NUMA_allocation<mutex>(i));
            rw_locks.emplace_back(NUMA_allocation<shared_mutex>(i));

            for (auto j = 0; j < core_num_per_node; ++j)
            {
                op.emplace_back(NUMA_allocation<optional<pair<CMD, ARGS>>>(i));
                response.emplace_back(NUMA_allocation<optional<Res>>(i));
            }
        }
    }

    Res execute(const CMD &cmd, const ARGS &args)
    {
        if (T::is_read_only(cmd))
        {
            return execute_read_only(cmd, args);
        }
        else
        {
            return combine(cmd, args);
        }
    }

    void init_per_thread()
    {
        thread_id = id_counter.fetch_add(1, memory_order_relaxed);
        // 각 thread를 id에 맞춰서 NUMA Node에 pinning
    }

    const T &get_latest_replica() const
    {
        unsigned max_tail = 0;
        unsigned max_node = 0;
        for (auto i = 0; i < node_num; ++i)
        {
            auto node_tail = local_tails[i]->load(memory_order_acquire);
            if (max_tail < node_tail)
            {
                max_tail = node_tail;
                max_node = i;
            }
        }
        return *replicas[max_node];
    }

private:
    vector<LogEntry<CMD, ARGS>> log;
    atomic<uint64_t> log_tail;
    atomic<uint64_t> completed_tail;
    atomic<uint64_t> log_min;
    const uint64_t max_batch;
    const uint64_t node_num;
    const uint64_t core_num_per_node;
    atomic_uint id_counter{0};

    // Node local data
    vector<T *> replicas;
    vector<atomic<uint64_t> *> local_tails;
    vector<mutex *> combiner_locks;
    vector<shared_mutex *> rw_locks;

    // Thread local data
    vector<optional<pair<CMD, ARGS>> *> op;
    vector<optional<Res> *> response;

    unsigned int reserve_log(unsigned int size)
    {
        // CAS에서 실패한 경우 old_log_tail이 갱신되므로 여기서만 load하면 충분
        auto old_log_tail = log_tail.load(memory_order_relaxed);
        while (true)
        {
            // end_entry가 log_min보다 크거나 같으면 다른 thread가 log_min을 업데이트 하기를 기다림
            while (log_min.load(memory_order_relaxed) < old_log_tail + size)
            {
                back_off(BACKOFF_DELAY);
            }
            // size만큼 tail을 CAS로 전진, 만약 wrap around가 발생하면 계산해서 정확한 위치를 목표로 CAS
            auto target = old_log_tail + size;
            if (true == log_tail.compare_exchange_strong(old_log_tail, target))
            {
                // low mark에 해당하는 entry를 가졌다면 log min을 갱신
                auto old_log_min = log_min.load(memory_order_relaxed);
                auto low_mark = old_log_min - core_num_per_node;
                if (old_log_tail <= low_mark && low_mark < target)
                {
                    update_log_min();
                    // while(log_min.load(memory_order_relaxed) - old_log_min < core_num_per_node)
                    // {
                    //     back_off(BACKOFF_DELAY);
                    //     update_log_min();
                    // }
                }
                // CAS에 성공하면 할당받은 첫번째 entry index를 반환
                return old_log_tail;
            }
        }
    }

    void update_log_min()
    {
        auto min_tail = UINT64_MAX;

        for (auto local_tail_ptr : local_tails)
        {
            auto local_tail = local_tail_ptr->load(memory_order_acquire);
            if (local_tail < min_tail)
            {
                min_tail = local_tail;
            }
        }
        min_tail += log.size();

        auto local_log_min = log_min.load(memory_order_relaxed) + 1;
        while (local_log_min < min_tail)
        {
            log[local_log_min % log.size()].is_empty = true;
            ++local_log_min;
        }

        log_min.store(min_tail, memory_order_release);
    }

    // delay 만큼 대기
    void back_off(unsigned int delay) const
    {
        for (volatile unsigned long long i = 0; i < delay; ++i)
        {
        }
    }

    Batch collect_batch() const
    {
        Batch batch;
        auto node_id = get_node_id();
        for (auto i = node_id * core_num_per_node; i < ((node_id + 1) * core_num_per_node); ++i)
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

    void update_replica(T &replica, unsigned node_id, uint64_t start_index, const unique_lock<shared_mutex> &lg)
    {
        assert(lg && "the writer lock has not been acquired");
        auto &local_tail = *local_tails[node_id];

        // 이 함수를 호출하기 전에 writer lock을 걸기 때문에 local tail과 replica를 마음껏 수정해도 된다.
        unsigned l_tail = local_tail.load(memory_order_relaxed);
        while (l_tail < start_index)
        {
            const auto idx = l_tail % log.size();
            while (log[idx].is_empty)
            {
                back_off(BACKOFF_DELAY);
            }
            replica.execute(log[idx].command, log[idx].args);

            ++l_tail;
        }
        local_tail.store(l_tail, memory_order_release);
    }

    void update_completed_tail(uint64_t last_index)
    {
        auto local_c_tail = completed_tail.load(memory_order_relaxed);
        while (true)
        {
            if (completed_tail.compare_exchange_strong(local_c_tail, last_index))
                break;

            if (last_index < local_c_tail)
                break;
        }
    }

    void write_responses(const Batch &batch, T &replica)
    {
        for (auto &[id, cmd, args] : batch)
        {
            *response[id] = replica.execute(cmd, args);
        }
    }

    unsigned int get_node_id() const
    {
        return thread_id / core_num_per_node;
    }

    Res execute_read_only(const CMD &cmd, const ARGS &args)
    {
        auto read_tail = completed_tail.load(memory_order_relaxed);
        auto node_id = get_node_id();
        auto &rw_lock = *rw_locks[node_id];
        auto &local_tail = *local_tails[node_id];
        auto &replica = *replicas[node_id];

        while (local_tail.load(memory_order_acquire) < read_tail)
        {
            unique_lock<shared_mutex> lock{rw_lock, try_to_lock};
            if (lock)
            {
                update_replica(replica, node_id, read_tail, lock);
            }
            else
            {
                back_off(BACKOFF_DELAY);
            }
        }

        shared_lock<shared_mutex> lock{rw_lock};
        return replica.execute(cmd, args);
    }

    Res combine(const CMD &cmd, const ARGS &args)
    {
        auto node_id = get_node_id();
        auto &combiner_lock = *combiner_locks[node_id];
        auto &rw_lock = *rw_locks[node_id];
        auto &res = *response[thread_id];
        auto &replica = *replicas[node_id];

        res.reset();
        op[thread_id]->emplace(cmd, args);

        // 최대한 완료된 곳(completed tail)까지 local tail 및 replica를 갱신한다.
        // 실행이 지연된 Node의 thread가 여기에서 local tail을 갱신해야 log_min을 갱신하는 thread가 진행 가능
        // {
        //     auto w_lock = unique_lock<shared_mutex>{rw_lock};
        //     update_replica(replica, node_id, completed_tail.load(memory_order_acquire), w_lock);
        // }
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
                update_replica(replica, node_id, start_index, w_lock);
                // local tail을 업데이트 한다.
                local_tails[node_id]->store(start_index + batch.size(), memory_order_release);
                // completed tail이 할당받은 마지막 entry index가 되도록 CAS를 시도한다.
                update_completed_tail((start_index + batch.size()) % log.size());
                // response에 결과들을 등록 한다.
                write_responses(batch, replica);

                return *res;
            }
            // 실패한 경우
            else
            {
                // 자신의 response가 갱신이 되거나 combiner lock이 해제될 때까지 대기한다.
                while (!res && !lock.try_lock())
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
};