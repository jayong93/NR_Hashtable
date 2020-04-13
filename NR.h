#include <vector>
#include <atomic>
#include <mutex>

using std::vector, std::atomic, std::mutex;

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

template <typename T, typename CMD, typename ARGS>
class NR
{
public:
    NR<T, CMD, ARGS>(unsigned node_num, unsigned core_num_per_node) : log{node_num * core_num_per_node * 10}, log_min{node_num * core_num_per_node * 10 - 1}, log_tail{0}, completed_tail{0}
    {
        replicas.reserve(node_num);
        local_tails.reserve(node_num);
        combiner_locks.reserve(node_num);
        for (auto i = 0; i < node_num; ++i)
        {
            replicas.emplace_back(NUMA_allocation<T>(i));
            local_tails.emplace_back(NUMA_allocation<atmoic<unsigned int>>(i));
            combiner_locks.emplace_back(NUMA_allocation<mutex>(i));
        }
    }

private:
    vector<LogEntry<CMD, ARGS>> log;
    atomic<unsigned int> log_tail;
    atomic<unsigned int> completed_tail;
    unsigned int log_min;

    vector<T *> replicas;
    vector<atomic<unsigned int> *> local_tails;
    vector<mutex *> combiner_locks;
};