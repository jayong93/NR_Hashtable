#include <iostream>
#include <unordered_map>
#include <variant>
#include <chrono>
#include <thread>
#include <random>
#include "NR.h"
#include "rand_seeds.h"

#ifndef WRITE_RATIO
#define WRITE_RATIO 30
#endif

using namespace std;
using namespace chrono;

struct HashTable
{
    enum class CMD
    {
        Insert,
        Remove,
        Contains
    };
    using ARGS = pair<unsigned, unsigned>;
    using Res = unsigned;

    unordered_map<unsigned, unsigned> map;

    static bool is_read_only(const CMD &cmd)
    {
        return cmd == CMD::Contains;
    }

    Res execute(const CMD &cmd, const ARGS &args)
    {
        switch (cmd)
        {
        case CMD::Insert:
            return map.insert(args).second;
        case CMD::Remove:
            return map.erase(args.first);
        case CMD::Contains:
            return map.find(args.first) != map.end();
        default:
            return -1;
        }
    }
};

constexpr unsigned NUM_TEST = 4'000'000;
constexpr unsigned RANGE = 1'000;

using NR_HashTable = NR<HashTable, HashTable::CMD, HashTable::ARGS, HashTable::Res>;

void benchmark(uint num_thread, NR_HashTable *table)
{
    mt19937_64 rng{rand_seeds[thread_id]};
#ifdef RANGE_LIMIT
    uniform_int_distribution<unsigned long> dist{0, RANGE-1};
#else
    uniform_int_distribution<unsigned long> dist;
#endif
    uniform_int_distribution<unsigned long> cmd_dist{0, 99};

    table->init_per_thread();
    for (int i = 0; i < NUM_TEST / num_thread; ++i)
    {
        if (cmd_dist(rng) < WRITE_RATIO) {
            if (cmd_dist(rng) < 50) {
                table->execute(HashTable::CMD::Insert, make_pair(dist(rng), dist(rng)));
            }
            else {
                table->execute(HashTable::CMD::Remove, make_pair(dist(rng), 0));
            }
        }
        else {
                table->execute(HashTable::CMD::Contains, make_pair(dist(rng), 0));
        }
#ifdef DEBUG
        if (i % 500 == 0)
            printf("Thread #%d run operation #%d\n", thread_id, i);
#endif
    }
}

int main(int argc, char *argv[])
{
    printf("[INFO] node_num = %d, cpu num = %d\n", numa_num_configured_nodes(), numa_num_configured_cpus());
    for (uint num_thread = 1; num_thread <= 32; num_thread *= 2)
    {
        NR_HashTable nr_table{num_thread};

        vector<thread> worker;
        auto start_t = high_resolution_clock::now();
        for (uint i = 0; i < num_thread; ++i)
            worker.emplace_back(benchmark, num_thread, &nr_table);
        for (auto &th : worker)
            th.join();
        auto du = high_resolution_clock::now() - start_t;

        auto i = 0;
        for (auto &[key, value] : nr_table.get_latest_replica().map)
        {
            printf("{%d, %d},", key, value);
            if (++i >= 10)
                break;
        }
        printf("\n");

        printf("%d Threads, Time=%lld ms\n", num_thread, duration_cast<milliseconds>(du).count());
        fflush(NULL);
    }
}
