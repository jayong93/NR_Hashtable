#include <iostream>
#include <unordered_map>
#include <variant>
#include <chrono>
#include <thread>
#include "NR.h"

using namespace std;
using namespace chrono;

unsigned long fastrand(void)
{ //period 2^96-1
    static thread_local unsigned long x = 123456789, y = 362436069, z = 521288629;
    unsigned long t;
    x ^= x << 16;
    x ^= x >> 5;
    x ^= x << 1;

    t = x;
    x = y;
    y = z;
    z = t ^ x ^ y;

    return z;
}

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
    table->init_per_thread();
    for (int i = 0; i < NUM_TEST / num_thread; ++i)
    {
        //	if (0 == i % 100000) cout << ".";
        switch (fastrand() % 3)
        {
        case 0:
            table->execute(HashTable::CMD::Insert, make_pair(fastrand() % RANGE, fastrand()));
            break;
        case 1:
            table->execute(HashTable::CMD::Remove, make_pair(fastrand() % RANGE, 0));
            break;
        case 2:
            table->execute(HashTable::CMD::Contains, make_pair(fastrand() % RANGE, 0));
            break;
        default:
            printf("Unknown Command!\n");
            exit(-1);
        }
    }
}

int main()
{
    for (uint num_thread = 1; num_thread < 32; num_thread *= 2)
    {
        NR_HashTable nr_table{1, num_thread};

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