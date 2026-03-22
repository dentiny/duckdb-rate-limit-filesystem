#include "catch/catch.hpp"

#include "counting_semaphore.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace duckdb;

TEST_CASE("CountingSemaphore - unlimited is no-op", "[counting_semaphore]") {
	CountingSemaphore sem;
	REQUIRE(sem.GetMax() == CountingSemaphore::UNLIMITED);
	sem.Acquire();
	REQUIRE(sem.GetCurrent() == 0);
	sem.Release();
	REQUIRE(sem.GetCurrent() == 0);
}

TEST_CASE("CountingSemaphore - blocks at max and unblocks on release", "[counting_semaphore]") {
	CountingSemaphore sem(1);
	sem.Acquire();

	std::atomic<bool> acquired {false};
	std::thread t([&] {
		sem.Acquire();
		acquired.store(true);
		sem.Release();
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE_FALSE(acquired.load());

	sem.Release();
	t.join();
	REQUIRE(acquired.load());
}

TEST_CASE("CountingSemaphore - multiple threads respect limit", "[counting_semaphore]") {
	constexpr int64_t MAX_CONCURRENT = 3;
	constexpr int NUM_THREADS = 10;
	CountingSemaphore sem(MAX_CONCURRENT);

	std::atomic<int64_t> concurrent_count {0};
	std::atomic<int64_t> max_observed {0};

	auto worker = [&] {
		for (int i = 0; i < 5; i++) {
			sem.Acquire();
			auto current = ++concurrent_count;
			auto prev_max = max_observed.load();
			while (current > prev_max && !max_observed.compare_exchange_weak(prev_max, current)) {
			}
			std::this_thread::sleep_for(std::chrono::microseconds(100));
			--concurrent_count;
			sem.Release();
		}
	};

	std::vector<std::thread> threads;
	threads.reserve(NUM_THREADS);
	for (int i = 0; i < NUM_THREADS; i++) {
		threads.emplace_back(worker);
	}
	for (auto &t : threads) {
		t.join();
	}

	REQUIRE(max_observed.load() <= MAX_CONCURRENT);
	REQUIRE(max_observed.load() > 1);
	REQUIRE(sem.GetCurrent() == 0);
}
