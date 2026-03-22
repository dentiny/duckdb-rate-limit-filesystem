#pragma once

#include <condition_variable>
#include <cstdint>

#include "mutex.hpp"

namespace duckdb {

// Forward declaration.
class SemaphoreGuard;

// Thread-safe counting semaphore. When max_count is UNLIMITED (-1), Acquire/Release are no-ops.
class CountingSemaphore {
	friend class SemaphoreGuard;

public:
	static constexpr int64_t UNLIMITED = -1;

	explicit CountingSemaphore(int64_t max_count = UNLIMITED);

	void Acquire();
	void Release();
	[[nodiscard]] SemaphoreGuard AcquireGuard();

	void SetMax(int64_t new_max);
	int64_t GetMax() const;
	int64_t GetCurrent() const;

private:
	mutable concurrency::mutex mtx;
	std::condition_variable_any cv DUCKDB_GUARDED_BY(mtx);
	int64_t max_count DUCKDB_GUARDED_BY(mtx);
	int64_t current_count DUCKDB_GUARDED_BY(mtx);
};

// RAII guard for CountingSemaphore.
class SemaphoreGuard {
public:
	SemaphoreGuard();
	explicit SemaphoreGuard(CountingSemaphore *sem);
	~SemaphoreGuard();

	SemaphoreGuard(SemaphoreGuard &&other) noexcept;
	SemaphoreGuard &operator=(SemaphoreGuard &&other) noexcept;

	SemaphoreGuard(const SemaphoreGuard &) = delete;
	SemaphoreGuard &operator=(const SemaphoreGuard &) = delete;

private:
	CountingSemaphore *sem;
};

} // namespace duckdb
