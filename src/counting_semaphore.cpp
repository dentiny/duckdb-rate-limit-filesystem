#include "counting_semaphore.hpp"

#include <utility>

namespace duckdb {

CountingSemaphore::CountingSemaphore(int64_t max_count) : max_count(max_count), current_count(0) {
}

void CountingSemaphore::Acquire() {
	concurrency::unique_lock<concurrency::mutex> lock(mtx);
	if (max_count == UNLIMITED) {
		return;
	}
	cv.wait(lock, [this] { return current_count < max_count || max_count == UNLIMITED; });
	if (max_count != UNLIMITED) {
		++current_count;
	}
}

void CountingSemaphore::Release() {
	{
		concurrency::lock_guard<concurrency::mutex> guard(mtx);
		if (max_count == UNLIMITED) {
			return;
		}
		--current_count;
	}
	cv.notify_one();
}

SemaphoreGuard CountingSemaphore::AcquireGuard() {
	if (GetMax() == UNLIMITED) {
		return SemaphoreGuard();
	}
	return SemaphoreGuard(this);
}

void CountingSemaphore::SetMax(int64_t new_max) {
	{
		concurrency::lock_guard<concurrency::mutex> guard(mtx);
		max_count = new_max;
	}
	cv.notify_all();
}

int64_t CountingSemaphore::GetMax() const {
	concurrency::lock_guard<concurrency::mutex> guard(mtx);
	return max_count;
}

int64_t CountingSemaphore::GetCurrent() const {
	concurrency::lock_guard<concurrency::mutex> guard(mtx);
	return current_count;
}

SemaphoreGuard::SemaphoreGuard() : sem(nullptr) {
}

SemaphoreGuard::SemaphoreGuard(CountingSemaphore *sem) : sem(sem) {
	if (sem) {
		sem->Acquire();
	}
}

SemaphoreGuard::~SemaphoreGuard() {
	if (sem) {
		sem->Release();
	}
}

SemaphoreGuard::SemaphoreGuard(SemaphoreGuard &&other) noexcept : sem(other.sem) {
	other.sem = nullptr;
}

SemaphoreGuard &SemaphoreGuard::operator=(SemaphoreGuard &&other) noexcept {
	if (this != &other) {
		if (sem) {
			sem->Release();
		}
		sem = other.sem;
		other.sem = nullptr;
	}
	return *this;
}

} // namespace duckdb
