#include "tasksys.h"
#include "itasksys.h"
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemSerial::sync() { return; }

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::updateQueue() {
  size_t n = wait_queue_.size();
  bool promoted = false;
  for (size_t i = 0; i < n; i++) {
    BatchWork *bw = wait_queue_.front();
    wait_queue_.pop();
    bool ready = true;
    for (auto d : bw->deps) {
      if (!done_.count(d)) {
        ready = false;
        break;
      }
    }
    if (ready) {
      ready_queue_.push(bw);
      promoted = true;
    } else {
      wait_queue_.push(bw);
    }
  }
  if (promoted)
    cv_ready_.notify_all();
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads) {
  for (int i = 0; i < num_threads_; i++) {
    ths_.emplace_back([this]() {
      while (true) {
        BatchWork *bw = nullptr;
        int idx = -1;
        int total = -1;
        IRunnable *fn = nullptr;

        {
          std::unique_lock<std::mutex> lk(mtx_);
          cv_ready_.wait(lk, [this] { return stop_ || !ready_queue_.empty(); });
          if (stop_ && ready_queue_.empty())
            break;

          bool found = false;
          size_t qn = ready_queue_.size();
          // 轮转查找可分配任务
          for (size_t r = 0; r < qn; r++) {
            BatchWork *cand = ready_queue_.front();
            ready_queue_.pop();
            int got = cand->next_idx_.fetch_add(1);
            if (got < cand->total_work) {
              ready_queue_.push(cand); // 轮转
              bw = cand;
              idx = got;
              total = cand->total_work;
              fn = cand->func;
              found = true;
              break;
            } else {
              // 任务索引已分发完，放回原队列位置
              ready_queue_.push(cand);
            }
          }
          if (!found) {
            // 没有可执行 index，继续等待新提升
            continue;
          }
        }

        fn->runTask(idx, total);

        {
          std::unique_lock<std::mutex> lk(mtx_);
          int left_after = bw->remained_.fetch_sub(1) - 1;
          if (left_after == 0) {
            done_.insert(bw->batch_id);
            pending_batches_--;
            // 从 ready_queue_ 移除该批
            std::queue<BatchWork *> tmp;
            while (!ready_queue_.empty()) {
              BatchWork *cur = ready_queue_.front();
              ready_queue_.pop();
              if (cur != bw)
                tmp.push(cur);
            }
            ready_queue_.swap(tmp);
            updateQueue();
            if (pending_batches_ == 0)
              cv_done_.notify_all();
            if (!ready_queue_.empty())
              cv_ready_.notify_all();
          }
        }
      }
    });
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    stop_ = true;
  }
  cv_ready_.notify_all();
  for (auto &t : ths_)
    if (t.joinable())
      t.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  std::lock_guard<std::mutex> lk(mtx_);
  
  TaskID id = next_batch_idx_++;
  std::unique_ptr<BatchWork> up(
      new BatchWork(num_total_tasks, id, runnable, deps));
  BatchWork *raw = up.get();
  bw_map_[id] = std::move(up);

  if (num_total_tasks == 0) {
    done_.insert(id);
    updateQueue();
    return id;
  }

  pending_batches_++;
  bool ready = true;
  for (auto d : deps) {
    if (!done_.count(d)) {
      ready = false;
      break;
    }
  }
  if (ready) {
    ready_queue_.push(raw);
    cv_ready_.notify_all();
  } else {
    wait_queue_.push(raw);
  }
  return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  std::unique_lock<std::mutex> lk(mtx_);
  cv_done_.wait(lk, [this]() { return pending_batches_ == 0; });
}
