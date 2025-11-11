#include "tasksys.h"
#include "itasksys.h"
#include <mutex>
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads),
      taskid_gennerator_(0), pending_batches_(0), stop_(false) {
  ths_.reserve(num_threads_);
  for (int i = 0; i < num_threads_; i++) {
    ths_.emplace_back([this]() {
      while (true) {
        // 尝试从ready queue获取任务
        TaskArgs local_task;
        {
          std::unique_lock<std::mutex> lg(mtx_);
          cv_ready_.wait(
              lg, [this]() { return !this->ready_queue_.empty() || stop_; });
          if (this->ready_queue_.empty() && stop_) {
            break;
          }
          local_task = ready_queue_.front();
          ready_queue_.pop();
        }

        // 开始执行任务
        local_task.func->runTask(local_task.idx, local_task.total);

        {
          // 检查这次执行完成后，是否本批次任务全部执行完
          std::unique_lock<std::mutex> lg(mtx_);
          count_[local_task.global_id]++;

          // 是否需要修改ready & wait queue
          if (count_[local_task.global_id] == local_task.total) {
            this->done_.insert(local_task.global_id);
            pending_batches_--;
            cv_done_.notify_all();

            std::vector<TaskArgs> temp;
            bool promote = false;
            while (!wait_queue_.empty()) {
              auto task = wait_queue_.front();
              wait_queue_.pop();
              bool is_ready = true;
              for (auto &bid : task.deps) {
                if (this->done_.count(bid) == 0) {
                  is_ready = false;
                  break;
                }
              }
              if (is_ready) {
                this->ready_queue_.push(task);
                promote = true;
              } else {
                temp.emplace_back(task);
              }
            }

            for (auto &task : temp) {
              this->wait_queue_.push(std::move(task));
            }
            if (promote) {
              cv_ready_.notify_all();
            }
          }
        }
      }
    });
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  {
    std::lock_guard<std::mutex> lg(mtx_);
    stop_ = true;
  }
  cv_ready_.notify_all();

  for (auto &t : ths_) {
    if (t.joinable())
      t.join();
  }
  ths_.clear();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  std::lock_guard<std::mutex> lg(mtx_);
  TaskID this_batch_taskid = this->taskid_gennerator_++;

  // fast return
  if (num_total_tasks == 0) {
    done_.insert(this_batch_taskid);
    cv_done_.notify_all();
    return this_batch_taskid;
  }

  pending_batches_++;
  bool deps_ready = true;
  for (auto &d : deps) {
    if (done_.count(d) == 0) {
      deps_ready = false;
      break;
    }
  }

  for (int i = 0; i < num_total_tasks; i++) {
    TaskArgs ta{i, num_total_tasks, this_batch_taskid, runnable, deps};
    if (deps_ready) {
      ready_queue_.push(ta);
    } else {
      wait_queue_.push(ta);
    }
  }

  if (deps_ready) {
    cv_ready_.notify_all();
  }
  return this_batch_taskid;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  std::unique_lock<std::mutex> lg(mtx_);
  cv_done_.wait(lg, [this]() { return pending_batches_ == 0; });
}
