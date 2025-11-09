#include "tasksys.h"
#include "itasksys.h"
#include <cstddef>
#include <mutex>
#include <thread>

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
  // You do not need to implement this method.
  return 0;
}

void TaskSystemSerial::sync() {
  // You do not need to implement this method.
  return;
}

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
  num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
  /*
      这样的实现方法，通过限制最大的线程数取num_threads而不是num_total_tasks，
      防止了超多任务数量下产生过多的threads导致system被卡死的风险。
      但是这样的实现方法对这个 alwals spawn 这个名词是对应的么？
  */
  std::vector<std::thread> ths_;
  int batch_size = std::ceil(float(num_total_tasks) / this->num_threads_);

  for (int i = 0; i < num_threads_; i++) {
    ths_.emplace_back([=, &runnable]() {
      int st = i * batch_size;
      int ed = std::min((i + 1) * batch_size, num_total_tasks);
      while (st < ed) {
        runnable->runTask(st, num_total_tasks);
        st++;
      }
    });
  }

  for (auto &t : ths_) {
    t.join();
  }
  ths_.clear();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // You do not need to implement this method.
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
    : ITaskSystem(num_threads), num_threads_(num_threads), stop_(false),
      task_remained_(0), task_total_(0), cur_func_(nullptr) {

  for (int i = 0; i < num_threads_; i++) {
    ths_.emplace_back([this]() {
      while (true) {

        // 先检查是否退出，顺带获取任务id
        bool empty = false;
        int idx = -1;
        IRunnable *local_func_{nullptr};
        {
          std::lock_guard<std::mutex> lg(mtx_);
          empty = tasks_.empty();
          if (!tasks_.empty()) {
            idx = tasks_.front();
            tasks_.pop();
          }
          local_func_ = cur_func_;
        }
        if (stop_ && empty) {
          break;
        }
        if (empty || local_func_ == nullptr) {
          continue;
        }
        // 执行任务
        local_func_->runTask(idx, this->task_total_);
        task_remained_--;
      }
    });
  }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  stop_ = true;
  for (auto &t : ths_) {
    t.join();
  }
  ths_.clear();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {
  {
    std::lock_guard<std::mutex> lg(mtx_);
    while (!tasks_.empty())
      tasks_.pop();

    cur_func_ = runnable;
    task_total_ = num_total_tasks;
    task_remained_ = num_total_tasks;
    for (int i = 0; i < num_total_tasks; i++) {
      tasks_.push(i);
    }
  }
  while (task_remained_ > 0) {
    std::this_thread::yield();
  }
  {
    std::lock_guard<std::mutex> lg(mtx_);
    cur_func_ = nullptr;
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // You do not need to implement this method.
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
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //

  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {

  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}
