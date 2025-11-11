#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

  struct TaskArgs {
    int idx{0};
    int total{0};
    TaskID global_id;
    IRunnable *func{nullptr};
    std::vector<TaskID> deps;
    public:
    TaskArgs(int idx, int total, int global_id, IRunnable* func, std::vector<TaskID> deps) : idx(idx),total(total), global_id(global_id), func(func) {
      this->deps = deps;
    }
    TaskArgs() = default;
  };

  int num_threads_;
  int taskid_gennerator_;
  int pending_batches_;
  bool stop_;
  std::mutex mtx_;
  std::set<TaskID> done_;
  std::unordered_map<TaskID, int> count_;
  std::vector<std::thread> ths_;
  std::queue<TaskArgs> wait_queue_;
  std::queue<TaskArgs> ready_queue_;
  std::condition_variable cv_done_;
  std::condition_variable cv_ready_;

};

#endif
