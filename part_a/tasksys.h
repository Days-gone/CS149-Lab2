#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
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
  int num_threads_;
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

  int num_threads_;
  std::mutex mtx_;
  std::atomic_bool stop_;
  std::atomic_int task_remained_;
  std::atomic_int task_total_;
  std::queue<int> tasks_;
  std::vector<std::thread> ths_;
  IRunnable *cur_func_;
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

  int num_threads_;
  std::mutex mtx_;
  bool stop_;
  int task_total_;
  int task_remained_;
  std::queue<int> tasks_;
  std::condition_variable cv_ready_;
  std::condition_variable cv_done_;
  std::vector<std::thread> ths_;
  IRunnable* cur_func_;
};

#endif
