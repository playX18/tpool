use std::{
    mem::size_of,
    num::NonZeroUsize,
    sync::{ atomic::{ AtomicBool, AtomicUsize, Ordering }, Arc, Barrier, Condvar, Mutex },
};

use bheap::{ BinaryMaxHeap, Uid };
use crossbeam_queue::ArrayQueue;
use sys::pin_thread_to_core;

mod sys;

/// A trait representing task.

pub trait Task: Send + 'static {
    /// Run the task.
    ///
    /// Accepts `pool` as an argument which allows
    /// to spawn new tasks using [`Pool::schedule`](Pool::schedule)
    fn run(&self, pool: &Arc<Pool>);
}

/// The maximum number of jobs we can store in a local queue before overflowing
/// to the global queue.
///
/// The exact value here doesn't really matter as other threads may steal from
/// our local queue, as long as the value is great enough to avoid excessive
/// overflowing in most common cases.
const LOCAL_QUEUE_CAPACITY: usize = 2048 / size_of::<Box<dyn Task>>();
/// The maximum number of jobs to steal at a time.
///
/// This puts an upper bound on the time spent stealing from a single queue.
const STEAL_LIMIT: usize = 32;

/// The shared half of a thread.
struct Shared {
    id: usize,
    /// The queue threads can steal work from.
    queue: Arc<ArrayQueue<Box<dyn Task>>>,
}

struct Worker {
    id: usize,
    /// The thread-local queue new work is scheduled onto, unless we consider it
    /// to be too full.
    work: Arc<ArrayQueue<Box<dyn Task>>>,

    /// The pool this thread belongs to.
    pool: Arc<Pool>,
}

impl PartialEq for Shared {
    fn eq(&self, other: &Self) -> bool {
        // workers are equal if they have the same amount of tasks
        self.queue.len() == other.queue.len()
    }
}

impl Eq for Shared {}

impl Ord for Shared {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.queue.len().cmp(&other.queue.len())
    }
}

impl PartialOrd for Shared {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Uid for Shared {
    fn uid(&self) -> u64 {
        self.id as _
    }
}

/// A thread-pool type.
///
/// Allows to spawn tasks that will be executed in background. It's up to the user of API to
/// control how tasks are spawned
pub struct Pool {
    workers: Mutex<BinaryMaxHeap<Shared>>,
    global: Mutex<Vec<Box<dyn Task>>>,
    global_cv: Condvar,
    alive: AtomicBool,
    sleeping: AtomicUsize,
    sleeping_cvar: Condvar,
    /// Upper limit for amount of workers spawned
    pub upper_limit: usize,
    task_manager_spawned: AtomicBool,
}

impl Pool {
    /// Create new thread-pool.
    ///
    /// This function will spawn task-manager background thread which is responsible for
    /// simple load-balancing of newly scheduled tasks.
    ///
    /// `parallelism` option controls how many threads we can spawn during runtime. If it is `None`
    /// then [`std::thread::available_parallelism`] is used.
    pub fn new(parallelism: Option<NonZeroUsize>) -> Arc<Self> {
        let this = Arc::new(Self {
            workers: Mutex::new(BinaryMaxHeap::new()),
            global: Mutex::new(Vec::new()),
            global_cv: Condvar::new(),
            alive: AtomicBool::new(true),
            sleeping: AtomicUsize::new(0),
            sleeping_cvar: Condvar::new(),
            upper_limit: parallelism
                .or_else(|| std::thread::available_parallelism().ok())
                .map(|x| x.get())
                .unwrap_or(4),
            task_manager_spawned: AtomicBool::new(false),
        });
        this.run();
        this
    }

    fn run(self: &Arc<Pool>) {
        let pool = self.clone();
        let b1 = Arc::new(Barrier::new(2));
        let b2 = b1.clone();
        let _ = std::thread::spawn(move || {
            b2.wait();
            Pool::run_task_manager(&pool);
        });
        b1.wait();

        self.task_manager_spawned.store(true, Ordering::Relaxed);
        self.global_cv.notify_one();
        log::trace!("Pool is running");
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    pub fn sleeping(&self) -> usize {
        self.sleeping.load(Ordering::Relaxed)
    }

    /// Schedule a job onto the thread-pool.
    ///
    /// Task starts execution only when it arrives to the worker and worker processes it.
    ///
    /// If all workers are full and no more can be spawned (due to limited `parallelism` option, see [`Pool::new`])
    /// task will be executed only when one of the workers becames free.
    pub fn schedule(&self, task: Box<dyn Task>) {
        self.global.lock().unwrap().push(task);
        /* wake up sleeping workers if there's any. They would take a new job to execute
            and also wake up task manager *after* stealing from other, low-priority workers
            and from global-queue. 
        */
        if self.sleeping() > 0 {
            self.sleeping_cvar.notify_all();
        } else {
            /*
                no sleeping workers means all workers are busy *or* there's no workers at all:
                wake up task manager thread and spawn a new worker if necessary
             */
            self.global_cv.notify_one();
        }
    }

    /// Schedule multiple jobs onto the thread-pool.
    pub fn schedule_multiple(&self, mut tasks: Vec<Box<dyn Task>>) {
        if tasks.is_empty() {
            return;
        }

        let mut queue = self.global.lock().unwrap();

        queue.append(&mut tasks);

        if self.sleeping() > 0 {
            self.sleeping_cvar.notify_all();
        } else {
            self.global_cv.notify_one();
        }
    }

    fn run_task_manager(pool: &Arc<Pool>) {
        log::trace!("Task manager instantiated");
        while pool.alive.load(Ordering::Relaxed) {
            let mut tasks = pool.global.lock().unwrap();

            tasks = pool.global_cv
                .wait_while(tasks, |tasks: &mut Vec<_>| {
                    !pool.alive.load(Ordering::Relaxed) || tasks.is_empty()
                })
                .unwrap();

            let mut workers = pool.workers.lock().unwrap();
            // rebuild the heap as some workers might've processed some tasks and could've became high-priority (empty/almost empty)
            workers.build_heap();
            let target = workers.get(0);
            let mut should_spawn_worker = false;
            if let Some(target) = target {
                while let Some(task) = tasks.pop() {
                    if let Err(task) = target.queue.push(task) {
                        tasks.push(task);
                        should_spawn_worker = true;
                        break;
                    }
                }
            } else {
                should_spawn_worker = true;
            }

            if should_spawn_worker && workers.len() < pool.upper_limit {
                let id = workers.len();
                log::trace!("Spawning new worker #{}", id);
                let limit = pool.upper_limit;
                let pool = pool.clone();
                let shared = Shared {
                    id,
                    queue: Arc::new(ArrayQueue::new(LOCAL_QUEUE_CAPACITY)),
                };
                let q = shared.queue.clone();
                std::thread::spawn(move || {
                    pin_thread_to_core(id % limit);
                    Worker::new(id, pool, q).run();
                });

                workers.push(shared);
                workers.build_heap();
                drop(workers);
                drop(tasks);
                // worker created, give a chance for other workers to steal from global queue
                // if they don't we just reschedule tasks on a new worker
                std::thread::yield_now();
                continue;
            }
        }
    }
}

impl Worker {
    fn new(id: usize, pool: Arc<Pool>, queue: Arc<ArrayQueue<Box<dyn Task>>>) -> Self {
        Self {
            id,
            pool,
            work: queue,
        }
    }

    fn sleep(&self) {
        let global = self.pool.global.lock().unwrap();

        if !global.is_empty() || !self.pool.is_alive() {
            return;
        }

        self.pool.sleeping.fetch_add(1, Ordering::AcqRel);

        // We don't handle spurious wakeups here because:
        //
        // 1. We may be woken up when new work is produced on a local queue,
        //    while the global queue is still empty
        // 2. If we're woken up too early we'll just perform another work
        //    iteration, then go back to sleep.
        let _result = self.pool.sleeping_cvar.wait(global).unwrap();

        self.pool.sleeping.fetch_sub(1, Ordering::AcqRel);
    }

    fn run(&mut self) {
        log::trace!("Worker #{} instantiated", self.id);
        while self.pool.is_alive() {
            if let Some(task) = self.work.pop() {
                task.run(&self.pool);
                continue;
            } else if let Some(task) = self.steal_from_thread() {
                task.run(&self.pool);
                continue;
            } else if let Some(task) = self.steal_from_global() {
                task.run(&self.pool);
                continue;
            }

            self.sleep();
        }
    }

    fn steal_from_thread(&mut self) -> Option<Box<dyn Task>> {
        let start = self.id + 1;
        let mut workers = self.pool.workers.lock().unwrap();
        let len = workers.len();

        // iterate workers with lowest-priority (workers that are loaded with jobs)
        // and try to steal from them.
        for index in (0..len).rev() {
            let index = (start + index) % len;

            if index == self.id {
                continue;
            }

            let steal_from = workers.get(index).unwrap();

            if let Some(initial) = steal_from.queue.pop() {
                let len = steal_from.queue.len();
                let steal = std::cmp::min(len / 2, STEAL_LIMIT);

                for _ in 0..steal {
                    if let Some(task) = steal_from.queue.pop() {
                        if let Err(task) = self.work.push(task) {
                            workers.restore_heap_property(index);
                            drop(workers);
                            self.pool.schedule(task);
                            break;
                        }
                    } else {
                        workers.restore_heap_property(index);
                        break;
                    }
                }

                return Some(initial);
            }
        }
        None
    }

    fn steal_from_global(&mut self) -> Option<Box<dyn Task>> {
        let mut global = self.pool.global.lock().unwrap();

        if let Some(initial) = global.pop() {
            let len = global.len();
            let steal = std::cmp::min(len / 2, STEAL_LIMIT);

            if steal > 0 {
                // We're splitting at an index, so we must subtract one from the
                // amount.
                let mut to_steal = global.split_off(steal - 1);

                drop(global);

                while let Some(task) = to_steal.pop() {
                    if let Err(task) = self.work.push(task) {
                        to_steal.push(task);
                        self.pool.schedule_multiple(to_steal);
                        break;
                    }
                }
            }
            // notify task manager that we might have a chance to schedule up more tasks
            self.pool.global_cv.notify_one();
            Some(initial)
        } else {
            None
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
        self.sleeping_cvar.notify_all();
        self.global_cv.notify_one();
    }
}
