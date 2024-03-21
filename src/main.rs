use std::{ hint::black_box, num::NonZeroUsize, sync::{ atomic::AtomicUsize, Arc } };

use tpool::{ Pool, Task };

struct Fib(u128);

fn fib(n: u128) -> u128 {
    (0..n).fold((0, 1), |(a, b), _| (b, a + b)).0
}
static TASKS_COMPLETED: AtomicUsize = AtomicUsize::new(0);
impl Task for Fib {
    fn run(&self, _pool: &Arc<Pool>) {
        black_box(fib(self.0));

        TASKS_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn main() {
    env_logger::init();
    let pool = Pool::new(NonZeroUsize::new(16));
    for _ in 0..1000000 {
        pool.schedule(Box::new(Fib(130)));
    }

    while TASKS_COMPLETED.load(std::sync::atomic::Ordering::Relaxed) != 1000000 {
        std::thread::yield_now();
    }
}
