use rustix::process::{ sched_setaffinity, CpuSet, Pid };

pub fn pin_thread_to_core(core: usize) {
    let mut set = CpuSet::new();
    set.set(core);
    let _ = sched_setaffinity(Pid::from_raw(0), &set);
}
