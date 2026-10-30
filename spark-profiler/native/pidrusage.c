// Prints every per-process counter macOS keeps for one process.
//
// Usage: pidrusage <pid>
//
// Output: one line of "<kind>:<name>=<value>" items separated by spaces.
//   kind "c" = counter that only grows (the profiler stores after - before)
//   kind "g" = gauge, a level at this moment (the profiler stores the value after the run and the change)
// Time fields are converted to nanoseconds (on Apple Silicon the kernel reports mach ticks).
//
// Sources:
//   proc_pid_rusage(RUSAGE_INFO_V6)  -> CPU time, instructions, cycles, energy, disk I/O, memory footprint, ...
//   proc_pidinfo(PROC_PIDTASKINFO)   -> page faults, context switches, syscalls, threads, virtual/resident size
//
// Build: cc -O2 -o pidrusage pidrusage.c
#include <libproc.h>
#include <mach/mach_time.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/proc_info.h>
#include <sys/resource.h>

static double ticks_to_ns;

static void c(const char *name, uint64_t v) { printf("c:%s=%llu ", name, (unsigned long long)v); }
static void g(const char *name, uint64_t v) { printf("g:%s=%llu ", name, (unsigned long long)v); }
static void ct(const char *name, uint64_t ticks) { c(name, (uint64_t)(ticks * ticks_to_ns)); }

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <pid>\n", argv[0]);
        return 2;
    }
    int pid = atoi(argv[1]);

    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    ticks_to_ns = (double)tb.numer / (double)tb.denom;

    struct rusage_info_v6 ri;
    if (proc_pid_rusage(pid, RUSAGE_INFO_V6, (rusage_info_t *)&ri) != 0) {
        perror("proc_pid_rusage");
        return 1;
    }

    // CPU time
    ct("user_ns", ri.ri_user_time);
    ct("system_ns", ri.ri_system_time);
    ct("user_pcore_ns", ri.ri_user_ptime);          // part of user time spent on performance cores
    ct("system_pcore_ns", ri.ri_system_ptime);
    ct("runnable_ns", ri.ri_runnable_time);         // time ready to run (running or waiting for a core)
    ct("billed_system_ns", ri.ri_billed_system_time);
    ct("serviced_system_ns", ri.ri_serviced_system_time);
    ct("secure_system_ns", ri.ri_secure_time_in_system);
    ct("secure_system_pcore_ns", ri.ri_secure_ptime_in_system);
    ct("cpu_qos_default_ns", ri.ri_cpu_time_qos_default);
    ct("cpu_qos_maintenance_ns", ri.ri_cpu_time_qos_maintenance);
    ct("cpu_qos_background_ns", ri.ri_cpu_time_qos_background);
    ct("cpu_qos_utility_ns", ri.ri_cpu_time_qos_utility);
    ct("cpu_qos_legacy_ns", ri.ri_cpu_time_qos_legacy);
    ct("cpu_qos_user_initiated_ns", ri.ri_cpu_time_qos_user_initiated);
    ct("cpu_qos_user_interactive_ns", ri.ri_cpu_time_qos_user_interactive);

    // Hardware counters and energy
    c("instructions", ri.ri_instructions);
    c("cycles", ri.ri_cycles);
    c("instructions_pcore", ri.ri_pinstructions);
    c("cycles_pcore", ri.ri_pcycles);
    c("energy_nj", ri.ri_energy_nj);
    c("energy_pcore_nj", ri.ri_penergy_nj);
    c("billed_energy", ri.ri_billed_energy);
    c("serviced_energy", ri.ri_serviced_energy);
    c("pkg_idle_wakeups", ri.ri_pkg_idle_wkups);
    c("interrupt_wakeups", ri.ri_interrupt_wkups);

    // Disk I/O
    c("disk_read_bytes", ri.ri_diskio_bytesread);
    c("disk_write_bytes", ri.ri_diskio_byteswritten);
    c("logical_write_bytes", ri.ri_logical_writes);
    c("pageins", ri.ri_pageins);

    // Memory levels
    g("phys_footprint_bytes", ri.ri_phys_footprint);
    g("resident_bytes", ri.ri_resident_size);
    g("wired_bytes", ri.ri_wired_size);
    g("lifetime_max_phys_footprint_bytes", ri.ri_lifetime_max_phys_footprint);
    g("interval_max_phys_footprint_bytes", ri.ri_interval_max_phys_footprint);
    g("neural_footprint_bytes", ri.ri_neural_footprint);

    // Task info: faults, context switches, syscalls, threads
    struct proc_taskinfo ti;
    int n = proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &ti, sizeof(ti));
    if (n == (int)sizeof(ti)) {
        c("page_faults", (uint32_t)ti.pti_faults);
        c("cow_faults", (uint32_t)ti.pti_cow_faults);
        c("task_pageins", (uint32_t)ti.pti_pageins);
        c("context_switches", (uint32_t)ti.pti_csw);
        c("syscalls_unix", (uint32_t)ti.pti_syscalls_unix);
        c("syscalls_mach", (uint32_t)ti.pti_syscalls_mach);
        c("mach_messages_sent", (uint32_t)ti.pti_messages_sent);
        c("mach_messages_received", (uint32_t)ti.pti_messages_received);
        g("virtual_bytes", ti.pti_virtual_size);
        g("task_resident_bytes", ti.pti_resident_size);
        g("threads", (uint32_t)ti.pti_threadnum);
        g("threads_running", (uint32_t)ti.pti_numrunning);
    }
    printf("\n");
    return 0;
}
