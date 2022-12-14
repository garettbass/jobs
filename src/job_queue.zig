const std = @import("std");

const assert = std.debug.assert;

const panic = std.debug.panic;

const print = std.debug.print;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

pub const cache_line_size = 64;

pub const min_jobs = 16;

pub const Error = error{ Uninitialized, Stopped };

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

pub const JobId = enum(u32) {
    none,
    _, // non-exhaustive enum

    pub fn format(
        id: JobId,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        const f = id.fields();
        return writer.print("{}:{}", .{ f.index, f.cycle });
    }

    pub inline fn cycle(id: JobId) u16 {
        return id.fields().cycle;
    }

    pub inline fn index(id: JobId) u16 {
        return id.fields().index;
    }

    inline fn fields(id: *const JobId) Fields {
        return @ptrCast(*const Fields, id).*;
    }

    const Fields = packed struct {
        cycle: u16, // lo bits
        index: u16, // hi bits

        inline fn init(_index: u16, _cycle: u16) Fields {
            return .{ .index = _index, .cycle = _cycle };
        }

        inline fn id(_fields: *const Fields) JobId {
            comptime assert(@sizeOf(Fields) == @sizeOf(JobId));
            return @ptrCast(*const JobId, _fields).*;
        }
    };
};

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

/// Returns a struct that executes jobs on a pool of threads, which may be
/// configured as follows:
/// * `max_jobs` - the maximum number of jobs that can be waiting in the queue.
/// * `max_job_size` - the maximum size of a job struct that can be stored in
///    the queue.
/// * `max_threads` - the maximum number of threads that can be spawned by the
///   `JobQueue`. Even when `max_threads` is greater, the `JobQueue` will never
///   spawn more than `std.Thread.getCpuCount() - 1` threads.
/// * `idle_sleep_ns` - the maximum number of nanoseconds to sleep when a
///   thread is waiting for a job to become available.  When `idle_sleep_ns`
///   is `0`, idle threads will not sleep at all.
///
/// Open issues:
/// * `JobQueue` is not designed to support single threaded environments, and
///   has not been tested for correctness in case background threads cannot be
///   spawned.
pub fn JobQueue(
    comptime config: struct {
        // zig fmt: off
        max_jobs      : u16 = 256,
        max_job_size  : u16 =  64,
        max_threads   : u8  =   8,
        idle_sleep_ns : u32 =  10,
        // zig fmt: on
    },
) type {
    compileAssert(
        config.max_jobs >= min_jobs,
        "config.max_jobs ({}) must be at least min_jobs ({})",
        .{ config.max_jobs, min_jobs },
    );

    compileAssert(
        config.max_job_size >= cache_line_size,
        "config.max_job_size ({}) must be at least cache_line_size ({})",
        .{ config.max_job_size, cache_line_size },
    );

    compileAssert(
        config.max_job_size % cache_line_size == 0,
        "config.max_job_size ({}) must be a multiple of cache_line_size ({})",
        .{ config.max_job_size, cache_line_size },
    );

    const Atomic = std.atomic.Atomic;

    const Slot = struct {
        const Self = @This();

        pub const max_job_size = config.max_job_size;

        const Data = [max_job_size]u8;
        const Main = *const fn (*Data) void;

        // zig fmt: off
        data     : Data align(cache_line_size) = undefined,
        main     : Main align(cache_line_size) = undefined,
        name     : []const u8                  = undefined,
        id       : JobId                       = JobId.none,
        prereq   : JobId                       = JobId.none,
        cycle    : Atomic(u16)                 = .{ .value = 0 },
        // zig fmg: on

        fn storeJob(
            self: *Self,
            comptime Job: type,
            job: *const Job,
            index: usize,
            prereq: JobId,
        ) JobId {
            const old_cycle: u16 = self.cycle.load(.Acquire);
            assert(isFreeCycle(old_cycle));

            const new_cycle: u16 = old_cycle +% 1;
            assert(isLiveCycle(new_cycle));

            const acquired : bool = null == self.cycle.compareAndSwap(
                old_cycle,
                new_cycle,
                .Monotonic,
                .Monotonic,
            );
            assert(acquired);

            std.mem.set(u8, &self.data, 0);
            std.mem.copy(u8, &self.data, std.mem.asBytes(job));

            const main: *const fn (*Job) void = &@field(Job, "main");
            const id = jobId(@truncate(u16, index), new_cycle);

            self.main = @ptrCast(Main, main);
            self.name = @typeName(Job);
            self.id = id;
            self.prereq = if (prereq != id) prereq else JobId.none;
            return id;
        }

        fn executeJob(self: *Self, id: JobId) void {
            const old_id = @atomicLoad(JobId, &self.id, .Monotonic);
            assert(old_id == id);

            const old_cycle: u16 = old_id.cycle();
            assert(isLiveCycle(old_cycle));

            const new_cycle: u16 = old_cycle +% 1;
            assert(isFreeCycle(new_cycle));

            self.main(&self.data);

            const released : bool = null == self.cycle.compareAndSwap(
                old_cycle,
                new_cycle,
                .Monotonic,
                .Monotonic,
            );
            assert(released);
        }

        fn jobId(index: u16, cycle: u16) JobId {
            return JobId.Fields.init(index, cycle).id();
        }
    };

    compileAssert(
        @alignOf(Slot) == cache_line_size,
        "@alignOf({s}) ({}) not equal to cache_line_size ({})",
        .{ @typeName(Slot), @alignOf(Slot), cache_line_size },
    );

    compileAssert(
        @sizeOf(Slot) % cache_line_size == 0,
        "@sizeOf({s}) ({}) not a multiple of cache_line_size ({})",
        .{ @typeName(Slot), @sizeOf(Slot), cache_line_size },
    );

    return struct {

        pub const max_jobs: u16 = config.max_jobs;

        pub const max_threads: u8 = config.max_threads;

        pub const max_job_size: u16 = config.max_job_size;

        pub const idle_sleep_ns: u64 = config.idle_sleep_ns;

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        const Self = @This();
        const Instant = std.time.Instant;
        const Mutex = std.Thread.Mutex;
        const Thread = std.Thread;

        const Slots = [max_jobs]Slot;
        const Threads = [max_threads]Thread;

        const RingQueue = @import("ring_queue.zig").RingQueue;
        const FreeQueue = RingQueue(usize, max_jobs);
        const LiveQueue = RingQueue(JobId, max_jobs);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // zig fmt: off
        // slots first because they are cache-aligned
        _slots       : Slots     = [_]Slot{.{}} ** max_jobs,
        _threads     : Threads   = [_]Thread{undefined} ** max_threads,
        _mutex       : Mutex     = .{},
        _live_queue  : LiveQueue = .{},
        _free_queue  : FreeQueue = .{},
        _num_threads : u64       = 0,
        _main_thread : Atomic(u64)  = .{ .value = 0 },
        _lock_thread : Atomic(u64)  = .{ .value = 0 },
        _initialized : Atomic(bool) = .{ .value = false },
        _started     : Atomic(bool) = .{ .value = false },
        _running     : Atomic(bool) = .{ .value = false },
        _stopping    : Atomic(bool) = .{ .value = false },
        // zig fmt: on

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        /// Initializes the `JobQueue`, required before calling `start()`.
        pub fn init() Self {
            comptime compileAssert(
                @alignOf(Self) == cache_line_size,
                "@alignOf({s}) ({}) not equal to cache_line_size ({})",
                .{ @typeName(Self), @alignOf(Self), cache_line_size },
            );

            var self = Self{};

            // initialize free queue
            var i: usize = 0;
            while (i < FreeQueue.capacity) : (i += 1) {
                self._free_queue.enqueueAssumeNotFull(i);
            }

            self._initialized.store(true, .Monotonic);

            return self;
        }

        /// Calls `stop()` and `join()` as needed.
        pub fn deinit(self: *Self) void {
            if (self.isInitialized()) self.stop();
            if (self.isStarted()) self.join();
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        /// Spawn threads and begin executing jobs.
        /// `JobQueue` must be initialized, and not yet started or stopped.
        pub fn start(self: *Self) void {
            self.lock("start");
            defer self.unlock("start");

            const this_thread = Thread.getCurrentId();
            const prev_thread = self._main_thread.swap(this_thread, .Monotonic);
            assert(prev_thread == 0);

            const was_initialized = self._initialized.load(.Monotonic);
            assert(was_initialized == true);

            const was_started = self._started.swap(true, .Monotonic);
            assert(was_started == false);

            const was_running = self._running.swap(true, .Monotonic);
            assert(was_running == false);

            const was_stopping = self._stopping.load(.Monotonic);
            assert(was_stopping == false);

            // spawn up to (num_cpus - 1) threads
            var n: usize = 0;
            const num_cpus = Thread.getCpuCount() catch 2;
            const num_threads_goal = std.math.min(num_cpus - 1, max_threads);
            while (n < num_threads_goal) {
                if (Thread.spawn(.{}, threadMain, .{ self, n })) |thread| {
                    nameThread(thread, "JobQueue[{}]", .{n});
                    self._threads[n] = thread;
                    n += 1;
                } else |err| {
                    print("thread[{}]: {}\n", .{ n, err });
                    break;
                }
            }
            // print("spawned {}/{} threads\n", .{ n, num_threads_goal });
            self._num_threads = n;
        }

        /// Signals threads to stop running, and prevents scheduling more jobs.
        /// Call `stop()` from any thread.
        pub fn stop(self: *Self) void {
            if (!self.isRunning()) return;

            // signal threads to stop running
            const was_running = self._running.swap(false, .Monotonic);
            assert(was_running == true);

            // prevent scheduling more jobs
            const was_stopping = self._stopping.swap(true, .Monotonic);
            assert(was_stopping == false);
        }

        /// Waits for all threads to finish, then executes any remaining jobs
        /// before returning.  After `join()` returns, the `JobQueue` has been
        /// reset to its default, uninitialized state.
        /// Call `join()` from the same thread that called `start()`.
        /// `JobQueue` must be initialized and started before calling `join()`.
        /// You may call `join()` before calling `stop()`, but since `join()`
        /// will not return until after `stop()` is called, you must then call
        /// `stop()` from another thread, e.g. from a job.
        pub fn join(self: *Self) void {
            assert(self.isMainThread());

            if (!self.isStarted()) return;

            const n = self._num_threads;
            // print("joining {} threads...\n", .{n});

            var i: u64 = 0;
            while (i < n) : (i += 1) {
                self._threads[i].join();
            }

            // print("joined {} threads\n", .{n});

            // drain job queue
            assert(self.isUnlockedThread());
            self.executeJobs(.unlocked, .dequeue_jobid_after_join);

            // reset to default state
            self.* = Self{};
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        /// Returns `true` if `init()` has been called, and `join()` has not
        /// yet run to completion.
        pub fn isInitialized(self: *const Self) bool {
            return self._initialized.load(.Monotonic);
        }

        /// Returns `true` if `start()` has been called, and `join()` has not
        /// yet run to completion.
        pub fn isStarted(self: *const Self) bool {
            return self._started.load(.Monotonic);
        }

        /// Returns `true` if `start()` has been called, and `stop()` has not
        /// yet been called.
        pub fn isRunning(self: *const Self) bool {
            return self._running.load(.Monotonic);
        }

        /// Returns `true` if `stop()` has been called, and `join()` has not
        /// yet run to completion.
        pub fn isStopping(self: *const Self) bool {
            return self._stopping.load(.Monotonic);
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        /// Returns `true` if the provided `JobId` identifies a job that has
        /// been completed.
        /// Returns `false` for a `JobId` that is scheduled and has not yet
        /// completed execution.
        /// Always returns `true` for `JobId.none`, which is always considered
        /// trivially complete.
        pub fn isComplete(self: *const Self, id: JobId) bool {
            if (id == JobId.none) return true;

            const _id = id.fields();
            assert(isLiveCycle(_id.cycle));

            const slot: *const Slot = &self._slots[_id.index];
            const slot_cycle = slot.cycle.load(.Monotonic);
            return slot_cycle != _id.cycle;
        }

        /// Returns `true` if the provided `JobId` identifies a job that has
        /// been scheduled and has not yet completed execution.  The job may or
        /// may not already be executing on a background thread.
        /// Returns `false` for a JobId that has not been scheduled, or has
        /// already completed.
        /// Always returns false for `JobId.none`, which is considered
        /// trivially complete.
        pub fn isPending(self: *const Self, id: JobId) bool {
            if (id == JobId.none) return false;

            const _id = id.fields();
            assert(isLiveCycle(_id.cycle));

            const slot: *const Slot = &self._slots[_id.index];
            const slot_cycle = slot.cycle.load(.Monotonic);
            return slot_cycle == _id.cycle;
        }

        /// Returns the number of jobs waiting in the queue.
        /// Only includes jobs that have not yet begun execution.
        pub fn numWaiting(self: *const Self) usize {
            return self._live_queue.len();
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        /// Inserts a job into the queue, and returns a `JobId` that can be
        /// used to specify dependencies between jobs.
        ///
        /// `prereq` - specifies a job that must run to completion before the
        /// job being scheduled can begin.  To schedule a job that must wait
        /// for more than one job to complete, call `combine()` to consolidate
        /// a set of `JobIds` into a single `JobId` that can be provided as the
        /// `prereq` argument to `schedule()`.
        ///
        /// `job` - provides an instance of a struct that captures the context
        /// and provides a function that will be executed on a separate thread.
        /// The provided `job` must satisfy the following requirements:
        /// * The total size of the job must not exceed `config.max_job_size`
        /// * The job must be an instance of a struct
        /// * The job must declare a function named `main` with one of the
        ///   following supported signatures:
        /// ```
        ///   fn main(*@This())
        ///   fn main(*const @This())
        /// ```
        pub fn schedule(self: *Self, prereq: JobId, job: anytype) Error!JobId {
            const Job = @TypeOf(job);
            validateJob(Job);

            self.lock("schedule");
            defer self.unlock("schedule");

            if (!self.isInitialized()) return Error.Uninitialized;
            if (self.isStopping()) return Error.Stopped;

            const index = self.dequeueFreeIndex();
            const slot: *Slot = &self._slots[index];
            const id = slot.storeJob(Job, &job, index, prereq);
            self.enqueueJobId(id);
            return id;
        }

        /// Combines zero or more `JobIds` into a single `JobId` that can be
        /// provided to the `prereq` argument when calling `schedule()`.
        /// This enables scheduling jobs that must wait on the completion of
        /// an arbitrary number of other jobs.
        /// Returns `JobId.none` when `prereqs` is empty.
        /// Returns `prereqs[0]` when `prereqs` contains only one element.
        pub fn combine(self: *Self, prereqs: []const JobId) Error!JobId {
            if (prereqs.len == 0) return JobId.none;
            if (prereqs.len == 1) return prereqs[0];

            var id = JobId.none;
            var i: usize = 0;
            const in: []const JobId = prereqs;
            while (i < in.len) {
                var job = CombinePrereqsJob{ .jobs = self };

                // copy prereqs to job
                var o: usize = 0;
                const out: []JobId = &job.prereqs;
                while (i < in.len and o < out.len) {
                    out[o] = in[i];
                    i += 1;
                    o += 1;
                }

                id = try self.schedule(id, job);
            }
            return id;
        }

        /// Waits until the specified `prereq` is completed.
        pub fn wait(self: *Self, prereq: JobId) void {
            while (self.isPending(prereq)) {
                // print("waiting for prereq {}...\n", .{prereq});
                threadIdle();
            }
        }

        //----------------------------------------------------------------------

        fn dequeueFreeIndex(self: *Self) usize {
            assert(self.isLockedThread());

            if (self._free_queue.dequeueIfNotEmpty()) |index| {
                return index;
            }

            while (true) {
                // must process jobs to acquire free index
                const id = self._live_queue.dequeueAssumeNotEmpty();
                if (self.executeJob(id, .locked, .acquire_free_index)) |index| {
                    return index;
                }
            }
        }

        fn enqueueJobId(self: *Self, new_id: JobId) void {
            assert(self.isLockedThread());

            while (self._live_queue.isFull()) {
                // must process jobs to unblock live queue
                const old_id = self._live_queue.dequeueAssumeNotEmpty();
                self.executeJob(old_id, .locked, .enqueue_free_index);
            }

            self._live_queue.enqueueAssumeNotFull(new_id);
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fn threadMain(self: *Self, n: usize) void {
            print("thread[{}]: {}\n", .{ n, Thread.getCurrentId() });
            ignore(n);

            assert(self.notMainThread());
            assert(self.isUnlockedThread());

            while (self.isRunning()) {
                self.executeJobs(.unlocked, .dequeue_jobid_if_running);
                threadIdle();
            }

            // print("thread[{}] DONE\n", .{n});
        }

        fn threadIdle() void {
            if (idle_sleep_ns > 0) {
                std.time.sleep(idle_sleep_ns);
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fn nameThread(t: Thread, comptime fmt: []const u8, args: anytype) void {
            var buf: [Thread.max_name_len]u8 = undefined;
            if (std.fmt.bufPrint(&buf, fmt, args)) |name| {
                t.setName(name) catch |err| ignore(err);
            } else |err| {
                ignore(err);
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        const CombinePrereqsJob = struct {
            const jobs_size = @sizeOf(*Self);
            const prereq_size = @sizeOf(JobId);
            const max_prereqs = (max_job_size - jobs_size) / prereq_size;

            jobs: *Self = .{},
            prereqs: [max_prereqs]JobId = [_]JobId{JobId.none} ** max_prereqs,

            fn main(job: *@This()) void {
                for (job.prereqs) |prereq| {
                    job.jobs.wait(prereq);
                }
            }
        };

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        const ExecuteJobResult = enum {
            acquire_free_index,
            enqueue_free_index,
            dequeue_jobid_after_join,
            dequeue_jobid_if_running,
        };

        fn ExecuteJobReturnType(comptime result: ExecuteJobResult) type {
            return switch (result) {
                // zig fmt: off
                .acquire_free_index       => ?usize,
                .enqueue_free_index       => void,
                .dequeue_jobid_after_join => ?JobId,
                .dequeue_jobid_if_running => ?JobId,
                // zig fmt: on
            };
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fn executeJobs(
            self: *Self,
            comptime scope: LockScope,
            comptime result: ExecuteJobResult,
        ) void {
            var id = JobId.none;
            if (self.acquireJobId(scope, result)) |a| {
                id = a;
                while (self.executeJob(id, scope, result)) |b| {
                    id = b;
                }
            }
        }

        inline fn acquireJobId(
            self: *Self,
            comptime scope: LockScope,
            comptime result: ExecuteJobResult,
        ) ?JobId {
            if (self._live_queue.isEmpty()) return null;

            switch (result) {
                .acquire_free_index => unreachable,
                .enqueue_free_index => unreachable,
                .dequeue_jobid_after_join => {
                    assert(self.isMainThread());
                    return self._live_queue.dequeueIfNotEmpty();
                },
                .dequeue_jobid_if_running => {
                    assert(self.notMainThread());

                    self.lockIfScopeUnlocked("dequeueJobId", scope);
                    defer self.unlockIfScopeUnlocked("dequeueJobId", scope);

                    assert(self.isLockedThread());
                    return self._live_queue.dequeueIfNotEmpty();
                },
            }
        }

        fn executeJob(
            self: *Self,
            id: JobId,
            comptime scope: LockScope,
            comptime result: ExecuteJobResult,
        ) ExecuteJobReturnType(result) {
            // print("executeJob({}, {}, {})\n", .{id, scope, result});

            const _id = id.fields();
            assert(isLiveCycle(_id.cycle));

            // this index was assigned to us,
            // no other threads should be reading or writing this slot,
            // so we don't need to be locked to read/write here
            const slot: *Slot = &self._slots[_id.index];
            assert(slot.id == id);
            assert(slot.cycle.load(.Monotonic) == _id.cycle);
            assert(slot.prereq != id);

            {
                self.unlockIfScopeLocked("executeJob(a)", scope);
                defer self.lockIfScopeLocked("executeJob(a)", scope);

                // we cannot be locked when executing a job,
                // because the job may call schedule() or stop()
                assert(self.isUnlockedThread());
                self.wait(slot.prereq);
                slot.executeJob(id);
            }

            const free_index = _id.index;

            switch (result) {
                .acquire_free_index => {
                    return free_index;
                },
                .enqueue_free_index => {
                    self.lockIfScopeUnlocked("executeJob(b)", scope);
                    defer self.unlockIfScopeUnlocked("executeJob(b)", scope);

                    assert(self.isLockedThread());
                    self._free_queue.enqueueAssumeNotFull(free_index);
                    return;
                },
                .dequeue_jobid_after_join => {
                    assert(self.isMainThread());
                    return self._live_queue.dequeueIfNotEmpty();
                },
                .dequeue_jobid_if_running => {
                    assert(self.notMainThread());

                    self.lockIfScopeUnlocked("executeJob(d)", scope);
                    defer self.unlockIfScopeUnlocked("executeJob(d)", scope);

                    assert(self.isLockedThread());
                    self._free_queue.enqueueAssumeNotFull(free_index);
                    if (self.isRunning()) {
                        return self._live_queue.dequeueIfNotEmpty();
                    } else {
                        return null;
                    }
                },
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        const LockScope = enum { unlocked, locked };

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn lockIfScopeLocked(
            self: *Self,
            comptime site: []const u8,
            comptime scope: LockScope,
        ) void {
            switch (scope) {
                .unlocked => assert(self.isUnlockedThread()),
                .locked => self.lock(site),
            }
        }

        inline fn unlockIfScopeLocked(
            self: *Self,
            comptime site: []const u8,
            comptime scope: LockScope,
        ) void {
            switch (scope) {
                .unlocked => assert(self.isUnlockedThread()),
                .locked => self.unlock(site),
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn lockIfScopeUnlocked(
            self: *Self,
            comptime site: []const u8,
            comptime scope: LockScope,
        ) void {
            switch (scope) {
                .unlocked => self.lock(site),
                .locked => assert(self.isLockedThread()),
            }
        }

        inline fn unlockIfScopeUnlocked(
            self: *Self,
            comptime site: []const u8,
            comptime scope: LockScope,
        ) void {
            switch (scope) {
                .unlocked => self.unlock(site),
                .locked => assert(self.isLockedThread()),
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn lock(self: *Self, comptime site: []const u8) void {
            const this_thread = Thread.getCurrentId();
            const lock_thread = self._lock_thread.load(.Acquire);
            assert(this_thread != lock_thread);
            assert(site.len > 0);

            // print("~{} '{s}' scope...\n", .{this_thread, site});
            self._mutex.lock();
            self._lock_thread.store(this_thread, .Release);
            // print("~{} '{s}' locked\n", .{this_thread, site});
        }

        inline fn unlock(self: *Self, comptime site: []const u8) void {
            const this_thread = Thread.getCurrentId();
            const lock_thread = self._lock_thread.swap(0, .Monotonic);
            assert(this_thread == lock_thread);
            assert(site.len > 0);

            self._mutex.unlock();
            // print("~{} '{s}' unlocked\n", .{this_thread, site});
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn isLockedThread(self: *const Self) bool {
            const this_thread = Thread.getCurrentId();
            const lock_thread = self._lock_thread.load(.Acquire);
            return lock_thread == this_thread;
        }

        inline fn isUnlockedThread(self: *const Self) bool {
            const this_thread = Thread.getCurrentId();
            const lock_thread = self._lock_thread.load(.Acquire);
            return lock_thread != this_thread;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn isMainThread(self: *const Self) bool {
            const this_thread = Thread.getCurrentId();
            const main_thread = self._main_thread.load(.Monotonic);
            assert(main_thread != 0);
            return main_thread == this_thread;
        }

        inline fn notMainThread(self: *const Self) bool {
            const this_thread = Thread.getCurrentId();
            const main_thread = self._main_thread.load(.Monotonic);
            assert(main_thread != 0);
            return main_thread != this_thread;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        pub fn validateJob(comptime Job: type) void {
            comptime {
                const struct_info = switch (@typeInfo(Job)) {
                    .Struct => |info| info,
                    else => {
                        compileError("{s} must be a struct", .{@typeName(Job)});
                        unreachable;
                    },
                };

                compileAssert(
                    @sizeOf(Job) <= max_job_size,
                    "@sizeOf({s}) ({}) exceeds max_job_size ({})",
                    .{ @typeName(Job), @sizeOf(Job), max_job_size },
                );

                for (struct_info.decls) |decl| {
                    if (std.mem.eql(u8, decl.name, "main")) {
                        // compileAssert(
                        //     decl.is_pub,
                        //     "{s}.main must be public",
                        //     .{ @typeName(Job) },
                        // );
                        break;
                    }
                } else {
                    compileError(
                        "{s}.main(*{s}) not found",
                        .{ @typeName(Job), @typeName(Job) },
                    );
                }

                const Main = @TypeOf(@field(Job, "main"));
                const fn_info = switch (@typeInfo(Main)) {
                    .Fn => |info| info,
                    else => {
                        compileError(
                            "{s}.main must be a function",
                            .{@typeName(Job)},
                        );
                        unreachable;
                    },
                };

                compileAssert(
                    fn_info.is_generic == false,
                    "{s}.main() must not be generic",
                    .{@typeName(Job)},
                );

                compileAssert(
                    fn_info.is_var_args == false,
                    "{s}.main() must not have variadic arguments",
                    .{@typeName(Job)},
                );

                compileAssert(
                    fn_info.return_type != null,
                    "{s}.main() must return void",
                    .{@typeName(Job)},
                );

                compileAssert(
                    fn_info.return_type == void,
                    "{s}.main() must return void, not {s}",
                    .{ @typeName(Job), @typeName(fn_info.return_type.?) },
                );

                compileAssert(
                    fn_info.args.len > 0,
                    "{s}.main() must have at least one parameter",
                    .{@typeName(Job)},
                );

                const arg_type_0 = fn_info.args[0].arg_type;

                compileAssert(
                    arg_type_0 == *Job or arg_type_0 == *const Job,
                    "{s}.main() must accept *@This() or *const @This() as first parameter",
                    .{@typeName(Job)},
                );
            }
        }
    };
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

inline fn isFreeCycle(cycle: u64) bool {
    return (cycle & 1) == 0;
}

inline fn isLiveCycle(cycle: u64) bool {
    return (cycle & 1) == 1;
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

fn compileError(
    comptime format: []const u8,
    comptime args: anytype,
) void {
    @compileError(std.fmt.comptimePrint(format, args));
}

fn compileAssert(
    comptime ok: bool,
    comptime format: []const u8,
    comptime args: anytype,
) void {
    if (!ok) compileError(format, args);
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

inline fn ignore(_: anytype) void {}

////////////////////////////////// T E S T S ///////////////////////////////////

inline fn now() std.time.Instant {
    return std.time.Instant.now() catch unreachable;
}

test "JobQueue basics" {
    const Jobs = JobQueue(.{
        .max_threads = 8,
    });

    print("\n@sizeOf(Jobs):{}\n", .{@sizeOf(Jobs)});

    const main_thread = std.Thread.getCurrentId();
    print("main_thread: {}\n", .{main_thread});

    const job_count = 64;
    const job_workload_size = cache_line_size * 1024 * 1024;
    const JobWorkload = struct {
        const Unit = u64;
        const unit_size = @sizeOf(Unit);
        const unit_count = job_workload_size / unit_size;
        units: [unit_count]Unit align(cache_line_size) = [_]Unit{undefined} ** unit_count,
    };

    var allocator = std.testing.allocator;
    var job_workloads: []JobWorkload = try allocator.alignedAlloc(JobWorkload, @alignOf(JobWorkload), job_count);
    defer print("allocator.free(job_workloads) DONE\n", .{});
    defer allocator.free(job_workloads);
    defer print("allocator.free(job_workloads)...\n", .{});

    const JobStat = struct {
        main: std.Thread.Id = 0,
        thread: std.Thread.Id = 0,
        started: std.time.Instant = undefined,
        stopped: std.time.Instant = undefined,

        fn start(self: *@This()) void {
            self.thread = std.Thread.getCurrentId();
            self.started = now();
        }

        fn stop(self: *@This()) void {
            assert(self.thread == std.Thread.getCurrentId());
            self.stopped = now();
        }

        fn ms(self: @This()) u64 {
            return self.stopped.since(self.started) / std.time.ns_per_ms;
        }

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            if (self.thread == self.main) {
                return writer.print("~--MAIN-- {}ms", .{self.ms()});
            }
            return writer.print("~{} {}ms", .{ self.thread, self.ms() });
        }
    };

    var job_stats: [job_count]JobStat = [_]JobStat{.{ .main = main_thread }} ** job_count;

    const FillJob = struct {
        stat: *JobStat,
        workload: *JobWorkload,

        fn main(self: *@This()) void {
            self.stat.start();
            defer self.stat.stop();

            assert(@ptrToInt(self.workload) % 64 == 0);
            const thread: u64 = self.stat.thread;
            for (self.workload.units) |*unit, index| {
                unit.* = thread +% index;
            }
        }
    };

    var jobs = Jobs.init();
    defer jobs.deinit();

    // schedule job_count jobs to fill some arrays
    for (job_stats) |*job_stat, i| {
        _ = try jobs.schedule(.none, FillJob{
            .stat = job_stat,
            .workload = &job_workloads[i % job_count],
        });
    }

    // schedule a job to stop the job queue
    _ = try jobs.schedule(.none, struct {
        jobs: *Jobs,
        fn main(self: *@This()) void {
            self.jobs.stop();
        }
    }{ .jobs = &jobs });

    jobs.start();
    const started = now();

    jobs.join();
    const stopped = now();
    const main_ms = stopped.since(started) / std.time.ns_per_ms;
    var job_ms: u64 = 0;

    for (job_stats) |job_stat, i| {
        print("    job {}: {}\n", .{ i, job_stat });
        job_ms += job_stat.ms();
    }

    const throughput = @intToFloat(f64, job_ms) / @intToFloat(f64, main_ms);
    print("completed {} jobs ({}ms) in {}ms ({d:.1}x)\n", .{ job_count, job_ms, main_ms, throughput });
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

test "JobQueue prerequisites" {
    print("\n", .{});

    const Jobs = JobQueue(.{});

    var jobs = Jobs.init();
    defer jobs.deinit(); // waits for all threads and jobs to complete

    // jobs can be scheduled before start() is called
    var a: JobId = try jobs.schedule(.none, struct {
        pub fn main(_: *@This()) void {
            print("hello ", .{});
        }
    }{});

    // scheduled jobs will not execute until start() is called
    jobs.start();

    // jobs can be scheduled after start() is called
    var b: JobId = try jobs.schedule(a, struct {
        fn main(_: *@This()) void {
            print("world!\n", .{});
        }
    }{});

    // a job can call stop()
    _ = try jobs.schedule(b, struct {
        jobs: *Jobs,
        fn main(self: *@This()) void {
            self.jobs.stop();
        }
    }{ .jobs = &jobs });

    jobs.join(); // call join() to wait for all threads to finish after stop()
}
