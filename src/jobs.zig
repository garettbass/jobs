const std = @import("std");
const ring_queue = @import("ring_queue.zig");

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

const Order = std.builtin.AtomicOrder;

const RingQueue = ring_queue.RingQueue;

const cache_line_size = 64;

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

const assert = std.debug.assert;

const panic = std.debug.panic;

const print = std.debug.print;

inline fn assertEqual(
    comptime name: []const u8,
    actual: anytype,
    expected: @TypeOf(actual),
) void {
    if (actual == expected) return;
    panic("assertEqual({s}: {}, expected: {})", .{ name, actual, expected });
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

pub const JobId = enum(u32) {
    none,
    _, // non-exhaustive enum

    inline fn init(_index: u16, _cycle: u16) JobId {
        return JobIdFields.init(_index, _cycle).id();
    }

    inline fn cycle(id: JobId) u16 {
        return id.fields().cycle;
    }

    inline fn index(id: JobId) u16 {
        return id.fields().index;
    }

    inline fn fields(id: *const JobId) JobIdFields {
        return @ptrCast(*const JobIdFields, id).*;
    }

    pub fn format(
        id: JobId,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        const f = id.fields();
        return writer.print("{}:{}", .{ f.index, f.cycle });
    }
};

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

const JobIdFields = packed struct {
    cycle: u16, // lo bits
    index: u16, // hi bits

    inline fn init(_index: u16, _cycle: u16) JobIdFields {
        return .{ .index = _index, .cycle = _cycle };
    }

    inline fn id(fields: *const JobIdFields) JobId {
        return @ptrCast(*const JobId, fields).*;
    }

    inline fn isFree(fields: *const JobIdFields) bool {
        return isFreeCycle(fields.cycle);
    }

    inline fn isLive(fields: *const JobIdFields) bool {
        return isLiveCycle(fields.cycle);
    }
};

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

fn JobSlot(comptime _max_job_size: u16) type {
    return struct {
        const Self = @This();

        pub const max_job_size = _max_job_size;

        const Data = [max_job_size]u8;
        const Main = *const fn (*Data) void;

        // zig fmt: off
        data     : Data align(cache_line_size) = undefined,
        main     : Main align(cache_line_size) = undefined,
        name     : []const u8                  = undefined,
        id       : JobId                       = JobId.none,
        prereq   : JobId                       = JobId.none,
        cycle    : u64                         = 0,
        // zig fmg: on

        fn store(
            self: *Self,
            job: anytype,
            index: usize,
            prereq: JobId,
        ) JobId {
            const Job = @TypeOf(job);
            comptime compileAssert(
                @sizeOf(Job) <= max_job_size,
                "@sizeOf({s}) ({}) exceeds max_job_size ({})",
                .{ @typeName(Job), @sizeOf(Job), max_job_size },
            );

            const old_cycle: u16 = @truncate(u16, self.cycle);
            assert(isFreeCycle(old_cycle));

            const new_cycle: u16 = old_cycle +% 1;
            assert(isLiveCycle(new_cycle));

            const acquired : bool = null == @cmpxchgStrong(
                u64,
                &self.cycle,
                old_cycle,
                new_cycle,
                .Monotonic,
                .Monotonic,
            );
            assert(acquired);

            std.mem.set(u8, &self.data, 0);
            std.mem.copy(u8, &self.data, std.mem.asBytes(&job));

            const main: *const fn (*Job) void = &@field(Job, "main");
            self.main = @ptrCast(Main, main);
            self.name = @typeName(Job);
            self.id = JobId.init(@truncate(u16, index), new_cycle);
            self.prereq = prereq;
            return self.id;
        }

        fn executeJob(self: *Self, id: JobId) void {
            const old_id = @atomicLoad(JobId, &self.id, .Monotonic);
            assertEqual("old_id", old_id, id);

            const old_cycle: u16 = old_id.cycle();
            assert(isLiveCycle(old_cycle));

            const new_cycle: u16 = old_cycle +% 1;
            assert(isFreeCycle(new_cycle));

            self.main(&self.data);

            const released : bool = null == @cmpxchgStrong(
                u64,
                &self.cycle,
                old_cycle,
                new_cycle,
                .Monotonic,
                .Monotonic,
            );
            assert(released);
        }
    };
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

const LockScope = enum { unlocked, locked };

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

const ProcessJobResult = enum {
    none,
    free_index,
    next_jobid,
    next_jobid_if_running,
};

fn ProcessJobResultType(comptime result: ProcessJobResult) type {
    return switch (result) {
        .none => void,
        .free_index => ?usize,
        .next_jobid => ?JobId,
        .next_jobid_if_running => ?JobId,
    };
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

pub const min_jobs = 256;

pub const min_job_size = cache_line_size;

pub fn JobQueue(
    comptime config: struct {
        max_jobs: u16 = min_jobs,
        max_threads: u8 = 15,
        max_job_size: u16 = min_job_size,
        idle_sleep_ns: u64 = 10,
    },
) type {
    compileAssert(
        config.max_jobs >= min_jobs,
        "config.max_jobs ({}) must be at least min_jobs ({})",
        .{ config.max_jobs, min_jobs },
    );
    compileAssert(
        config.max_job_size >= min_job_size,
        "config.max_job_size ({}) must be at least min_job_size ({})",
        .{ config.max_job_size, min_job_size },
    );
    compileAssert(
        config.max_job_size % cache_line_size == 0,
        "config.max_job_size ({}) must be a multiple of cache_line_size ({})",
        .{ config.max_job_size, cache_line_size },
    );

    const Slot = JobSlot(config.max_job_size);

    comptime compileAssert(
        @alignOf(Slot) == cache_line_size,
        "@alignOf({s}) ({}) not equal to cache_line_size ({})",
        .{ @typeName(Slot), @alignOf(Slot), cache_line_size },
    );
    comptime compileAssert(
        @sizeOf(Slot) % cache_line_size == 0,
        "@sizeOf({s}) ({}) not a multiple of cache_line_size ({})",
        .{ @typeName(Slot), @sizeOf(Slot), cache_line_size },
    );

    return struct {
        const Self = @This();

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        pub const max_jobs: u16 = config.max_jobs;

        pub const max_threads: u8 = config.max_threads;

        pub const max_job_size: u16 = config.max_job_size;

        pub const idle_sleep_ns: u64 = config.idle_sleep_ns;

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        const Mutex = std.Thread.Mutex;
        const Thread = std.Thread;
        const Instant = std.time.Instant;

        const Slots = [max_jobs]Slot;
        const JobIds = []const JobId;
        const Threads = [max_threads]Thread;
        const FreeQueue = RingQueue(usize, max_jobs);
        const LiveQueue = RingQueue(JobId, max_jobs);

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // zig fmt: off
        // slots come first because they are cache-aligned
        _slots       : Slots     = [_]Slot{.{}} ** max_jobs,
        _threads     : Threads   = [_]Thread{undefined} ** max_threads,
        _mutex       : Mutex     = .{},
        _live_queue  : LiveQueue = .{},
        _free_queue  : FreeQueue = .{},
        _main_thread : Thread.Id = 0,
        _lock_thread : Thread.Id = 0,
        _num_threads : usize     = 0,
        _initialized : usize     = 0,
        _started     : usize     = 0,
        _running     : usize     = 0,
        _stopped     : usize     = 0,
        // zig fmt: on

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

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

            self._initialized = 1;

            return self;
        }

        pub fn reinit(self: *Self) void {
            self.deinit();
            self.* = init();
        }

        pub fn deinit(self: *Self) void {
            if (self._initialized != 0) {
                self.stop();
                self.join();
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        pub fn start(self: *Self) void {
            self.lock("start");
            defer self.unlock("start");

            assertEqual("self._initialized", self._initialized, 1);

            assertEqual("self._main_thread", self._main_thread, 0);
            self._main_thread = Thread.getCurrentId();

            assertEqual("self._started", self._started, 0);
            self._started = 1;

            assertEqual("self._running", self._running, 0);
            self._running = 1;

            // spawn up to (num_cpus - 1) threads
            var n: usize = 0;
            const num_cpus = Thread.getCpuCount() catch 2;
            const num_threads_goal = std.math.min(num_cpus - 1, max_threads);
            while (n < num_threads_goal) {
                if (Thread.spawn(.{}, threadMain, .{self, n})) |thread| {
                    self._threads[n] = thread;
                    n += 1;
                } else |err| {
                    print("thread[{}]: {}\n", .{ n, err });
                    break;
                }
            }
            print("spawned {}/{} threads\n", .{n, num_threads_goal});
            self._num_threads = n;
        }

        pub fn stop(self: *Self) void {
            if (!self.isRunning()) return;

            // signal threads to stop running
            @atomicStore(u64, &self._running, 0, .Monotonic);

            // prevent scheduling more jobs
            assertEqual("self._stopped", self._stopped, 0);
            @atomicStore(u64, &self._stopped, 1, .Monotonic);

            // drain job queue on calling thread
            var id = JobId.none;
            if (self.dequeueJobIdIfAvailable()) |a| {
                id = a;
                while (self.processJob(id, .unlocked, .next_jobid)) |b| {
                    id = b;
                }
            }
        }

        pub fn join(self: *Self) void {
            const started = @atomicLoad(u64, &self._started, .Monotonic);
            if (started == 0) return;

            assertEqual("self._main_thread", self._main_thread, Thread.getCurrentId());

            var i: u64 = 0;
            const n = self._num_threads;
            while (i < n) : (i += 1) {
                self._threads[i].join();
            }

            // drain job queue on calling thread
            var id = JobId.none;
            if (self.dequeueJobIdIfAvailable()) |a| {
                id = a;
                while (self.processJob(id, .unlocked, .next_jobid)) |b| {
                    id = b;
                }
            }

            // reset to default state
            self.* = Self{};
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        pub fn isRunning(self: *const Self) bool {
            const running = @atomicLoad(u64, &self._running, .Monotonic);
            // print("running: {}\n", .{running});
            if (running == 0) return false;

            assertEqual("running", running, 1);
            return true;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        pub fn isComplete(self: *const Self, id: JobId) bool {
            if (id == JobId.none) return true;

            const _id = id.fields();
            assert(isLiveCycle(_id.cycle));

            const slot: *const Slot = &self._slots[_id.index];
            const slot_cycle = @atomicLoad(u64, &slot.cycle, .Monotonic);
            return slot_cycle != _id.cycle;
        }

        pub fn isPending(self: *const Self, id: JobId) bool {
            if (id == JobId.none) return false;

            const _id = id.fields();
            assert(isLiveCycle(_id.cycle));

            const slot: *const Slot = &self._slots[_id.index];
            const slot_cycle = @atomicLoad(u64, &slot.cycle, .Monotonic);
            return slot_cycle == _id.cycle;
        }

        pub fn len(self: *const Self) usize {
            return self._live_queue.lenAtomic();
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // TODO: consider making this return an error if you cannot schedule
        // a job because of one of the following:
        // - uninitialized
        // - stopped/joining
        pub fn schedule(self: *Self, prereqs: JobIds, job: anytype) JobId {
            self.lock("schedule");
            defer self.unlock("schedule");

            assertEqual("self._initialized", self._initialized, 1);
            assertEqual("self._stopped", self._stopped, 0);

            assert(prereqs.len <= 1); // TODO: multiple prereqs
            var prereq = if (prereqs.len == 0) JobId.none else prereqs[0];

            const index = self.acquireNextFreeIndex();

            assert(self.isLockedThread());
            const id = self._slots[index].store(job, index, prereq);

            for (prereqs) |p| {
                assert(p != id);
            }

            self.enqueueJobId(id);
            return id;
        }

        inline fn acquireNextFreeIndex(self: *Self) usize {
            assert(self.isLockedThread());

            if (self._free_queue.dequeueIfNotEmpty()) |index| {
                return index;
            }

            while (true) {
                // must process jobs to acquire free index
                const id = self._live_queue.dequeueAssumeNotEmpty();
                if (self.processJob(id, .locked, .free_index)) |index| {
                    return index;
                }
            }
        }

        inline fn enqueueJobId(self: *Self, new_id: JobId) void {
            assert(self.isLockedThread());

            while (self._live_queue.isFullAtomic()) {
                // must process jobs to unblock live queue
                const old_id = self._live_queue.dequeueAssumeNotEmpty();
                self.processJob(old_id, .locked, .none);
            }

            self._live_queue.enqueueAssumeNotFull(new_id);
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fn threadMain(self: *Self, n: usize) void {
            print("thread[{}]: {}\n", .{ n, Thread.getCurrentId() });

            assert(self.notMainThread());
            assert(self.isUnlockedThread());

            var id = JobId.none;
            while (self.isRunning()) {
                if (self.dequeueJobIdIfAvailable()) |a| {
                    id = a;
                    // self.processJob(id, .unlocked, .none);
                    while (self.processJob(id, .unlocked, .next_jobid_if_running)) |b| {
                        id = b;
                    }
                }
                if (idle_sleep_ns > 0) {
                    std.time.sleep(idle_sleep_ns);
                }
            }

            print("thread[{}] DONE\n", .{ n });
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fn dequeueJobIdIfAvailable(self: *Self) ?JobId {
            assert(self.isUnlockedThread());

            if (self._live_queue.isEmptyAtomic()) {
                return null;
            } else {
                self.lock("dequeueJobIdIfAvailable");
                defer self.unlock("dequeueJobIdIfAvailable");
                return self._live_queue.dequeueIfNotEmpty();
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fn processJob(
            self: *Self,
            id: JobId,
            comptime scope: LockScope,
            comptime result: ProcessJobResult,
        ) ProcessJobResultType(result) {
            // print("processJob({}, {}, {})\n", .{id, scope, result});

            const _id = id.fields();
            assert(isLiveCycle(_id.cycle));

            // this index was assigned to us,
            // no other threads should be reading or writing this slot,
            // so we don't need to be locked to read/write here
            const slot: *Slot = &self._slots[_id.index];
            assert(slot.id == id);
            assert(slot.cycle == _id.cycle);
            assert(slot.prereq != id);

            if (self.isPending(slot.prereq)) {
                // this job's prerequisite is pending,
                // requeue this job to be processed later
                self.lockIfScopeUnlocked("processJob(a)", scope);
                defer self.unlockIfScopeUnlocked("processJob(a)", scope);

                self.enqueueJobId(id);
                switch (result) {
                    .none => return,
                    .free_index => return null,
                    .next_jobid => {
                        assert(self.isMainThread());
                        assert(self.isLockedThread());
                        return self._live_queue.dequeueIfNotEmpty();
                    },
                    .next_jobid_if_running => {
                        assert(self.notMainThread());
                        if (self.isRunning()) {
                            assert(self.isLockedThread());
                            return self._live_queue.dequeueIfNotEmpty();
                        } else {
                            return null;
                        }
                    },
                }
            } else {
                self.executeJob(slot, id, scope);

                self.lockIfScopeUnlocked("processJob(b)", scope);
                defer self.unlockIfScopeUnlocked("processJob(b)", scope);

                switch (result) {
                    .none => {
                        assert(self.isLockedThread());
                        self._free_queue.enqueueAssumeNotFull(_id.index);
                        return;
                    },
                    .free_index => return _id.index,
                    .next_jobid => {
                        assert(self.isMainThread());
                        assert(self.isLockedThread());
                        self._free_queue.enqueueAssumeNotFull(_id.index);
                        return self._live_queue.dequeueIfNotEmpty();
                    },
                    .next_jobid_if_running => {
                        assert(self.notMainThread());
                        assert(self.isLockedThread());
                        self._free_queue.enqueueAssumeNotFull(_id.index);
                        if (self.isRunning()) {
                            return self._live_queue.dequeueIfNotEmpty();
                        } else {
                            return null;
                        }
                    },
                }
            }
        }

        inline fn executeJob(self: *Self, slot: *Slot, id: JobId, comptime scope: LockScope) void {
            self.unlockIfScopeLocked("executeJob", scope);
            defer self.lockIfScopeLocked("executeJob", scope);

            // we cannot be locked when executing a job,
            // because the job may call start() or schedule()
            slot.executeJob(id);
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn lockIfScopeLocked(self: *Self, comptime site: []const u8, comptime scope: LockScope) void {
            switch (scope) {
                .unlocked => assert(self.isUnlockedThread()),
                .locked => self.lock(site),
            }
        }

        inline fn unlockIfScopeLocked(self: *Self, comptime site: []const u8, comptime scope: LockScope) void {
            switch (scope) {
                .unlocked => assert(self.isUnlockedThread()),
                .locked => self.unlock(site),
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn lockIfScopeUnlocked(self: *Self, comptime site: []const u8, comptime scope: LockScope) void {
            switch (scope) {
                .unlocked => self.lock(site),
                .locked => assert(self.isLockedThread()),
            }
        }

        inline fn unlockIfScopeUnlocked(self: *Self, comptime site: []const u8, comptime scope: LockScope) void {
            switch (scope) {
                .unlocked => self.unlock(site),
                .locked => assert(self.isLockedThread()),
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn isMainThread(self: *const Self) bool {
            const main_thread = @atomicLoad(u64, &self._main_thread, .Monotonic);
            assert(main_thread != 0);
            return main_thread == Thread.getCurrentId();
        }

        inline fn notMainThread(self: *const Self) bool {
            const main_thread = @atomicLoad(u64, &self._main_thread, .Monotonic);
            assert(main_thread != 0);
            return main_thread != Thread.getCurrentId();
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        inline fn lock(self: *Self, comptime site:[]const u8) void {
            const lock_thread = @atomicLoad(u64, &self._lock_thread, .Monotonic);
            const this_thread = Thread.getCurrentId();
            assert(this_thread != lock_thread);
            assert(site.len > 0);

            // print("~{} '{s}' locking...\n", .{this_thread, site});
            self._mutex.lock();
            @atomicStore(u64, &self._lock_thread, this_thread, .Monotonic);
            // print("~{} '{s}' locked\n", .{this_thread, site});
        }

        inline fn unlock(self: *Self, comptime site: []const u8) void {
            const lock_thread = @atomicLoad(u64, &self._lock_thread, .Acquire);
            const this_thread = Thread.getCurrentId();
            assert(this_thread == lock_thread);
            assert(site.len > 0);

            @atomicStore(u64, &self._lock_thread, 0, .Release);
            self._mutex.unlock();
            // print("~{} '{s}' unlocked\n", .{this_thread, site});
        }

        inline fn isLockedThread(self: *const Self) bool {
            const lock_thread = @atomicLoad(u64, &self._lock_thread, .Monotonic);
            const this_thread = Thread.getCurrentId();
            return lock_thread == this_thread;
        }

        inline fn isUnlockedThread(self: *const Self) bool {
            const lock_thread = @atomicLoad(u64, &self._lock_thread, .Monotonic);
            const this_thread = Thread.getCurrentId();
            return lock_thread != this_thread;
        }
    };
}

////////////////////////////////// T E S T S ///////////////////////////////////

inline fn now() std.time.Instant {
    return std.time.Instant.now() catch unreachable;
}

inline fn maybeUnused(_: anytype) void {}

test "JobQueue basics" {
    const Jobs = JobQueue(.{
        // .max_threads = 4,
    });

    std.debug.print("\n@sizeOf(Jobs):{}\n", .{@sizeOf(Jobs)});

    const job_workload_size = cache_line_size * 1024 * 1024 * 4;
    const JobWorkload = struct {
        const Unit = u64;
        const unit_size = @sizeOf(Unit);
        const unit_count = job_workload_size / unit_size;
        units: [unit_count]Unit align(cache_line_size) = [_]Unit{undefined} ** unit_count,
    };

    const JobStat = struct {
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
            return writer.print("~{} {}ms", .{ self.thread, self.ms() });
        }
    };

    const job_count = 32;

    var allocator = std.testing.allocator;
    var job_workloads: []JobWorkload = try allocator.alignedAlloc(JobWorkload, @alignOf(JobWorkload), job_count);
    defer allocator.free(job_workloads);

    const StopJob = struct {
        jobs: *Jobs,

        fn main(self: *@This()) void {
            print("StopJob\n", .{});
            self.jobs.stop();
        }
    };

    const ZeroJob = struct {
        stat: *JobStat,
        workload: *JobWorkload,

        fn main(self: *@This()) void {
            self.stat.start();
            defer self.stat.stop();

            assertEqual("workload aligned", @ptrToInt(self.workload) % 64, 0);
            for (self.workload.units) |*unit| {
                unit.* = 0;
            }
        }
    };
    maybeUnused(ZeroJob);

    const FillJob = struct {
        stat: *JobStat,
        workload: *JobWorkload,

        fn main(self: *@This()) void {
            self.stat.start();
            defer self.stat.stop();

            assertEqual("workload aligned", @ptrToInt(self.workload) % 64, 0);
            for (self.workload.units) |*unit, index| {
                unit.* = index;
            }
        }
    };
    maybeUnused(FillJob);

    {
        const loop_count = 4;
        var job_stats: [job_count * loop_count]JobStat = undefined;

        var jobs = Jobs.init();
        defer jobs.deinit();

        jobs.start();
        const started = now();

        for (job_stats) |*job_stat, i| {
            _ = jobs.schedule(&.{}, FillJob{
                .stat = job_stat,
                .workload = &job_workloads[i % job_count],
            });
        }

        _ = jobs.schedule(&.{}, StopJob{
            .jobs = &jobs,
        });

        jobs.join();
        const stopped = now();
        const main_ms = stopped.since(started) / std.time.ns_per_ms;
        var job_ms: u64 = 0;

        for (job_stats) |job_stat, i| {
            print("    job {}: {}\n", .{ i, job_stat });
            job_ms += job_stat.ms();
        }

        const throughput = job_ms / main_ms;
        print("completed {} jobs in {}ms ({}ms {}x)\n", .{job_count, main_ms, job_ms, throughput});
    }

}