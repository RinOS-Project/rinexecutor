/* SPDX-License-Identifier: MIT */
#ifndef RINEXECUTOR_EXECUTOR_HPP
#define RINEXECUTOR_EXECUTOR_HPP

#include <cstddef>
#include <cstdint>

namespace RinExecutor {

constexpr std::size_t kMaxQueuedTasks = 64u;
constexpr std::uint32_t kPriorityLevels = 4u;
constexpr std::uint32_t kHighPriorityBurst = 8u;

enum class Priority : std::uint8_t {
    Low = 0u,
    Normal = 1u,
    High = 2u,
    Critical = 3u
};

enum class TaskResult : std::uint8_t {
    Complete = 0u,
    Requeue = 1u,
    Failed = 2u
};

enum class RunResult : std::uint8_t {
    Idle = 0u,
    Ran = 1u,
    Cancelled = 2u,
    Shutdown = 3u
};

struct TaskId {
    std::uint64_t value = 0u;

    constexpr bool valid() const noexcept { return value != 0u; }
};

class CancellationToken {
public:
    bool requested() const noexcept
    {
        return requested_ != nullptr && *requested_;
    }

private:
    friend class TaskExecutor;
    explicit CancellationToken(const bool* requested) : requested_(requested) {}
    const bool* requested_;
};

using TaskFn = TaskResult (*)(void* context, const CancellationToken& token);

class TaskExecutor {
public:
    TaskExecutor() noexcept = default;
    TaskExecutor(const TaskExecutor&) = delete;
    TaskExecutor& operator=(const TaskExecutor&) = delete;

    /* This is a single-owner cooperative executor.  The owner calls runOne()
     * from its event loop, so it never creates an unbounded hidden thread. */
    static constexpr std::uint32_t workerLimit() noexcept { return 1u; }

    bool submit(TaskFn function, void* context, Priority priority,
                TaskId* id_out) noexcept
    {
        if (function == nullptr || id_out == nullptr || shutting_down_ ||
            queue_count_ >= kMaxQueuedTasks || next_id_ == 0u ||
            next_sequence_ == 0u || !priority_valid(priority)) {
            if (id_out != nullptr) *id_out = TaskId{};
            return false;
        }
        for (std::size_t index = 0u; index < kMaxQueuedTasks; ++index) {
            Slot& slot = slots_[index];
            if (!slot.occupied) {
                slot.occupied = true;
                slot.queued = true;
                slot.running = false;
                slot.cancel_requested = false;
                slot.function = function;
                slot.context = context;
                slot.priority = priority;
                slot.id = TaskId{next_id_++};
                slot.sequence = next_sequence_++;
                ++queue_count_;
                *id_out = slot.id;
                return true;
            }
        }
        *id_out = TaskId{};
        return false;
    }

    /* A queued task is removed immediately.  A running callback cannot be
     * forcefully interrupted, but its token observes the request and a
     * requested callback is never requeued. */
    bool cancel(TaskId id) noexcept
    {
        Slot* slot = find(id);
        if (slot == nullptr) return false;
        if (slot->running) {
            slot->cancel_requested = true;
            return true;
        }
        if (slot->queued) {
            --queue_count_;
            clear(*slot);
            return true;
        }
        return false;
    }

    /* Reject new work and discard queued work.  A callback already on the
     * owner stack receives cancellation through its token. */
    void shutdown() noexcept
    {
        shutting_down_ = true;
        for (std::size_t index = 0u; index < kMaxQueuedTasks; ++index) {
            Slot& slot = slots_[index];
            if (!slot.occupied) continue;
            if (slot.running) {
                slot.cancel_requested = true;
            } else if (slot.queued) {
                --queue_count_;
                clear(slot);
            }
        }
    }

    bool isShutdown() const noexcept { return shutting_down_; }
    std::size_t queuedCount() const noexcept { return queue_count_; }

    RunResult runOne() noexcept
    {
        if (queue_count_ == 0u) {
            return shutting_down_ ? RunResult::Shutdown : RunResult::Idle;
        }
        const std::size_t index = choose_slot();
        if (index == kMaxQueuedTasks) return RunResult::Idle;
        Slot& slot = slots_[index];
        slot.queued = false;
        slot.running = true;
        --queue_count_;
        if (static_cast<std::uint32_t>(slot.priority) <
            static_cast<std::uint32_t>(Priority::High)) {
            high_priority_burst_ = 0u;
        } else if (high_priority_burst_ < kHighPriorityBurst) {
            ++high_priority_burst_;
        }

        if (slot.cancel_requested) {
            clear(slot);
            return RunResult::Cancelled;
        }
        const TaskId id = slot.id;
        const TaskResult result = slot.function(
            slot.context, CancellationToken(&slot.cancel_requested));
        (void)id;
        slot.running = false;
        if (slot.cancel_requested || result == TaskResult::Complete ||
            result == TaskResult::Failed || shutting_down_) {
            const bool cancelled = slot.cancel_requested;
            clear(slot);
            return cancelled ? RunResult::Cancelled : RunResult::Ran;
        }
        slot.queued = true;
        slot.sequence = next_sequence_ == 0u ? UINT64_MAX : next_sequence_++;
        ++queue_count_;
        return RunResult::Ran;
    }

    std::size_t runUntilIdle(std::size_t budget = kMaxQueuedTasks) noexcept
    {
        std::size_t executed = 0u;
        while (executed < budget && queue_count_ != 0u) {
            const RunResult result = runOne();
            if (result == RunResult::Idle || result == RunResult::Shutdown) break;
            ++executed;
        }
        return executed;
    }

private:
    struct Slot {
        bool occupied = false;
        bool queued = false;
        bool running = false;
        bool cancel_requested = false;
        TaskFn function = nullptr;
        void* context = nullptr;
        Priority priority = Priority::Normal;
        TaskId id{};
        std::uint64_t sequence = 0u;
    };

    static bool priority_valid(Priority priority) noexcept
    {
        return static_cast<std::uint32_t>(priority) < kPriorityLevels;
    }

    Slot* find(TaskId id) noexcept
    {
        if (!id.valid()) return nullptr;
        for (std::size_t index = 0u; index < kMaxQueuedTasks; ++index) {
            if (slots_[index].occupied && slots_[index].id.value == id.value) {
                return &slots_[index];
            }
        }
        return nullptr;
    }

    static void clear(Slot& slot) noexcept
    {
        slot = Slot{};
    }

    std::size_t choose_slot() const noexcept
    {
        std::size_t selected = kMaxQueuedTasks;
        std::uint32_t selected_priority = 0u;
        std::uint64_t selected_sequence = UINT64_MAX;
        const bool force_aging = high_priority_burst_ >= kHighPriorityBurst;
        for (std::size_t index = 0u; index < kMaxQueuedTasks; ++index) {
            const Slot& slot = slots_[index];
            if (!slot.occupied || !slot.queued) continue;
            const std::uint32_t priority = static_cast<std::uint32_t>(slot.priority);
            if (force_aging && priority >= static_cast<std::uint32_t>(Priority::High)) {
                continue;
            }
            if (selected == kMaxQueuedTasks || priority > selected_priority ||
                (priority == selected_priority && slot.sequence < selected_sequence)) {
                selected = index;
                selected_priority = priority;
                selected_sequence = slot.sequence;
            }
        }
        if (selected != kMaxQueuedTasks || !force_aging) return selected;

        /* If every queued task is high priority, make progress rather than
         * manufacturing an idle result. */
        selected_sequence = UINT64_MAX;
        for (std::size_t index = 0u; index < kMaxQueuedTasks; ++index) {
            const Slot& slot = slots_[index];
            if (slot.occupied && slot.queued && slot.sequence < selected_sequence) {
                selected = index;
                selected_sequence = slot.sequence;
            }
        }
        return selected;
    }

    Slot slots_[kMaxQueuedTasks]{};
    std::size_t queue_count_ = 0u;
    std::uint64_t next_id_ = 1u;
    std::uint64_t next_sequence_ = 1u;
    std::uint32_t high_priority_burst_ = 0u;
    bool shutting_down_ = false;
};

} // namespace RinExecutor

#endif /* RINEXECUTOR_EXECUTOR_HPP */
