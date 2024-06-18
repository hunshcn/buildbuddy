package action_merger

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/go-redis/redis/v8"
)

const (
	// TTL for action-merging data about queued executions. This should be
	// approximately equal to the longest execution queue times.
	queuedExecutionTTL = 10 * time.Minute

	// Default TTL for action-merging data about claimed executions. This is
	// set to twice the length of `remote_execution.lease_duration` to give a
	// short grace period in the event of missed leases.
	DefaultClaimedExecutionTTL = 20 * time.Second
)

var (
	enableActionMerging = flag.Bool("remote_execution.enable_action_merging", true, "If enabled, identical actions being executed concurrently are merged into a single execution.")
	hedgedActionCount   = flag.Int("remote_execution.action_merging_hedges", 1, "When action merging is enabled, merged actions are run this many extra times if subsequent requests are received while the merged-against action is still pending.")
)

// These are: pendingExecution/ANON/action-digest and pendingExecutionCount/ANON/action-digest
func redisKeysForPendingExecution(ctx context.Context, adResource *digest.ResourceName) (string, string, error) {
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", "", err
	}
	downloadString, err := adResource.DownloadString()
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("pendingExecution/%s%s", userPrefix, downloadString), fmt.Sprintf("pendingExecutionCount/%s%s", userPrefix, downloadString), nil
}

// These are: pendingExecutionDigest/execution-id
func redisKeyForPendingExecutionDigest(executionID string) string {
	return fmt.Sprintf("pendingExecutionDigest/%s", executionID)
}

func MergeAction(count int) bool {
	if !*enableActionMerging {
		return false
	}
	if count < *hedgedActionCount+1 {
		return false
	}
	return true
}

// Action merging is an optimization that detects when an execution is
// requested for an action that is in-flight, but not yet in the action cache.
// This optimization is particularly helpful for preventing duplicate work for
// long-running actions. It is implemented as a pair of entries in redis, one
// from the action digest to the execution ID (the forward mapping), and one
// from the execution ID to the action digest (the reverse mapping). These
// entries are initially written to Redis when an execution is enqueued, and
// then rewritten (to extend the TTL) while the action is running. Because
// the queueing mechanism isn't 100% reliable, it is possible for the merging
// data to exist in Redis pointing to a dead execution. For this reason, the
// TTLs are set somewhat conservatively so this problem self-heals reasonably
// quickly.
//
// This function records a queued execution in Redis.
func RecordQueuedExecution(ctx context.Context, rdb redis.UniversalClient, executionID string, adResource *digest.ResourceName) error {
	if !*enableActionMerging {
		return nil
	}

	forwardKey, countKey, err := redisKeysForPendingExecution(ctx, adResource)
	if err != nil {
		return err
	}
	reverseKey := redisKeyForPendingExecutionDigest(executionID)
	pipe := rdb.TxPipeline()
	pipe.Set(ctx, forwardKey, executionID, queuedExecutionTTL)
	pipe.Incr(ctx, countKey)
	pipe.Expire(ctx, countKey, queuedExecutionTTL)
	pipe.Set(ctx, reverseKey, forwardKey, queuedExecutionTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// This function records a claimed execution in Redis.
func RecordClaimedExecution(ctx context.Context, rdb redis.UniversalClient, executionID string, ttl time.Duration) error {
	if !*enableActionMerging {
		return nil
	}

	// Use the execution ID to lookup the action digest and insert the forward
	// entry (from action digest to execution ID).
	reverseKey := redisKeyForPendingExecutionDigest(executionID)
	forwardKey, err := rdb.Get(ctx, reverseKey).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}

	// No need to update the count here as it was set in RecordQueuedExecution.
	pipe := rdb.TxPipeline()
	pipe.Set(ctx, forwardKey, executionID, ttl)
	pipe.Set(ctx, reverseKey, forwardKey, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// Returns the execution ID of a pending execution working on the action with
// the provided action digest, or an empty string and possibly an error if no
// pending execution was found.
func FindPendingExecution(ctx context.Context, rdb redis.UniversalClient, schedulerService interfaces.SchedulerService, adResource *digest.ResourceName) (string, int, error) {
	if !*enableActionMerging {
		return "", 0, nil
	}

	executionIDKey, countKey, err := redisKeysForPendingExecution(ctx, adResource)
	if err != nil {
		return "", 0, nil
	}
	executionID, err := rdb.Get(ctx, executionIDKey).Result()
	if err == redis.Nil {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, nil
	}

	count, err := rdb.Get(ctx, countKey).Int()
	if err == redis.Nil {
		// There must be at least one as we didn't exit above.
		count = 1
	}
	if err != nil {
		return "", 0, nil
	}

	// Validate that the reverse mapping exists as well. The reverse mapping is
	// used to delete the pending task information when the task is done.
	// Bail out if it doesn't exist.
	err = rdb.Get(ctx, redisKeyForPendingExecutionDigest(executionID)).Err()
	if err == redis.Nil {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, nil
	}

	// Finally, confirm this execution exists in the scheduler and hasn't been
	// lost somehow.
	ok, err := schedulerService.ExistsTask(ctx, executionID)
	if err != nil {
		return "", 0, nil
	}
	if !ok {
		log.CtxWarningf(ctx, "Pending execution %q does not exist in the scheduler", executionID)
		return "", 0, nil
	}
	return executionID, count, nil
}

// Deletes the pending execution with the provided execution ID.
func DeletePendingExecution(ctx context.Context, rdb redis.UniversalClient, executionID string) error {
	if !*enableActionMerging {
		return nil
	}

	pendingExecutionDigestKey := redisKeyForPendingExecutionDigest(executionID)
	pendingExecutionKey, err := rdb.Get(ctx, pendingExecutionDigestKey).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if err := rdb.Del(ctx, pendingExecutionKey).Err(); err != nil {
		log.CtxWarningf(ctx, "could not delete pending execution key %q: %s", pendingExecutionKey, err)
	}
	if err := rdb.Del(ctx, pendingExecutionDigestKey).Err(); err != nil {
		log.CtxWarningf(ctx, "could not delete pending execution digest key %q: %s", pendingExecutionDigestKey, err)
	}
	return nil
}
