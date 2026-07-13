package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/VocalVirus/jobq/internal/job"
)

// blockTimeout is how long Dequeue waits for a new job before returning ErrNoJob.
// Bounded (rather than forever) so a shutdown signal can be noticed promptly.
const blockTimeout = 5 * time.Second

// RedisQueue implements Queue using a Redis Stream + consumer group.
//
// Why Streams (not a plain list)? A consumer group tracks which messages have
// been delivered but not yet acknowledged (the "pending entries list"). If a
// worker reads a job and crashes before acking, the job stays pending and can
// be reclaimed — giving us at-least-once delivery for free.
type RedisQueue struct {
	client   *redis.Client
	stream   string // the Redis Stream key that holds jobs
	group    string // consumer group name (shared by all workers)
	consumer string // this instance's name within the group
}

// NewRedisQueue connects to Redis and ensures the stream + consumer group exist.
func NewRedisQueue(addr, stream, group, consumer string) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create the group (and the stream, via MKSTREAM) starting from "$" = only
	// new messages. If the group already exists Redis returns BUSYGROUP, which
	// is fine — we just reuse it.
	err := client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		_ = client.Close()
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	return &RedisQueue{client: client, stream: stream, group: group, consumer: consumer}, nil
}

// Enqueue serializes the job to JSON and appends it to the stream (XADD).
func (q *RedisQueue) Enqueue(ctx context.Context, j job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{"job": data},
	}).Err()
}

// Dequeue reads the next undelivered job for this group (XREADGROUP with ">"),
// blocking up to blockTimeout. Returns ErrNoJob if nothing arrived in time.
func (q *RedisQueue) Dequeue(ctx context.Context) (Message, error) {
	res, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: q.consumer,
		Streams:  []string{q.stream, ">"}, // ">" = messages never delivered to this group
		Count:    1,
		Block:    blockTimeout,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return Message{}, ErrNoJob // blocked, timed out, nothing to do
		}
		return Message{}, err
	}

	entry := res[0].Messages[0]

	// The payload was stored under the "job" field as JSON bytes; Redis returns
	// it as a string.
	raw, _ := entry.Values["job"].(string)
	var j job.Job
	if err := json.Unmarshal([]byte(raw), &j); err != nil {
		return Message{}, fmt.Errorf("unmarshal job %s: %w", entry.ID, err)
	}

	return Message{Job: j, id: entry.ID}, nil
}

// Ack confirms a message so Redis stops tracking it as pending (XACK).
func (q *RedisQueue) Ack(ctx context.Context, m Message) error {
	return q.client.XAck(ctx, q.stream, q.group, m.id).Err()
}

// Close releases the Redis connection.
func (q *RedisQueue) Close() error {
	return q.client.Close()
}
