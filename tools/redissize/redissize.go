package main

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/docker/go-units"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

var (
	target = flag.String("target", "localhost:6380", "")
)

func main() {
	flag.Parse()

	c := redis.NewClient(&redis.Options{Addr: *target})

	sizes := make(map[string]int64)
	counts := make(map[string]int64)

	type prefixSize struct {
		prefix string
		size   int64
	}

	ctx := context.Background()
	cursor := uint64(0)
	count := 0
	totalSize := int64(0)
	for {
		keys, newCursor, err := c.Scan(ctx, cursor, "*", 1000).Result()
		if err != nil {
			log.Fatalf("scan err: %s", err)
		}
		if newCursor == 0 {
			break
		}
		cursor = newCursor

		cmds, err := c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, k := range keys {
				pipe.MemoryUsage(ctx, k)
			}
			return nil
		})
		if err != nil {
			log.Fatalf("Pipeline error: %s", err)
		}

		for i, k := range keys {
			p := strings.Split(k, "/")[0]
			v, err := cmds[i].(*redis.IntCmd).Result()
			if err != nil && err != redis.Nil {
				log.Warningf("memusage err: %s", err)
			}
			if _, err := uuid.Parse(p); err == nil {
				p = "logs"
			}
			if p == "hit_tracker" && strings.HasSuffix(k, "/results") {
				p = "hit_tracker_results"
			} else if p == "hit_tracker" {
				p = "hit_tracker_counters"
			}
			//if v > 3500 {
			//	log.Infof("key %q v %d", k, v)
			//}
			totalSize += v
			sizes[p] += v
			counts[p] += 1
			count++
			if count%1000 == 0 {
				log.Infof("keys scanned: %d", count)
				log.Infof("average size: %f", float64(totalSize)/float64(count))
				var s []prefixSize
				for k, v := range sizes {
					s = append(s, prefixSize{k, v})
				}
				slices.SortFunc(s, func(a, b prefixSize) int {
					if a.size > b.size {
						return -1
					} else if a.size == b.size {
						return 0
					} else {
						return 1
					}
				})
				for _, v := range s {
					log.Infof("Prefix %q size %s, avg %s, n=%d", v.prefix, units.BytesSize(float64(v.size)), units.BytesSize(float64(v.size/counts[v.prefix])), counts[v.prefix])
				}
				fmt.Println()
			}
		}
	}
}
