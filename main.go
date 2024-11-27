package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"

	"strings"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	listenAddress = flag.String("web.listen-address", ":10025",
		"Address to listen on for HTTP requests.")
	metricsPath = flag.String("web.metrics-path", "/metrics",
		"Path to expose metrics on.")

	// Provided at build time
	builtBy, commit, date, version string
)

func main() {
	flag.Parse()
	blocks := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bsky_blocks_total",
		Help: "Number of Bluesky blocks",
	})
	follows := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bsky_follows_total",
		Help: "Number of Bluesky follows",
	})
	likes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bsky_likes_total",
		Help: "Number of Bluesky likes",
	})
	posts := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bsky_posts_total",
		Help: "Number of Bluesky posts",
	})
	reposts := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bsky_reposts_total",
		Help: "Number of Bluesky reposts",
	})
	build_info := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bsky_firehose_exporter_build_info",
		Help: "Build info about the bsky_firehose_exporter",
		ConstLabels: prometheus.Labels{
			"builtBy": builtBy,
			"commit":  commit,
			"date":    date,
			"version": version,
		}})

	prometheus.MustRegister(blocks, follows, likes, posts, reposts, build_info)
	build_info.Set(1)

	http.Handle(*metricsPath, promhttp.Handler())
	go func() {
		slog.Info("Starting metrics server",
			"web.listen-address", *listenAddress,
			"web.metrics-path", *metricsPath,
		)
		log.Fatal(http.ListenAndServe(*listenAddress, nil))
	}()

	uri := "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	con, _, err := websocket.DefaultDialer.Dial(uri, http.Header{})
	if err != nil {
		slog.Error("Error dialing websocket", "err", err, "uri", uri)
		os.Exit(1)
	}

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			// fmt.Println("Event from ", evt.Repo)
			for _, op := range evt.Ops {
				// fmt.Printf(" - %s record %s\n", op.Action, op.Path)
				if op.Action == "create" {
					switch {
					case strings.Contains(op.Path, "app.bsky.graph.block"):
						blocks.Inc()
					case strings.Contains(op.Path, "app.bsky.feed.like"):
						likes.Inc()
					case strings.Contains(op.Path, "app.bsky.graph.follow"):
						follows.Inc()
					case strings.Contains(op.Path, "app.bsky.feed.post"):
						posts.Inc()
					case strings.Contains(op.Path, "app.bsky.feed.repost"):
						reposts.Inc()
					}
				}
			}
			return nil
		},
	}

	// sched := sequential.NewScheduler("exporter", rsc.EventHandler)
	sched := parallel.NewScheduler(24, 10000, "parallel", rsc.EventHandler)
	slog.Info("Exporter started, beginning to slurp the stream")
	events.HandleRepoStream(context.Background(), con, sched)
}
