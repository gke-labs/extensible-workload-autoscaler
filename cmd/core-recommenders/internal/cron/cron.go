package cron

import (
	"fmt"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
)

type Recommender struct {
	Clock clock.Clock
}

type config struct {
	start    string
	end      string
	location *time.Location
	replicas *int32
}

func (r *Recommender) Recommend(def *pb.RecommenderDefinition, _, _ *pb.ControlMetrics, _ int32, _ *pb.Workload) *pb.Recommendation {
	cfg, err := parseConfig(def)
	if err != nil {
		return pb.Recommendation_builder{
			Message: proto.String(err.Error()),
		}.Build()
	}

	isActive, err := checkCron(cfg.start, cfg.end, cfg.location, r.now())
	if err != nil {
		return pb.Recommendation_builder{
			Message: proto.String(err.Error()),
		}.Build()
	}

	var replicas *int32
	if cfg.replicas != nil {
		desired := int32(0)
		if isActive {
			desired = *cfg.replicas
		}
		replicas = &desired
	}

	return pb.Recommendation_builder{
		IsActive: proto.Bool(isActive),
		Replicas: replicas,
	}.Build()
}

func (r *Recommender) now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}

func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	startStr := def.GetParams()["start"]
	endStr := def.GetParams()["end"]

	if startStr == "" {
		return nil, fmt.Errorf("missing start param")
	}
	if endStr == "" {
		return nil, fmt.Errorf("missing end param")
	}

	loc := time.Local
	if tz := def.GetParams()["timezone"]; tz != "" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone: %v", err)
		}
		loc = l
	}

	var replicas *int32
	if repStr := def.GetParams()["replicas"]; repStr != "" {
		v, err := strconv.ParseInt(repStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid replicas format")
		}
		i32 := int32(v)
		replicas = &i32
	}

	return &config{
		start:    startStr,
		end:      endStr,
		location: loc,
		replicas: replicas,
	}, nil
}

func checkCron(start, end string, loc *time.Location, now time.Time) (bool, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	s, err := parser.Parse(start)
	if err != nil {
		return false, err
	}
	e, err := parser.Parse(end)
	if err != nil {
		return false, err
	}

	nowInLoc := now.In(loc)
	nextStart := s.Next(nowInLoc)
	nextEnd := e.Next(nowInLoc)

	return nextStart.After(nextEnd), nil
}
