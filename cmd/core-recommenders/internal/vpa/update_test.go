package vpa

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func q(s string) *resource.Quantity {
	v := resource.MustParse(s)
	return &v
}

func TestNeedsUpdate(t *testing.T) {
	bounded := map[string]resourceRecommendation{
		"cpu":    {Target: *q("500m"), Lower: q("400m"), Upper: q("800m")},
		"memory": {Target: *q("512Mi"), Lower: q("256Mi"), Upper: q("1Gi")},
	}
	tests := []struct {
		name       string
		current    map[string]string
		recs       map[string]resourceRecommendation
		want       bool
		wantReason string
	}{
		{
			name:       "below the lower bound",
			current:    map[string]string{"cpu": "300m", "memory": "512Mi"},
			recs:       bounded,
			want:       true,
			wantReason: "cpu request 300m is below the lower bound 400m",
		},
		{
			name:       "above the upper bound",
			current:    map[string]string{"cpu": "500m", "memory": "2Gi"},
			recs:       bounded,
			want:       true,
			wantReason: "memory request 2Gi is above the upper bound 1Gi",
		},
		{
			name:       "within the bounds, a single resource changes by at least 10%",
			current:    map[string]string{"cpu": "450m", "memory": "512Mi"}, // 50/450 = 11%
			recs:       bounded,
			want:       true,
			wantReason: "changes by at least 10%",
		},
		{
			name: "within the bounds, small changes are not summed",
			// 40/500 = 8% and 35/512 ~ 7%: OSS VPA would sum them to 15%.
			current:    map[string]string{"cpu": "540m", "memory": "547Mi"},
			recs:       bounded,
			want:       false,
			wantReason: "within bounds",
		},
		{
			name:       "at the bounds",
			current:    map[string]string{"cpu": "400m", "memory": "1Gi"},
			recs:       map[string]resourceRecommendation{"cpu": {Target: *q("420m"), Lower: q("400m"), Upper: q("800m")}, "memory": {Target: *q("1000Mi"), Lower: q("256Mi"), Upper: q("1Gi")}},
			want:       false,
			wantReason: "within bounds",
		},
		{
			name:       "missing current request",
			current:    map[string]string{"cpu": "500m"},
			recs:       bounded,
			want:       true,
			wantReason: "no current memory request",
		},
		{
			name:       "invalid current request",
			current:    map[string]string{"cpu": "lots", "memory": "512Mi"},
			recs:       bounded,
			want:       true,
			wantReason: `invalid current cpu request "lots"`,
		},
		{
			name:       "without bounds, any difference",
			current:    map[string]string{"cpu": "499m"},
			recs:       map[string]resourceRecommendation{"cpu": {Target: *q("500m")}},
			want:       true,
			wantReason: "differs from the target",
		},
		{
			name:    "without bounds, equal in another unit",
			current: map[string]string{"cpu": "0.5"},
			recs:    map[string]resourceRecommendation{"cpu": {Target: *q("500m")}},
			want:    false,
		},
		{
			name:       "one-sided bound",
			current:    map[string]string{"cpu": "900m"},
			recs:       map[string]resourceRecommendation{"cpu": {Target: *q("500m"), Lower: q("400m")}},
			want:       true,
			wantReason: "changes by at least 10%",
		},
		{
			name:       "zero current request",
			current:    map[string]string{"cpu": "0"},
			recs:       map[string]resourceRecommendation{"cpu": {Target: *q("500m"), Lower: q("0"), Upper: q("800m")}},
			want:       true,
			wantReason: "changes by at least 10%",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := needsUpdate(tt.current, tt.recs)
			if got != tt.want {
				t.Errorf("needsUpdate() = %v (%s), want %v", got, reason, tt.want)
			}
			if !strings.Contains(reason, tt.wantReason) {
				t.Errorf("needsUpdate() reason = %q, want it to contain %q", reason, tt.wantReason)
			}
		})
	}
}
