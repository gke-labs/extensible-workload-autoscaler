package policy

import (
	"fmt"
	"slices"
	"strings"

	"github.com/mennanov/fmutils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

const (
	// UpdateMaskWildcard selects every field set in the given policy, leaving
	// the fields it does not set untouched.
	UpdateMaskWildcard = "*"

	// recommenderMetricsField is the name of the Policy field holding the
	// metrics owned by individual recommenders.
	recommenderMetricsField = "recommender_metrics"
)

// policyFields describes the fields of a Policy, used to resolve mask paths.
var policyFields = (*pb.Policy)(nil).ProtoReflect().Descriptor().Fields()

// RecommenderMetricsPath returns the update mask path selecting the metrics
// owned by a single recommender, leaving the entries of the other recommenders
// untouched.
func RecommenderMetricsPath(recommenderName string) string {
	return recommenderMetricsField + "." + recommenderName
}

// ValidateUpdateMask reports whether every path of an update mask is understood
// by ApplyUpdateMask. A nil or empty mask is valid: it requests a full
// replacement of the resource.
func ValidateUpdateMask(mask *fieldmaskpb.FieldMask) error {
	_, err := ApplyUpdateMask(&pb.Policy{}, &pb.Policy{}, mask)
	return err
}

// ApplyUpdateMask returns the policy resulting from applying update on top of
// current, restricted to the fields selected by mask. Neither current nor
// update is modified.
//
// The supported mask paths are:
//   - Empty mask: update the whole resource, i.e. update is the new policy.
//   - "*": every field set in update is copied over, the fields update leaves
//     unset keep their current value.
//   - a field name, nested or not (e.g. "max_replicas", "workload.name").
//   - a key of a map field (e.g. "recommender_metrics.my-recommender"): only that
//     entry is replaced, or removed if update does not hold it.
func ApplyUpdateMask(current, update *pb.Policy, mask *fieldmaskpb.FieldMask) (*pb.Policy, error) {
	if len(mask.GetPaths()) == 0 {
		return update, nil
	}
	if current == nil {
		current = &pb.Policy{Id: update.GetId()}
	}

	// Clone both sides: the result must not alias memory owned by the caller.
	merged := proto.Clone(current).(*pb.Policy)
	src := proto.Clone(update).(*pb.Policy)

	paths, err := resolvePaths(mask.GetPaths(), src)
	if err != nil {
		return nil, err
	}
	entries := selectedMapEntries(paths)

	// fmutils drops the entries of a masked map that the update holds but the
	// mask leaves out. Dropping them from the update first keeps them untouched.
	keepSelectedMapEntries(src, entries)

	fmutils.Overwrite(src, merged, paths)

	// fmutils only copies the entries the update holds, so removing an entry is
	// done here.
	clearMissingMapEntries(merged, src, entries)

	return merged, nil
}

// resolvePaths expands the wildcard and reports the paths that fmutils would
// misread. Validating is required: fmutils panics on unknown fields.
func resolvePaths(paths []string, update *pb.Policy) ([]string, error) {
	resolved := make([]string, 0, len(paths))
	for _, path := range paths {
		if path == UpdateMaskWildcard {
			resolved = append(resolved, setFieldPaths(update)...)
			continue
		}
		// fmutils silently ignores empty segments, which would turn
		// "recommender_metrics." into a mask over the whole field.
		if slices.Contains(strings.Split(path, "."), "") {
			return nil, fmt.Errorf("path %q: empty segment", path)
		}
		resolved = append(resolved, path)
	}
	if err := fmutils.Validate(&pb.Policy{}, resolved); err != nil {
		return nil, err
	}
	return resolved, nil
}

// setFieldPaths returns the path of every field the policy sets.
func setFieldPaths(p *pb.Policy) []string {
	var paths []string
	p.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		paths = append(paths, fd.TextName())
		return true
	})
	return paths
}

// selectedMapEntries returns the keys the paths select in each map field of the
// policy, e.g. "recommender_metrics.my-recommender" selects the key
// "my-recommender" of the recommender_metrics field.
func selectedMapEntries(paths []string) map[protoreflect.FieldDescriptor]map[string]bool {
	entries := make(map[protoreflect.FieldDescriptor]map[string]bool)
	for _, path := range paths {
		field, rest, selectsEntry := strings.Cut(path, ".")
		if !selectsEntry {
			continue
		}
		fd := policyFields.ByName(protoreflect.Name(field))
		if fd == nil || !fd.IsMap() || fd.MapKey().Kind() != protoreflect.StringKind {
			continue
		}
		// Only the segment right after the map field is a key; the deeper ones
		// address the fields of the entry.
		key, _, _ := strings.Cut(rest, ".")
		if entries[fd] == nil {
			entries[fd] = make(map[string]bool)
		}
		entries[fd][key] = true
	}
	return entries
}

// keepSelectedMapEntries drops from p every entry of the given map fields that is not selected.
func keepSelectedMapEntries(p *pb.Policy, entries map[protoreflect.FieldDescriptor]map[string]bool) {
	msg := p.ProtoReflect()
	for fd, keys := range entries {
		if !msg.Has(fd) {
			continue
		}
		m := msg.Mutable(fd).Map()
		m.Range(func(key protoreflect.MapKey, _ protoreflect.Value) bool {
			if !keys[key.String()] {
				m.Clear(key)
			}
			return true
		})
	}
}

// clearMissingMapEntries removes from dst the selected entries that src does not hold.
func clearMissingMapEntries(dst, src *pb.Policy, entries map[protoreflect.FieldDescriptor]map[string]bool) {
	dstMsg, srcMsg := dst.ProtoReflect(), src.ProtoReflect()
	for fd, keys := range entries {
		if !dstMsg.Has(fd) {
			continue
		}
		srcMap, dstMap := srcMsg.Get(fd).Map(), dstMsg.Mutable(fd).Map()
		for key := range keys {
			mapKey := protoreflect.ValueOfString(key).MapKey()
			if !srcMap.Get(mapKey).IsValid() {
				dstMap.Clear(mapKey)
			}
		}
	}
}
