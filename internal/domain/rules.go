package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// RolloutPin is the zone serial identity a rollout is expected to converge on.
// It is derived from the immutable revision a plan has pinned and is used
// whenever observations reported by the fleet are reconciled with that rollout.
type RolloutPin struct {
	PlanID   string
	Revision string
	Serial   uint32
}

// NewRolloutPin derives the rollout identity from a plan and its revision.
func NewRolloutPin(plan RolloutPlan, revision ZoneRevision) RolloutPin {
	return RolloutPin{
		PlanID:   plan.ID,
		Revision: revision.ID,
		Serial:   revision.Serial,
	}
}

// Aligned maps an observation onto the rollout identity pinned for the plan so
// that every observation belonging to the same rollout shares one serial.
func (pin RolloutPin) Aligned(observed uint32) uint32 {
	if pin.Serial == 0 {
		return observed
	}
	return pin.Serial
}

// Matches reports whether an observation belongs to this pinned rollout.
func (pin RolloutPin) Matches(observed uint32) bool {
	return observed == pin.Serial
}

// SerialDrift reports how far an observation sits from the pinned serial.
func (pin RolloutPin) SerialDrift(observed uint32) uint32 {
	if observed >= pin.Serial {
		return observed - pin.Serial
	}
	return pin.Serial - observed
}

// AlignedProbe returns the observation stamped with this rollout identity.
func (pin RolloutPin) AlignedProbe(probe Probe) Probe {
	probe.ObservedSerial = pin.Aligned(probe.ObservedSerial)
	return probe
}

// RolloutPinFromState resolves the rollout identity a plan currently pins.
func RolloutPinFromState(state *State, planID string) (RolloutPin, bool) {
	plan, ok := state.Plans[planID]
	if !ok {
		return RolloutPin{}, false
	}
	revision, ok := state.Revisions[plan.LatestRevisionID]
	if !ok {
		return RolloutPin{}, false
	}
	return NewRolloutPin(plan, revision), true
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
	}
	probeCount := len(probes)
	successCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	pin := NewRolloutPin(plan, revision)
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Success {
			successCount++
			if !pin.Matches(pin.Aligned(probe.ObservedSerial)) {
				reasons["serial_mismatch"] = struct{}{}
			}
		}
	}
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       probeCount,
		SuccessCount:     successCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: maximumLatency,
		CreatedAt:        now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
