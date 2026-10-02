package workflowprobe

import (
	"fmt"
	"net/http"
	"slices"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runConsistencyAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-assess-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-assess-02", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "assess.example", 3001, 2, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "assess-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("assess-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}

	var consistent domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &consistent); err != nil {
		return err
	}
	if consistent.Verdict != domain.VerdictConsistent || consistent.ProbeCount != 2 {
		return ensure(false, "consistent assessment did not include both probes")
	}
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != consistent.ID {
		return ensure(false, "assessment replay returned another result")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != consistent.ID {
		return ensure(false, "latest assessment does not match")
	}

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 20)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	mismatchedSerial := reviewRevision.Serial + 1
	stored, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		mismatchedSerial,
		10,
	)
	if err != nil {
		return err
	}
	// The serial reported by the node is preserved verbatim on the observation;
	// mismatched observations are still accepted and saved, never rejected.
	if stored.ObservedSerial != mismatchedSerial {
		return ensure(false, "submitted observation serial was rewritten")
	}
	var review domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &review); err != nil {
		return err
	}
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "mismatched serial did not require review")
	}
	if !slices.Contains(review.Reasons, "serial_mismatch") {
		return ensure(false, "mismatched serial did not report the serial_mismatch reason")
	}
	// Replaying the same assessment request returns the same immutable verdict.
	var reviewReplay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &reviewReplay); err != nil {
		return err
	}
	if reviewReplay.ID != review.ID || reviewReplay.Verdict != domain.VerdictReviewRequired ||
		!slices.Contains(reviewReplay.Reasons, "serial_mismatch") {
		return ensure(false, "assessment replay changed the review verdict")
	}
	// A fresh assessment request against the same observations reaches the same
	// deterministic conclusion.
	var reviewAgain domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0002",
	}, &reviewAgain); err != nil {
		return err
	}
	if reviewAgain.Verdict != domain.VerdictReviewRequired ||
		!slices.Contains(reviewAgain.Reasons, "serial_mismatch") {
		return ensure(false, "fresh assessment of the same probes changed the verdict")
	}
	// After a full process-style restart the persisted observation still carries
	// the reported serial (a fresh assessment over the same probes must keep
	// flagging the mismatch) and the verdict stays review_required.
	if err := harness.restart(); err != nil {
		return err
	}
	var reviewAfterRestart domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0003",
	}, &reviewAfterRestart); err != nil {
		return err
	}
	if reviewAfterRestart.Verdict != domain.VerdictReviewRequired ||
		!slices.Contains(reviewAfterRestart.Reasons, "serial_mismatch") {
		return ensure(false, "verdict changed after restart")
	}
	var reviewReplayAfterRestart domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &reviewReplayAfterRestart); err != nil {
		return err
	}
	if reviewReplayAfterRestart.ID != review.ID {
		return ensure(false, "idempotent assessment replay changed after restart")
	}
	return nil
}
