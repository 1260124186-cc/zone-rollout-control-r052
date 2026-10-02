package workflowprobe

import (
	"fmt"
	"net/http"

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

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 40)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	mismatched, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		reviewRevision.Serial+1,
		30,
	)
	if err != nil {
		return err
	}
	// The observation must be accepted and stored with the serial the node
	// actually reported, even though it disagrees with the locked revision.
	if mismatched.ObservedSerial != reviewRevision.Serial+1 {
		return ensure(false, "reported serial was not preserved on the stored probe")
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
	if len(review.Reasons) != 1 || review.Reasons[0] != "serial_mismatch" {
		return ensure(false, fmt.Sprintf("expected only serial_mismatch, got %v", review.Reasons))
	}
	// Replaying the same assessment request must return the identical verdict.
	var reviewReplay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &reviewReplay); err != nil {
		return err
	}
	if reviewReplay.ID != review.ID ||
		reviewReplay.Verdict != review.Verdict ||
		len(reviewReplay.Reasons) != 1 || reviewReplay.Reasons[0] != "serial_mismatch" {
		return ensure(false, "assessment replay returned a different verdict")
	}
	// A service restart must keep both the raw reported serial and the verdict:
	// re-evaluating with a fresh request key reaches the same conclusion.
	if err := harness.restart(); err != nil {
		return err
	}
	var restartedLatest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+reviewPlan.ID+"/assessments/latest", nil, &restartedLatest); err != nil {
		return err
	}
	if restartedLatest.ID != review.ID ||
		restartedLatest.Verdict != domain.VerdictReviewRequired ||
		len(restartedLatest.Reasons) != 1 || restartedLatest.Reasons[0] != "serial_mismatch" {
		return ensure(false, "restart changed the stored assessment verdict")
	}
	var restartedReview domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0002",
	}, &restartedReview); err != nil {
		return err
	}
	if restartedReview.Verdict != domain.VerdictReviewRequired ||
		len(restartedReview.Reasons) != 1 || restartedReview.Reasons[0] != "serial_mismatch" {
		return ensure(false, "post-restart reassessment did not flag the serial mismatch")
	}
	return nil
}
