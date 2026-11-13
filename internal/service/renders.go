package service

import (
	"context"
	"time"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/render"
)

func (s *Service) CreateRender(
	ctx context.Context,
	id string,
	request RenderWriteRequest,
) (domain.RenderJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.RenderJob{}, false, err
	}
	if err := domain.ValidateResourceID("render_id", id); err != nil {
		return domain.RenderJob{}, false, err
	}
	if err := domain.ValidateResourceID("timeline_id", request.TimelineID); err != nil {
		return domain.RenderJob{}, false, err
	}

	existing, getErr := s.repo.GetJob(id)
	if getErr == nil {
		if existing.TimelineID != request.TimelineID {
			return domain.RenderJob{}, false, domain.E(domain.KindConflict, "render_conflict", "render_id", "identifier is already bound to a different timeline")
		}
		settled, err := s.settleInterruptedJob(existing)
		if err != nil {
			return domain.RenderJob{}, false, err
		}
		return settled, false, nil
	}
	if appErr := domain.AsAppError(getErr); appErr.Kind != domain.KindNotFound {
		return domain.RenderJob{}, false, getErr
	}

	plan, err := s.repo.GetTimeline(request.TimelineID)
	if err != nil {
		return domain.RenderJob{}, false, err
	}
	now := time.Now().UTC()
	job := domain.RenderJob{
		ID:         id,
		TimelineID: plan.ID,
		State:      domain.RenderQueued,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.repo.SaveJob(job); err != nil {
		return domain.RenderJob{}, false, err
	}
	if err := s.advanceJob(&job, domain.RenderRendering, nil); err != nil {
		return job, true, err
	}

	sources := make(map[string]render.SourceInput)
	for _, span := range plan.Spans {
		if _, loaded := sources[span.SourceID]; loaded {
			continue
		}
		asset, payload, loadErr := s.repo.GetSource(span.SourceID)
		if loadErr != nil {
			return s.failRender(job, loadErr)
		}
		sources[span.SourceID] = render.SourceInput{Asset: asset, WAV: payload}
	}
	// The render outlives the request that started it: a client disconnect
	// must neither strand the durable job in an active state nor discard
	// work that finishes after the caller went away.
	result, renderErr := render.Render(
		context.WithoutCancel(ctx),
		id,
		plan,
		sources,
		render.Limits{MaxOutputFrames: s.config.MaxOutputFrames},
		time.Now().UTC(),
	)
	if renderErr != nil {
		return s.failRender(job, domain.Wrap(domain.KindInternal, "render_failed", "", "timeline rendering failed", renderErr))
	}
	if err := s.advanceJob(&job, domain.RenderFinalizing, nil); err != nil {
		return job, true, err
	}
	if err := s.repo.CommitRenderArtifacts(id, result.WAV, result.Receipt); err != nil {
		return s.failRender(job, err)
	}
	// The artifacts are committed atomically; from here the only honest
	// outcome is completed, whatever happens to the client connection.
	job.Output = &result.Output
	job.ReceiptDigest = result.Output.Digest
	if err := s.advanceJob(&job, domain.RenderCompleted, nil); err != nil {
		return job, true, err
	}
	return job, true, nil
}

// ReconcileRenders settles render jobs that a previous service lifetime
// left in an active state, for example by stopping mid-render. A job
// whose output and receipt were fully committed is completed from the
// durable receipt; any other active job is failed with a stable
// interruption code and its partial artifacts are removed. Terminal jobs
// are never rewritten, so completed renders keep delivering their
// original output and receipt across restarts.
func (s *Service) ReconcileRenders(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	jobs, err := s.repo.ListJobs()
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if _, err := s.settleInterruptedJob(job); err != nil {
			return err
		}
	}
	return nil
}

// settleInterruptedJob moves a job stuck in an active state to the
// terminal state that matches the committed artifacts. Terminal jobs are
// returned unchanged.
func (s *Service) settleInterruptedJob(job domain.RenderJob) (domain.RenderJob, error) {
	switch job.State {
	case domain.RenderCompleted, domain.RenderFailed:
		return job, nil
	}
	if receipt, ok := s.committedReceipt(job); ok {
		job.Output = &receipt.Output
		job.ReceiptDigest = receipt.Output.Digest
		if err := s.completeCommittedJob(&job); err != nil {
			return domain.RenderJob{}, err
		}
		return job, nil
	}
	detail := &domain.ErrorDetail{
		Code:    "render_interrupted",
		Message: "render was interrupted before completion; submit a new render_id to retry",
		Field:   "render_id",
	}
	if err := s.advanceJob(&job, domain.RenderFailed, detail); err != nil {
		return domain.RenderJob{}, err
	}
	if err := s.repo.RemoveRenderArtifacts(job.ID); err != nil {
		return domain.RenderJob{}, err
	}
	return job, nil
}

// committedReceipt reports the committed receipt for a job when the
// render folder holds a complete output and receipt pair that belongs to
// this job.
func (s *Service) committedReceipt(job domain.RenderJob) (domain.RenderReceipt, bool) {
	receipt, err := s.repo.GetRenderReceipt(job.ID)
	if err != nil {
		return domain.RenderReceipt{}, false
	}
	if receipt.RenderID != job.ID || receipt.TimelineID != job.TimelineID {
		return domain.RenderReceipt{}, false
	}
	size, err := s.repo.RenderOutputSize(job.ID)
	if err != nil || size != receipt.Output.ByteCount {
		return domain.RenderReceipt{}, false
	}
	return receipt, true
}

// completeCommittedJob walks a job with committed artifacts through the
// remaining valid transitions into completed.
func (s *Service) completeCommittedJob(job *domain.RenderJob) error {
	switch job.State {
	case domain.RenderQueued:
		if err := s.advanceJob(job, domain.RenderRendering, nil); err != nil {
			return err
		}
		fallthrough
	case domain.RenderRendering:
		if err := s.advanceJob(job, domain.RenderFinalizing, nil); err != nil {
			return err
		}
		fallthrough
	case domain.RenderFinalizing:
		if err := s.advanceJob(job, domain.RenderCompleted, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) advanceJob(job *domain.RenderJob, state domain.RenderState, detail *domain.ErrorDetail) error {
	if err := job.Transition(state, detail, time.Now().UTC()); err != nil {
		return err
	}
	if err := s.repo.SaveJob(*job); err != nil {
		return err
	}
	return nil
}

func (s *Service) failRender(job domain.RenderJob, cause error) (domain.RenderJob, bool, error) {
	appErr := domain.AsAppError(cause)
	detail := &domain.ErrorDetail{
		Code:    appErr.Detail.Code,
		Message: appErr.Detail.Message,
		Field:   appErr.Detail.Field,
	}
	transitionErr := job.Transition(domain.RenderFailed, detail, time.Now().UTC())
	if transitionErr != nil {
		return job, true, transitionErr
	}
	job.Output = nil
	job.ReceiptDigest = ""
	if saveErr := s.repo.SaveJob(job); saveErr != nil {
		return job, true, saveErr
	}
	return job, true, appErr
}
