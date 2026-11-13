package service

import (
	"context"
	"errors"
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
		return existing, false, nil
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
	result, renderErr := render.Render(
		ctx,
		id,
		plan,
		sources,
		render.Limits{MaxOutputFrames: s.config.MaxOutputFrames},
		time.Now().UTC(),
	)
	if renderErr != nil {
		if errors.Is(renderErr, context.Canceled) || errors.Is(renderErr, context.DeadlineExceeded) {
			return s.checkpointInterruptedRender(job, renderErr)
		}
		return s.failRender(job, domain.Wrap(domain.KindInternal, "render_failed", "", "timeline rendering failed", renderErr))
	}
	if err := s.advanceJob(&job, domain.RenderFinalizing, nil); err != nil {
		return job, true, err
	}
	if err := s.repo.CommitRenderArtifacts(id, result.WAV, result.Receipt); err != nil {
		return s.failRender(job, err)
	}
	if err := ctx.Err(); err != nil {
		return s.failRender(job, domain.Wrap(domain.KindInternal, "render_cancelled", "render_id", "render response was cancelled", err))
	}
	job.Output = &result.Output
	job.ReceiptDigest = result.Output.Digest
	if err := s.advanceJob(&job, domain.RenderCompleted, nil); err != nil {
		return job, true, err
	}
	return job, true, nil
}

func (s *Service) checkpointInterruptedRender(job domain.RenderJob, cause error) (domain.RenderJob, bool, error) {
	current, err := s.repo.GetJob(job.ID)
	if err != nil {
		return job, true, err
	}
	if current.TimelineID != job.TimelineID {
		return job, true, domain.E(domain.KindConflict, "render_conflict", "render_id", "interrupted render belongs to another timeline")
	}
	if current.State == domain.RenderCompleted {
		return current, false, nil
	}
	current.Error = &domain.ErrorDetail{
		Code:    "render_interrupted",
		Message: "render request interrupted before response delivery",
		Field:   "render_id",
	}
	current.UpdatedAt = time.Now().UTC()
	if err := s.repo.SaveJob(current); err != nil {
		return current, true, err
	}
	return current, true, domain.Wrap(domain.KindInternal, "render_interrupted", "render_id", "render progress was checkpointed", cause)
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
