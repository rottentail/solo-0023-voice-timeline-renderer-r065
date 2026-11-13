package storage

import (
	"errors"
	"os"
	"path/filepath"

	"voice-timeline-renderer/internal/domain"
)

func (repo *Repository) SaveJob(job domain.RenderJob) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	path := repo.path("jobs", job.ID+".json")
	if err := writeJSONAtomic(path, job, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render job", err)
	}
	return nil
}

func (repo *Repository) GetJob(id string) (domain.RenderJob, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	var job domain.RenderJob
	if err := readJSON(repo.path("jobs", id+".json"), &job); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.RenderJob{}, domain.E(domain.KindNotFound, "render_not_found", "render_id", "render job does not exist")
		}
		return domain.RenderJob{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render job", err)
	}
	return job, nil
}

func (repo *Repository) ListJobs() ([]domain.RenderJob, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	entries, err := os.ReadDir(repo.path("jobs"))
	if err != nil {
		return nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot list render jobs", err)
	}
	jobs := make([]domain.RenderJob, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		var job domain.RenderJob
		if err := readJSON(repo.path("jobs", entry.Name()), &job); err != nil {
			return nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render job "+entry.Name(), err)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// CommitRenderArtifacts publishes the output and receipt as one indivisible
// unit: both files are staged in a temporary directory and the folder is
// moved into place with a single rename, so a render folder either appears
// complete or not at all.
func (repo *Repository) CommitRenderArtifacts(id string, wav []byte, receipt domain.RenderReceipt) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if receipt.RenderID != id {
		return domain.E(domain.KindConflict, "render_conflict", "render_id", "receipt does not belong to render")
	}
	final := repo.path("renders", id)
	if _, err := os.Stat(final); err == nil {
		return domain.E(domain.KindConflict, "render_conflict", "render_id", "render artifacts already exist")
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render destination", err)
	}
	temp, err := os.MkdirTemp(repo.path("tmp"), "render-*")
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create render staging directory", err)
	}
	defer func() {
		_ = os.RemoveAll(temp)
	}()
	if err := writeBytes(filepath.Join(temp, "output.wav"), wav, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render output", err)
	}
	if err := writeJSONAtomic(filepath.Join(temp, "receipt.json"), receipt, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render receipt", err)
	}
	if err := syncDirectory(temp); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync render staging directory", err)
	}
	if err := os.Rename(temp, final); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot commit render artifacts", err)
	}
	if err := syncDirectory(repo.path("renders")); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync render index", err)
	}
	return nil
}

func (repo *Repository) GetRenderReceipt(id string) (domain.RenderReceipt, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	var receipt domain.RenderReceipt
	if err := readJSON(repo.path("renders", id, "receipt.json"), &receipt); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.RenderReceipt{}, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render receipt is not available")
		}
		return domain.RenderReceipt{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render receipt", err)
	}
	return receipt, nil
}

func (repo *Repository) RenderOutputSize(id string) (int64, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	info, err := os.Stat(repo.path("renders", id, "output.wav"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render output is not available")
		}
		return 0, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render output", err)
	}
	return info.Size(), nil
}

// RemoveRenderArtifacts deletes a render folder that was never committed
// completely, so partial output cannot be mistaken for a finished render.
func (repo *Repository) RemoveRenderArtifacts(id string) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := os.RemoveAll(repo.path("renders", id)); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot remove incomplete render artifacts", err)
	}
	if err := syncDirectory(repo.path("renders")); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync render index", err)
	}
	return nil
}

func (repo *Repository) GetRenderOutput(id string) ([]byte, domain.RenderReceipt, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	var receipt domain.RenderReceipt
	if err := readJSON(repo.path("renders", id, "receipt.json"), &receipt); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, domain.RenderReceipt{}, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render output is not available")
		}
		return nil, domain.RenderReceipt{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render receipt", err)
	}
	payload, err := os.ReadFile(repo.path("renders", id, "output.wav"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, domain.RenderReceipt{}, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render output is not available")
		}
		return nil, domain.RenderReceipt{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render output", err)
	}
	return payload, receipt, nil
}
