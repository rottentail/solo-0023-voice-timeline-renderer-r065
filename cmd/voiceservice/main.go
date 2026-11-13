package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/httpapi"
	"voice-timeline-renderer/internal/service"
	"voice-timeline-renderer/internal/storage"
)

func main() {
	address := flag.String("addr", ":8093", "HTTP listen address")
	dataDir := flag.String("data", "./var/voice-timeline", "persistent data directory")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o750); err != nil {
		slog.Error("cannot create data directory", "error", err)
		os.Exit(1)
	}
	repo, err := storage.NewRepository(*dataDir)
	if err != nil {
		slog.Error("cannot initialize repository", "error", err)
		os.Exit(1)
	}
	if err := reconcilePublishedRenders(*dataDir); err != nil {
		slog.Error("cannot reconcile render checkpoints", "error", err)
		os.Exit(1)
	}
	serviceLayer := service.New(repo, service.DefaultConfig())
	api := httpapi.New(serviceLayer, httpapi.Options{
		MaxSourceRequestBytes: 48 << 20,
		MaxJSONBytes:          2 << 20,
	})
	httpServer := &http.Server{
		Addr:              *address,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      180 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	runContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-runContext.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownContext)
	}()

	slog.Info("voice timeline service listening", "addr", *address, "data", *dataDir)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("HTTP service stopped", "error", err)
		os.Exit(1)
	}
}

func reconcilePublishedRenders(root string) error {
	entries, err := os.ReadDir(filepath.Join(root, "jobs"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(root, "jobs", entry.Name())
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var job domain.RenderJob
		if err := json.Unmarshal(payload, &job); err != nil {
			return err
		}
		if job.State == domain.RenderCompleted || job.State == domain.RenderQueued {
			continue
		}
		info, err := os.Stat(filepath.Join(root, "renders", job.ID, "output.wav"))
		if err != nil || info.Size() < 44 {
			continue
		}
		job.State = domain.RenderCompleted
		job.Error = nil
		job.UpdatedAt = time.Now().UTC()
		payload, err = json.MarshalIndent(job, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, append(payload, '\n'), 0o640); err != nil {
			return err
		}
	}
	return nil
}
