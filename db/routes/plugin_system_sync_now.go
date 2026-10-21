package routes

import (
	"context"
	"errors"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"

	"pocketbase/pluginsystem"
)

const (
	defaultPluginSyncCooldown   = 15 * time.Minute
	defaultPluginSyncMaxRunning = 2
	// A first sync imported 93 Komoot tours in 3 min 40 s, about one per 2.4 s.
	// A sync cut short continues with the remaining trails on the next run.
	defaultPluginSyncTimeout = 30 * time.Minute
)

var (
	errPluginSyncDisabled = errors.New("manual plugin sync is disabled")
	errPluginSyncRunning  = errors.New("plugin instance is already syncing")
	errPluginSyncBusy     = errors.New("too many plugin syncs are running")
	errPluginSyncCooldown = errors.New("plugin sync was started too recently")
)

// pluginSyncLimiter bounds the syncs users start by hand, and keeps a manual
// sync and the cron from running the same instance at once. It lives in memory
// because users may write every field of their own plugin_instances record,
// last_sync_at and retry_not_before included.
type pluginSyncLimiter struct {
	mu         sync.Mutex
	now        func() time.Time
	cooldown   time.Duration
	maxRunning int
	running    int
	lastStart  map[string]time.Time // by instance id
	busy       map[string]bool      // by instance id
}

func newPluginSyncLimiter(cooldown time.Duration, maxRunning int) *pluginSyncLimiter {
	return &pluginSyncLimiter{
		now:        time.Now,
		cooldown:   cooldown,
		maxRunning: maxRunning,
		lastStart:  map[string]time.Time{},
		busy:       map[string]bool{},
	}
}

var pluginSyncs = newPluginSyncLimiter(
	envDuration("POCKETBASE_PLUGIN_SYNC_COOLDOWN", defaultPluginSyncCooldown, 0),
	manualSyncMaxRunning(envInt("POCKETBASE_PLUGIN_SYNC_MAX_RUNNING", defaultPluginSyncMaxRunning), pluginsystem.MaxWorkers()),
)

var pluginSyncTimeout = envDuration("POCKETBASE_PLUGIN_SYNC_TIMEOUT", defaultPluginSyncTimeout, time.Second)

// manualSyncMaxRunning keeps one plugin worker free of manual syncs. Every
// sync holds a worker for its whole run, and without a free one the cron and
// short requests such as sending a trail would time out waiting.
func manualSyncMaxRunning(configured int, workers int) int {
	return max(0, min(configured, workers-1))
}

// lockInstance marks an instance as syncing. It returns false when a sync of
// that instance is already running.
func (l *pluginSyncLimiter) lockInstance(instanceID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.busy[instanceID] {
		return false
	}
	l.busy[instanceID] = true
	return true
}

func (l *pluginSyncLimiter) unlockInstance(instanceID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.busy, instanceID)
}

// startManual reserves a manual sync of an instance. On success the caller
// must call release once the sync ends. retryAfter is set with
// errPluginSyncCooldown. The cooldown counts per instance, and each user has
// at most one instance per plugin. A start counts toward the cooldown even if
// the sync later fails, so failing syncs cannot be retried in a loop.
func (l *pluginSyncLimiter) startManual(instanceID string) (release func(), retryAfter time.Duration, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.maxRunning <= 0 {
		return nil, 0, errPluginSyncDisabled
	}
	if l.busy[instanceID] {
		return nil, 0, errPluginSyncRunning
	}
	now := l.now()
	if last, ok := l.lastStart[instanceID]; ok && now.Sub(last) < l.cooldown {
		return nil, l.cooldown - now.Sub(last), errPluginSyncCooldown
	}
	if l.running >= l.maxRunning {
		return nil, 0, errPluginSyncBusy
	}
	l.running++
	l.busy[instanceID] = true
	l.lastStart[instanceID] = now
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.running--
		delete(l.busy, instanceID)
	}, 0, nil
}

type pluginSyncRequest struct {
	InstanceID string `json:"instanceId"`
}

// PluginSystemSync starts a sync of one of the user's plugin instances in the
// background and answers 202 without waiting for it.
func PluginSystemSync(client meilisearch.ServiceManager) func(e *core.RequestEvent) error {
	return func(e *core.RequestEvent) error {
		if e.Auth == nil {
			return apis.NewUnauthorizedError("authentication required", nil)
		}
		var data pluginSyncRequest
		if err := e.BindBody(&data); err != nil {
			return apis.NewBadRequestError("failed to read request data", err)
		}
		if data.InstanceID == "" {
			return apis.NewBadRequestError("instanceId is required", nil)
		}
		instance, err := e.App.FindRecordById("plugin_instances", data.InstanceID)
		if err != nil || instance.GetString("user") != e.Auth.Id {
			return apis.NewNotFoundError("plugin instance not found", nil)
		}
		if !instance.GetBool("enabled") {
			return apis.NewBadRequestError("plugin instance is not enabled", nil)
		}
		if shouldSkipPluginInstance(instance) {
			return tooManyPluginSyncs(e, time.Until(instance.GetDateTime("retry_not_before").Time()), "the provider asked to wait before the next sync")
		}
		plugin, err := localPlugin(e.App, instance.GetString("plugin_id"))
		if err != nil {
			return err
		}
		if !pluginHasAnySyncCapability(plugin) {
			return apis.NewBadRequestError("plugin cannot sync", nil)
		}

		release, retryAfter, err := pluginSyncs.startManual(instance.Id)
		switch {
		case errors.Is(err, errPluginSyncDisabled):
			return apis.NewForbiddenError(err.Error(), nil)
		case errors.Is(err, errPluginSyncRunning):
			return apis.NewApiError(http.StatusConflict, err.Error(), nil)
		case errors.Is(err, errPluginSyncCooldown):
			return tooManyPluginSyncs(e, retryAfter, err.Error())
		case errors.Is(err, errPluginSyncBusy):
			return tooManyPluginSyncs(e, time.Minute, err.Error())
		case err != nil:
			return err
		}

		app := e.App
		userID := e.Auth.Id
		started := time.Now()
		go func() {
			defer release()
			ctx, cancel := context.WithTimeout(context.Background(), pluginSyncTimeout)
			defer cancel()
			result, skipped, err := syncFreshPluginInstance(ctx, app, client, plugin, instance.Id)
			elapsed := time.Since(started).Round(time.Second).String()
			switch {
			case skipped || errors.Is(err, errPluginInstanceSwitchedOff):
				app.Logger().Info("plugin instance manual sync skipped", "plugin", plugin.Manifest.ID, "instance", instance.Id, "user", userID)
			case err != nil:
				app.Logger().Warn("plugin instance manual sync failed", "plugin", plugin.Manifest.ID, "instance", instance.Id, "user", userID, "elapsed", elapsed, "error", err)
			default:
				app.Logger().Info("plugin instance manual sync completed", "plugin", result.PluginID, "instance", instance.Id, "user", userID, "elapsed", elapsed, "imported", result.Imported, "skipped", result.Skipped)
			}
		}()

		return e.JSON(http.StatusAccepted, map[string]any{"started": true})
	}
}

func tooManyPluginSyncs(e *core.RequestEvent, retryAfter time.Duration, message string) error {
	seconds := int(math.Ceil(retryAfter.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	e.Response.Header().Set("Retry-After", strconv.Itoa(seconds))
	// Set after construction: the constructor treats data as field validation
	// errors and would replace the number. The web proxy drops headers, so the
	// browser reads the wait from here.
	apiErr := apis.NewTooManyRequestsError(message, nil)
	apiErr.Data = map[string]any{"retryAfter": seconds}
	return apiErr
}

// envDuration reads a duration such as 15m, falling back when the value does
// not parse or is below minimum.
func envDuration(name string, fallback time.Duration, minimum time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < minimum {
		log.Printf("%s must be a duration of at least %s, such as 15m, got %q; using %s", name, minimum, raw, fallback)
		return fallback
	}
	return value
}

func envInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		log.Printf("%s must be a whole number of 0 or more, got %q; using %d", name, raw, fallback)
		return fallback
	}
	return value
}
