package routes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"

	"pocketbase/pluginsystem"
)

func TestPluginSyncLimiterCooldownPerInstance(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	limiter := newPluginSyncLimiter(15*time.Minute, 5)
	limiter.now = func() time.Time { return now }

	release, _, err := limiter.startManual("komoot")
	if err != nil {
		t.Fatal(err)
	}
	release()
	// Switching on a second plugin right after the first must still give it
	// its first sync.
	if release, _, err := limiter.startManual("strava"); err != nil {
		t.Fatalf("another instance blocked by the first one's cooldown: %v", err)
	} else {
		release()
	}
	now = now.Add(10 * time.Minute)
	_, retryAfter, err := limiter.startManual("komoot")
	if !errors.Is(err, errPluginSyncCooldown) || retryAfter != 5*time.Minute {
		t.Fatalf("second start within cooldown: retryAfter=%s, err=%v", retryAfter, err)
	}
	now = now.Add(5 * time.Minute)
	if _, _, err := limiter.startManual("komoot"); err != nil {
		t.Fatalf("start after cooldown: %v", err)
	}
}

func TestPluginSyncLimiterCapsRunningSyncs(t *testing.T) {
	limiter := newPluginSyncLimiter(0, 2)
	first, _, err := limiter.startManual("a1")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := limiter.startManual("b1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := limiter.startManual("c1"); !errors.Is(err, errPluginSyncBusy) {
		t.Fatalf("third running sync: %v", err)
	}
	first()
	if _, _, err := limiter.startManual("c1"); err != nil {
		t.Fatalf("start after a slot freed: %v", err)
	}
}

func TestManualSyncMaxRunningLeavesAWorkerFree(t *testing.T) {
	for _, c := range []struct{ configured, workers, want int }{
		{2, 2, 1}, {2, 8, 2}, {2, 1, 0}, {5, 4, 3}, {0, 8, 0},
	} {
		if got := manualSyncMaxRunning(c.configured, c.workers); got != c.want {
			t.Errorf("manualSyncMaxRunning(%d, %d) = %d, want %d", c.configured, c.workers, got, c.want)
		}
	}
}

func TestPluginSyncEnvFallsBackOnInvalidValues(t *testing.T) {
	for _, raw := range []string{"soon", "-5m", "0s"} {
		t.Setenv("TEST_PLUGIN_SYNC_DURATION", raw)
		if got := envDuration("TEST_PLUGIN_SYNC_DURATION", time.Minute, time.Second); got != time.Minute {
			t.Errorf("duration %q below its minimum: got %s, want the fallback", raw, got)
		}
	}
	t.Setenv("TEST_PLUGIN_SYNC_DURATION", "0s")
	if got := envDuration("TEST_PLUGIN_SYNC_DURATION", time.Minute, 0); got != 0 {
		t.Errorf("zero cooldown: %s", got)
	}
	t.Setenv("TEST_PLUGIN_SYNC_DURATION", "1h")
	if got := envDuration("TEST_PLUGIN_SYNC_DURATION", time.Minute, time.Second); got != time.Hour {
		t.Errorf("valid duration: %s", got)
	}
	t.Setenv("TEST_PLUGIN_SYNC_INT", "-1")
	if got := envInt("TEST_PLUGIN_SYNC_INT", 2); got != 2 {
		t.Errorf("negative int: %d", got)
	}
	t.Setenv("TEST_PLUGIN_SYNC_INT", "0")
	if got := envInt("TEST_PLUGIN_SYNC_INT", 2); got != 0 {
		t.Errorf("valid int: %d", got)
	}
}

type pluginSyncRouteFixture struct {
	*pluginInstanceFixture
	handler http.Handler
	owner   string
	other   string
}

func newPluginSyncRouteFixture(t *testing.T) *pluginSyncRouteFixture {
	t.Helper()
	f := &pluginSyncRouteFixture{pluginInstanceFixture: newPluginInstanceFixture(t)}
	users := core.NewAuthCollection("plugin_sync_users")
	if err := f.app.Save(users); err != nil {
		t.Fatal(err)
	}
	tokens := make([]string, 2)
	for i, email := range []string{"owner@example.com", "other@example.com"} {
		user := core.NewRecord(users)
		user.SetEmail(email)
		user.SetPassword("test-plugin-sync-password")
		if err := f.app.Save(user); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			f.ownerID = user.Id
		}
		token, err := user.NewAuthToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens[i] = token
	}
	f.owner, f.other = tokens[0], tokens[1]

	router, err := apis.NewRouter(f.app)
	if err != nil {
		t.Fatal(err)
	}
	router.POST("/api/plugins/sync", PluginSystemSync(nil))
	if f.handler, err = router.BuildMux(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *pluginSyncRouteFixture) sync(t *testing.T, token string, instanceID string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(pluginSyncRequest{InstanceID: instanceID})
	req := httptest.NewRequest(http.MethodPost, "/api/plugins/sync", bytes.NewReader(body))
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, req)
	return response
}

func usePluginSyncLimiter(t *testing.T, limiter *pluginSyncLimiter) {
	t.Helper()
	original := pluginSyncs
	pluginSyncs = limiter
	t.Cleanup(func() { pluginSyncs = original })
}

func retryAfterFromBody(t *testing.T, response *httptest.ResponseRecorder) int {
	t.Helper()
	var body struct {
		Data struct {
			RetryAfter int `json:"retryAfter"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("429 body: %v: %s", err, response.Body.String())
	}
	return body.Data.RetryAfter
}

func TestPluginSystemSyncRejectsBeforeStartingASync(t *testing.T) {
	f := newPluginSyncRouteFixture(t)
	enabled := f.instance(t, true, time.Time{})
	disabled := f.instance(t, false, time.Time{})
	backoff := f.instance(t, true, time.Now().Add(30*time.Minute))

	cases := []struct {
		name     string
		token    string
		instance string
		status   int
	}{
		{"anonymous", "", enabled, http.StatusUnauthorized},
		{"missing instance id", f.owner, "", http.StatusBadRequest},
		{"another user's instance", f.other, enabled, http.StatusNotFound},
		{"disabled instance", f.owner, disabled, http.StatusBadRequest},
		{"provider backoff", f.owner, backoff, http.StatusTooManyRequests},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			limiter := newPluginSyncLimiter(time.Hour, 2)
			usePluginSyncLimiter(t, limiter)
			response := f.sync(t, c.token, c.instance)
			if response.Code != c.status {
				t.Fatalf("status %d, want %d: %s", response.Code, c.status, response.Body.String())
			}
			if len(limiter.lastStart) != 0 || limiter.running != 0 {
				t.Fatal("a rejected request counted toward the limits")
			}
		})
	}
}

func TestPluginSystemSyncLimiterAnswers(t *testing.T) {
	f := newPluginSyncRouteFixture(t)
	instanceID := f.instance(t, true, time.Time{})
	f.installPlugin(t)

	t.Run("cooldown", func(t *testing.T) {
		limiter := newPluginSyncLimiter(15*time.Minute, 2)
		usePluginSyncLimiter(t, limiter)
		release, _, err := limiter.startManual(instanceID)
		if err != nil {
			t.Fatal(err)
		}
		release()
		response := f.sync(t, f.owner, instanceID)
		if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "900" || retryAfterFromBody(t, response) != 900 {
			t.Fatalf("status %d, Retry-After %q: %s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
		}
	})
	t.Run("all slots busy", func(t *testing.T) {
		limiter := newPluginSyncLimiter(0, 1)
		usePluginSyncLimiter(t, limiter)
		if _, _, err := limiter.startManual("someone-else"); err != nil {
			t.Fatal(err)
		}
		response := f.sync(t, f.owner, instanceID)
		if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "60" || retryAfterFromBody(t, response) != 60 {
			t.Fatalf("status %d, Retry-After %q: %s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
		}
	})
	t.Run("turned off", func(t *testing.T) {
		usePluginSyncLimiter(t, newPluginSyncLimiter(0, 0))
		if response := f.sync(t, f.owner, instanceID); response.Code != http.StatusForbidden {
			t.Fatalf("status %d: %s", response.Code, response.Body.String())
		}
	})
	t.Run("already syncing", func(t *testing.T) {
		limiter := newPluginSyncLimiter(0, 2)
		usePluginSyncLimiter(t, limiter)
		limiter.lockInstance(instanceID)
		if response := f.sync(t, f.owner, instanceID); response.Code != http.StatusConflict {
			t.Fatalf("status %d: %s", response.Code, response.Body.String())
		}
	})
}

func TestPluginSystemSyncRunsInBackgroundAndReleases(t *testing.T) {
	f := newPluginSyncRouteFixture(t)
	instanceID := f.instance(t, true, time.Time{})
	f.installPlugin(t)
	limiter := newPluginSyncLimiter(0, 2)
	usePluginSyncLimiter(t, limiter)
	called := make(chan bool, 1)
	usePluginInstanceSync(t, func(ctx context.Context, _ core.App, _ meilisearch.ServiceManager, _ pluginsystem.LocalPlugin, instance *core.Record) (*pluginSystemSyncResult, error) {
		_, hasDeadline := ctx.Deadline()
		called <- hasDeadline && instance.Id == instanceID
		return &pluginSystemSyncResult{PluginID: "komoot"}, nil
	})

	if response := f.sync(t, f.owner, instanceID); response.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	select {
	case ok := <-called:
		if !ok {
			t.Fatal("background sync ran without a deadline or on the wrong instance")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background sync never ran")
	}
	for deadline := time.Now().Add(5 * time.Second); !limiter.lockInstance(instanceID); {
		if time.Now().After(deadline) {
			t.Fatal("instance stayed locked after the background sync")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCronSkipsInstanceAManualSyncIsRunning(t *testing.T) {
	f := newPluginSyncRouteFixture(t)
	busy := f.instance(t, true, time.Time{})
	free := f.instance(t, true, time.Time{})
	limiter := newPluginSyncLimiter(0, 2)
	usePluginSyncLimiter(t, limiter)
	release, _, err := limiter.startManual(busy)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var synced []string
	usePluginInstanceSync(t, func(_ context.Context, _ core.App, _ meilisearch.ServiceManager, _ pluginsystem.LocalPlugin, instance *core.Record) (*pluginSystemSyncResult, error) {
		synced = append(synced, instance.Id)
		return &pluginSystemSyncResult{PluginID: "komoot"}, nil
	})

	if err := syncPluginInstancesOneByOne(t.Context(), f.app, nil, pluginsystem.LocalPlugin{}, []*core.Record{f.stored(t, busy), f.stored(t, free)}); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 1 || synced[0] != free {
		t.Fatalf("cron synced %v; want only the free instance", synced)
	}
	if !limiter.lockInstance(free) {
		t.Fatal("cron left the instance it synced locked")
	}
}

func TestSyncTimeoutIsNotReportedAsProviderOutage(t *testing.T) {
	f := newPluginSyncRouteFixture(t)
	instanceID := f.instance(t, true, time.Time{})
	ctx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	setPluginInstanceStatusAfterSyncError(ctx, f.app, f.stored(t, instanceID), pluginsystem.RuntimeSessionFatalError{Err: ctx.Err()})
	if code := pluginsystem.JSONMapFromRecord(f.stored(t, instanceID), "last_error")["code"]; code != "sync_timeout" {
		t.Fatalf("last_error code = %v, want sync_timeout", code)
	}
}

// installPlugin registers the shipped Komoot manifest, so a request gets past
// the plugin checks to the limiter.
func (f *pluginSyncRouteFixture) installPlugin(t *testing.T) {
	t.Helper()
	manifestData, err := os.ReadFile(filepath.Join("..", "..", "plugins", "komoot", "plugin.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest pluginsystem.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	installed := core.NewBaseCollection("installed_plugins")
	installed.Fields.Add(&core.TextField{Name: "plugin_id"}, &core.TextField{Name: "path"},
		&core.JSONField{Name: "manifest"}, &core.JSONField{Name: "config"})
	if err := f.app.Save(installed); err != nil {
		t.Fatal(err)
	}
	pluginDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(pluginDir, manifest.Runtime.Entrypoint), []byte("\x00asm\x01\x00\x00\x00"), 0600); err != nil {
		t.Fatal(err)
	}
	record := core.NewRecord(installed)
	record.Set("plugin_id", manifest.ID)
	record.Set("path", pluginDir)
	record.Set("manifest", manifest)
	if err := f.app.Save(record); err != nil {
		t.Fatal(err)
	}
}
