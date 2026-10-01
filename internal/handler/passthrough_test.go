package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/git-pkgs/cooldown"
	"github.com/git-pkgs/registries/fetch"
)

func newPassthroughProxy(t *testing.T, body string) (*Proxy, *mockStorage, *mockFetcher) {
	t.Helper()
	proxy, _, store, fetcher := setupTestProxy(t)
	proxy.Passthrough = true
	fetcher.artifact = &fetch.Artifact{
		Body:        io.NopCloser(strings.NewReader(body)),
		Size:        int64(len(body)),
		ContentType: "application/gzip",
	}
	return proxy, store, fetcher
}

func TestPassthroughFromURLStreamsWithoutStoring(t *testing.T) {
	proxy, store, fetcher := newPassthroughProxy(t, "fetched content")

	result, err := proxy.GetOrFetchArtifactFromURL(context.Background(), "pypi", "newpkg", "1.0.0",
		"newpkg-1.0.0.tar.gz", "https://pypi.org/files/newpkg-1.0.0.tar.gz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = result.Reader.Close() }()

	body, err := io.ReadAll(result.Reader)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if string(body) != "fetched content" {
		t.Errorf("body = %q, want %q", body, "fetched content")
	}
	if fetcher.fetchedURL != "https://pypi.org/files/newpkg-1.0.0.tar.gz" {
		t.Errorf("fetched URL = %q", fetcher.fetchedURL)
	}
	if result.Cached {
		t.Error("passthrough result reported as cached")
	}
	if result.Artifact.Size != int64(len("fetched content")) {
		t.Errorf("Size = %d, want the upstream size", result.Artifact.Size)
	}
	if result.Artifact.MediaType != "application/gzip" {
		t.Errorf("MediaType = %q", result.Artifact.MediaType)
	}
	if len(store.files) != 0 {
		t.Errorf("passthrough stored %d files, want none", len(store.files))
	}
	cached, err := proxy.DB.GetCachedArtifact("pkg:pypi/newpkg", "pkg:pypi/newpkg@1.0.0", "newpkg-1.0.0.tar.gz")
	if err != nil {
		t.Fatalf("GetCachedArtifact: %v", err)
	}
	if cached != nil {
		t.Error("passthrough recorded the artifact in the cache database")
	}
}

func TestPassthroughGetOrFetchArtifactStreamsWithoutStoring(t *testing.T) {
	proxy, store, fetcher := newPassthroughProxy(t, "tarball data")

	result, err := proxy.GetOrFetchArtifact(context.Background(), "npm", "leftpad", testVersion100, "leftpad-1.0.0.tgz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = result.Reader.Close() }()

	body, _ := io.ReadAll(result.Reader)
	if string(body) != "tarball data" {
		t.Errorf("body = %q", body)
	}
	if !fetcher.fetchCalled {
		t.Error("upstream was not fetched")
	}
	if len(store.files) != 0 {
		t.Errorf("passthrough stored %d files, want none", len(store.files))
	}
}

func TestPassthroughIgnoresCachedArtifacts(t *testing.T) {
	proxy, _, store, fetcher := setupTestProxy(t)
	fetcher.artifact = &fetch.Artifact{Body: io.NopCloser(strings.NewReader("old bytes"))}
	url := "https://pypi.org/files/newpkg-1.0.0.tar.gz"

	// Populate the cache in normal mode first.
	result, err := proxy.GetOrFetchArtifactFromURL(context.Background(), "pypi", "newpkg", "1.0.0", "newpkg-1.0.0.tar.gz", url)
	if err != nil {
		t.Fatalf("priming cache: %v", err)
	}
	_ = result.Reader.Close()
	if len(store.files) != 1 {
		t.Fatalf("expected the cache to hold the artifact, got %d files", len(store.files))
	}

	proxy.Passthrough = true
	cached, err := proxy.GetCachedArtifact(context.Background(), "pypi", "newpkg", "1.0.0", "newpkg-1.0.0.tar.gz")
	if err != nil || cached != nil {
		t.Fatalf("GetCachedArtifact = %v, %v; want nil, nil in passthrough", cached, err)
	}

	fetcher.fetchCalled = false
	fetcher.artifact = &fetch.Artifact{Body: io.NopCloser(strings.NewReader("new bytes"))}
	result, err = proxy.GetOrFetchArtifactFromURL(context.Background(), "pypi", "newpkg", "1.0.0", "newpkg-1.0.0.tar.gz", url)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = result.Reader.Close() }()
	body, _ := io.ReadAll(result.Reader)
	if !fetcher.fetchCalled || string(body) != "new bytes" {
		t.Errorf("passthrough served %q (fetched=%v), want a fresh upstream fetch", body, fetcher.fetchCalled)
	}
}

func TestPassthroughVerifiesUpstreamDigest(t *testing.T) {
	const content = "blob bytes"

	t.Run("matching digest streams the body", func(t *testing.T) {
		proxy, _, _ := newPassthroughProxy(t, content)
		result, err := proxy.GetOrFetchArtifactFromURLWithDigest(context.Background(), "oci", "library/app", "v1",
			"blob", "https://registry.test/blob", "sha256:"+sha256Hex(content))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer func() { _ = result.Reader.Close() }()

		body, err := io.ReadAll(result.Reader)
		if err != nil {
			t.Fatalf("reading verified body: %v", err)
		}
		if string(body) != content {
			t.Errorf("body = %q", body)
		}
		if got := result.Artifact.Digest.Encoded(); got != sha256Hex(content) {
			t.Errorf("Digest = %q, want the upstream digest", got)
		}
		if result.Artifact.Size >= 0 {
			t.Errorf("Size = %d, want unknown so the response is chunked", result.Artifact.Size)
		}
	})

	t.Run("mismatched digest fails the read", func(t *testing.T) {
		proxy, _, _ := newPassthroughProxy(t, content)
		result, err := proxy.GetOrFetchArtifactFromURLWithDigest(context.Background(), "oci", "library/app", "v1",
			"blob", "https://registry.test/blob", "sha256:"+sha256Hex("other bytes"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer func() { _ = result.Reader.Close() }()

		if _, err := io.ReadAll(result.Reader); !errors.Is(err, ErrArtifactDigestMismatch) {
			t.Errorf("read error = %v, want ErrArtifactDigestMismatch", err)
		}
	})
}

func TestServeArtifactAbortsOnPassthroughDigestMismatch(t *testing.T) {
	proxy, _, _ := newPassthroughProxy(t, "blob bytes")
	result, err := proxy.GetOrFetchArtifactFromURLWithDigest(context.Background(), "oci", "library/app", "v1",
		"blob", "https://registry.test/blob", "sha256:"+sha256Hex("other bytes"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec := httptest.NewRecorder()
	defer func() {
		if r := recover(); r != http.ErrAbortHandler {
			t.Fatalf("recovered %v, want http.ErrAbortHandler", r)
		}
		if rec.Header().Get(headerContentLength) != "" {
			t.Errorf("Content-Length = %q, want none so the client sees an unterminated response",
				rec.Header().Get(headerContentLength))
		}
	}()
	ServeArtifact(rec, result)
}

func TestNPMDownloadCooldownPassthrough(t *testing.T) {
	now := time.Now()
	packument := `{
		"name": "leftpad",
		"dist-tags": {"latest": "2.0.0"},
		"time": {
			"1.0.0": "` + now.Add(-30*24*time.Hour).Format(time.RFC3339) + `",
			"2.0.0": "` + now.Add(-1*time.Hour).Format(time.RFC3339) + `"
		},
		"versions": {"1.0.0": {}, "2.0.0": {}}
	}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentTypeJSON)
		_, _ = io.WriteString(w, packument)
	}))
	defer upstream.Close()

	tests := []struct {
		name       string
		version    string
		wantStatus int
	}{
		{"published before the window streams the tarball", testVersion100, http.StatusOK},
		{"published inside the window is withheld", "2.0.0", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxy, store, fetcher := newPassthroughProxy(t, "tarball data")
			proxy.HTTPClient = upstream.Client()
			proxy.Cooldown = &cooldown.Config{Default: "7d"}

			srv := httptest.NewServer(NewNPMHandler(proxy, "http://proxy.test", upstream.URL).Routes())
			defer srv.Close()

			resp, err := http.Get(srv.URL + "/leftpad/-/leftpad-" + tt.version + ".tgz")
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusOK && string(body) != "tarball data" {
				t.Errorf("body = %q", body)
			}
			if tt.wantStatus == http.StatusNotFound && fetcher.fetchCalled {
				t.Error("fetched a version that is still inside the cooldown window")
			}
			if len(store.files) != 0 {
				t.Errorf("passthrough stored %d files, want none", len(store.files))
			}
		})
	}
}
