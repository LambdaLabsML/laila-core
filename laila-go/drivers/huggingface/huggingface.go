// Package huggingface is the ``huggingface`` extra (``pip install
// laila-core[huggingface]``): it binds laila.HuggingFacePool to the Hub
// HTTP API (the same endpoints ``huggingface_hub`` drives) using only the
// standard library. Import it for its side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/huggingface"
package huggingface

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Endpoint is ``HF_ENDPOINT`` (``https://huggingface.co`` by default).
var Endpoint = func() string {
	if v := os.Getenv("HF_ENDPOINT"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "https://huggingface.co"
}()

// HfHubHTTPError is ``huggingface_hub.utils.HfHubHTTPError``.
var HfHubHTTPError = py.NewType("HfHubHTTPError", py.Exception)

// RepositoryNotFoundError / RevisionNotFoundError are the Hub's other
// 404 flavours.
var (
	RepositoryNotFoundError = py.NewType("RepositoryNotFoundError", HfHubHTTPError)
	RevisionNotFoundError   = py.NewType("RevisionNotFoundError", HfHubHTTPError)
)

// Driver implements laila.Hf_driver.
type Driver struct {
	HTTP *http.Client
}

var defaultDriver = Driver{HTTP: &http.Client{Timeout: 120 * time.Second}}

// HfApi is ``HfApi(token=token)``.
func (d Driver) HfApi(token *string) laila.Hf_api { return &api{d: d, token: token} }

// Hf_hub_download fetches ``<endpoint>/<prefix><repo_id>/resolve/<revision>/<filename>``
// into a content-addressed cache file and returns its path.
func (d Driver) Hf_hub_download(repo_id string, filename string, repo_type string, revision string, token *string) (string, error) {
	u := Endpoint + "/" + repoPrefix(repo_type) + repo_id + "/resolve/" + url.PathEscape(revision) + "/" + escapePath(filename)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return "", py.FromGo(err)
	}
	auth(req, token)
	resp, err := d.client().Do(req)
	if err != nil {
		return "", py.ConnectionError.New(err.Error()).From(err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return "", err
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", py.FromGo(err)
	}
	cache := cacheDir()
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return "", py.FromGo(err)
	}
	sum := sha256.Sum256(append([]byte(u+"\x00"), data...))
	path := filepath.Join(cache, hex.EncodeToString(sum[:]))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", py.FromGo(err)
	}
	return path, nil
}

func (d Driver) client() *http.Client {
	if d.HTTP != nil {
		return d.HTTP
	}
	return defaultDriver.HTTP
}

func cacheDir() string {
	if v := os.Getenv("HF_HUB_CACHE"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "laila_hf_cache")
	}
	return filepath.Join(home, ".cache", "huggingface", "hub", "laila")
}

func repoPrefix(repo_type string) string {
	switch repo_type {
	case "dataset", "datasets":
		return "datasets/"
	case "space", "spaces":
		return "spaces/"
	}
	return ""
}

func apiPrefix(repo_type string) string {
	switch repo_type {
	case "dataset", "datasets":
		return "datasets"
	case "space", "spaces":
		return "spaces"
	}
	return "models"
}

func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

func auth(req *http.Request, token *string) {
	if token != nil && *token != "" {
		req.Header.Set("Authorization", "Bearer "+*token)
	}
	req.Header.Set("User-Agent", "laila-go/huggingface")
}

// checkStatus maps Hub error responses onto the ``huggingface_hub``
// exception hierarchy (``X-Error-Code`` first, status code second).
func checkStatus(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	msg := fmt.Sprintf("%s for url %s: %s", resp.Status, resp.Request.URL, strings.TrimSpace(string(body)))
	switch resp.Header.Get("X-Error-Code") {
	case "EntryNotFound":
		return laila.EntryNotFoundError.New(msg)
	case "RepoNotFound":
		return RepositoryNotFoundError.New(msg)
	case "RevisionNotFound":
		return RevisionNotFoundError.New(msg)
	}
	if resp.StatusCode == 404 {
		return laila.EntryNotFoundError.New(msg)
	}
	return HfHubHTTPError.New(msg).With("status_code", resp.StatusCode)
}

type api struct {
	d     Driver
	token *string
}

type commitOp struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// commit posts one ndjson commit (``/api/<type>/<repo>/commit/<revision>``).
func (a *api) commit(repo_id string, repo_type string, revision string, message string, ops []commitOp) error {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	_ = enc.Encode(commitOp{Key: "header", Value: map[string]any{"summary": message, "description": ""}})
	for _, op := range ops {
		_ = enc.Encode(op)
	}
	u := Endpoint + "/api/" + apiPrefix(repo_type) + "/" + repo_id + "/commit/" + url.PathEscape(revision)
	req, err := http.NewRequest(http.MethodPost, u, &body)
	if err != nil {
		return py.FromGo(err)
	}
	auth(req, a.token)
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := a.d.client().Do(req)
	if err != nil {
		return py.ConnectionError.New(err.Error()).From(err)
	}
	defer resp.Body.Close()
	return checkStatus(resp)
}

func (a *api) Upload_file(content []byte, path_in_repo string, repo_id string, repo_type string, revision string, commit_message string) error {
	return a.commit(repo_id, repo_type, revision, commit_message, []commitOp{{
		Key: "file",
		Value: map[string]any{
			"content":  base64.StdEncoding.EncodeToString(content),
			"path":     path_in_repo,
			"encoding": "base64",
		},
	}})
}

func (a *api) Delete_file(path_in_repo string, repo_id string, repo_type string, revision string, commit_message string) error {
	err := a.commit(repo_id, repo_type, revision, commit_message, []commitOp{{
		Key:   "deletedFile",
		Value: map[string]any{"path": path_in_repo},
	}})
	if err != nil {
		var e *py.Error
		if errors.As(err, &e) && e.Type.Is(HfHubHTTPError) {
			low := strings.ToLower(e.Error())
			if strings.Contains(low, "not found") || strings.Contains(low, "does not exist") || strings.Contains(low, "no such file") {
				return laila.EntryNotFoundError.New(e.Error()).From(err)
			}
		}
	}
	return err
}

func (a *api) List_repo_files(repo_id string, repo_type string, revision string, token *string) ([]string, error) {
	u := Endpoint + "/api/" + apiPrefix(repo_type) + "/" + repo_id + "/revision/" + url.PathEscape(revision)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, py.FromGo(err)
	}
	if token == nil {
		token = a.token
	}
	auth(req, token)
	resp, err := a.d.client().Do(req)
	if err != nil {
		return nil, py.ConnectionError.New(err.Error()).From(err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return nil, err
	}
	var info struct {
		Siblings []struct {
			Rfilename string `json:"rfilename"`
		} `json:"siblings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, py.FromGo(err)
	}
	files := make([]string, 0, len(info.Siblings))
	for _, s := range info.Siblings {
		files = append(files, s.Rfilename)
	}
	return files, nil
}

func init() { laila.Register_driver("huggingface_hub", defaultDriver) }
