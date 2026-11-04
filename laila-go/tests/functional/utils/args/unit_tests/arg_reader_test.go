// Port of tests/functional/utils/args/unit_tests/test_arg_reader.py.
package unit_tests

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func newReader(t *testing.T) *laila.ArgReader {
	t.Helper()
	r, err := laila.New_ArgReader(py.Kw{"target": laila.Args})
	if err != nil {
		t.Fatal(err)
	}
	r.Clear()
	t.Cleanup(r.Clear)
	return r
}

func tmpFile(t *testing.T, suffix, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "args"+suffix)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func arg(key string) any { return laila.Args.Get(key) }

func mustLoad(t *testing.T, r *laila.ArgReader, source string, kw ...py.Kw) {
	t.Helper()
	if err := r.Load(source, kw...); err != nil {
		t.Fatalf("load(%s): %v", source, err)
	}
}

func TestArgReader(t *testing.T) {
	t.Run("load_json_flat_and_one_level_nested", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".json", `{"s3_bucket_region":"us-east-1","s3":{"bucket_name":"laila-test-1"}}`))
		if arg("s3_bucket_region") != "us-east-1" || arg("s3_bucket_name") != "laila-test-1" {
			t.Errorf("args = %s", py.Repr(laila.Args))
		}
	})
	t.Run("load_toml", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".toml", "s3_bucket_region = \"us-west-2\"\n[cloudflare]\naccount_id = \"abc123\"\n"))
		if arg("s3_bucket_region") != "us-west-2" || arg("cloudflare_account_id") != "abc123" {
			t.Errorf("args = %s", py.Repr(laila.Args))
		}
	})
	t.Run("load_env", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "S3_BUCKET_REGION=eu-central-1\nRETRIES=3\nUSE_SSL=true\n"))
		if arg("S3_BUCKET_REGION") != "eu-central-1" || !py.Eq(arg("RETRIES"), 3) || arg("USE_SSL") != true {
			t.Errorf("args = %s", py.Repr(laila.Args))
		}
	})
	t.Run("load_xml", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".xml", "<args><s3_bucket_region>ap-south-1</s3_bucket_region><s3><bucket_name>laila-test-2</bucket_name></s3></args>"))
		if arg("s3_bucket_region") != "ap-south-1" || arg("s3_bucket_name") != "laila-test-2" {
			t.Errorf("args = %s", py.Repr(laila.Args))
		}
	})
	t.Run("load_terminal", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, "terminal", py.Kw{"terminal_args": []string{"s3_bucket_region=us-east-2", "max_retries=5", "debug=false"}})
		if arg("s3_bucket_region") != "us-east-2" || !py.Eq(arg("max_retries"), 5) || arg("debug") != false {
			t.Errorf("args = %s", py.Repr(laila.Args))
		}
	})
	t.Run("read_args_returns_none", func(t *testing.T) {
		newReader(t)
		if err := laila.Read_args(tmpFile(t, ".json", `{"foo":"bar"}`)); err != nil {
			t.Fatal(err)
		}
		if arg("foo") != "bar" {
			t.Error("foo")
		}
	})

	// ── edge cases: file not found / malformed ──────────────
	t.Run("load_missing_file_raises", func(t *testing.T) {
		if err := newReader(t).Load("/nonexistent/path/file.json"); !errors.Is(err, py.FileNotFoundError) {
			t.Errorf("want FileNotFoundError, got %v", err)
		}
	})
	t.Run("load_empty_json_raises", func(t *testing.T) {
		if err := newReader(t).Load(tmpFile(t, ".json", "")); err == nil {
			t.Error("expected an error")
		}
	})
	t.Run("load_malformed_json_raises", func(t *testing.T) {
		if err := newReader(t).Load(tmpFile(t, ".json", "{bad json")); err == nil {
			t.Error("expected an error")
		}
	})
	t.Run("load_json_array_raises", func(t *testing.T) {
		if err := newReader(t).Load(tmpFile(t, ".json", "[1, 2, 3]")); !errors.Is(err, py.ValueError) {
			t.Errorf("want ValueError, got %v", err)
		}
	})
	t.Run("load_malformed_toml_raises", func(t *testing.T) {
		if err := newReader(t).Load(tmpFile(t, ".toml", "[[[invalid")); err == nil {
			t.Error("expected an error")
		}
	})
	t.Run("unsupported_suffix_raises", func(t *testing.T) {
		if err := newReader(t).Load(tmpFile(t, ".yaml", "key: value")); !errors.Is(err, py.ValueError) {
			t.Errorf("want ValueError, got %v", err)
		}
	})
	t.Run("load_no_suffix_raises", func(t *testing.T) {
		if err := newReader(t).Load(tmpFile(t, "", "")); !errors.Is(err, py.ValueError) {
			t.Errorf("want ValueError, got %v", err)
		}
	})

	// ── coercion ────────────────────────────────────────────
	t.Run("coerce_true_false", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "A=true\nB=false\nC=TRUE\nD=False\n"))
		if arg("A") != true || arg("B") != false || arg("C") != true || arg("D") != false {
			t.Errorf("args = %s", py.Repr(laila.Args))
		}
	})
	t.Run("coerce_none_null", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "A=none\nB=null\nC=None\n"))
		for _, k := range []string{"A", "B", "C"} {
			if !py.IsNone(arg(k)) {
				t.Errorf("%s = %v", k, arg(k))
			}
		}
	})
	t.Run("coerce_int_and_float", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "I=42\nF=3.14\n"))
		if v, ok := arg("I").(int); !ok || v != 42 {
			t.Errorf("I = %#v", arg("I"))
		}
		if v, ok := arg("F").(float64); !ok || v != 3.14 {
			t.Errorf("F = %#v", arg("F"))
		}
	})
	t.Run("coerce_json_object_in_value_gets_flattened", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", `DATA={"a": 1}`+"\n"))
		if !py.Eq(arg("DATA_a"), 1) {
			t.Errorf("DATA_a = %v", arg("DATA_a"))
		}
	})
	t.Run("coerce_json_array_in_value", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "DATA=[1,2,3]\n"))
		if !py.Eq(arg("DATA"), py.NewList(1, 2, 3)) {
			t.Errorf("DATA = %s", py.Repr(arg("DATA")))
		}
	})
	t.Run("coerce_quoted_string", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "NAME=\"hello world\"\n"))
		if arg("NAME") != "hello world" {
			t.Errorf("NAME = %v", arg("NAME"))
		}
	})

	// ── env file edge cases ─────────────────────────────────
	t.Run("env_blank_lines_and_comments_skipped", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "\n# comment\n\nKEY=val\n"))
		if arg("KEY") != "val" {
			t.Error("KEY")
		}
	})
	t.Run("env_line_without_equals_skipped", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "no-equals\nKEY=val\n"))
		if arg("KEY") != "val" {
			t.Error("KEY")
		}
	})
	t.Run("env_value_with_equals", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".env", "URL=http://host?a=1&b=2\n"))
		if arg("URL") != "http://host?a=1&b=2" {
			t.Errorf("URL = %v", arg("URL"))
		}
	})

	// ── clear ───────────────────────────────────────────────
	t.Run("clear_removes_loaded_args", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".json", `{"x": 1, "y": 2}`))
		if !py.Eq(arg("x"), 1) {
			t.Error("x")
		}
		r.Clear()
		if v, ok := laila.Args.Lookup("x"); ok && py.Eq(v, 1) {
			t.Error("x survived clear")
		}
	})
	t.Run("clear_on_empty_args_is_safe", func(t *testing.T) {
		newReader(t).Clear()
	})

	// ── multiple loads overwrite ────────────────────────────
	t.Run("second_load_overwrites_first", func(t *testing.T) {
		r := newReader(t)
		mustLoad(t, r, tmpFile(t, ".json", `{"k": "first"}`))
		if arg("k") != "first" {
			t.Error("first")
		}
		mustLoad(t, r, tmpFile(t, ".json", `{"k": "second"}`))
		if arg("k") != "second" {
			t.Error("second")
		}
	})

	// ── terminal edge cases ─────────────────────────────────
	t.Run("terminal_empty_args", func(t *testing.T) {
		mustLoad(t, newReader(t), "terminal", py.Kw{"terminal_args": []string{}})
	})
	t.Run("terminal_no_equals_skipped", func(t *testing.T) {
		mustLoad(t, newReader(t), "terminal", py.Kw{"terminal_args": []string{"noequals", "k=v"}})
		if arg("k") != "v" {
			t.Error("k")
		}
	})
	t.Run("terminal_empty_key_skipped", func(t *testing.T) {
		mustLoad(t, newReader(t), "terminal", py.Kw{"terminal_args": []string{"=value", "k=v"}})
		if arg("k") != "v" {
			t.Error("k")
		}
	})
}
