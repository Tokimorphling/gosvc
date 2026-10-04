package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/store/object"
)

func testStore(t *testing.T, handler http.HandlerFunc, change func(*config.S3Config), opts ...Option) (*Store, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") && r.URL.Query().Get("X-Amz-Signature") == "" {
			t.Error("unsigned S3 request")
			w.WriteHeader(401)
			return
		}
		if r.Method == "HEAD" && r.URL.Path == "/bucket" {
			w.WriteHeader(200)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	cfg := config.Default().Storage.S3
	cfg.Enabled, cfg.Endpoint, cfg.Bucket, cfg.UsePathStyle = true, server.URL, "bucket", true
	cfg.AccessKeyID, cfg.SecretAccessKey, cfg.Region = "test-key", "test-secret", "us-east-1"
	cfg.MaxAttempts, cfg.PartSizeBytes = 1, 5<<20
	if change != nil {
		change(&cfg)
	}
	store, err := New(t.Context(), cfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, server
}

func TestCRUDMetadataRangeAndPresigning(t *testing.T) {
	var mu sync.Mutex
	var body []byte
	var contentType, author string
	exists := false
	key := "folder/hello +世界.txt"
	s, server := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/"+key {
			t.Errorf("key corrupted: %q", r.URL.Path)
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case "PUT":
			var err error
			body, err = io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			contentType, author, exists = r.Header.Get("Content-Type"), r.Header.Get("X-Amz-Meta-Author"), true
			w.Header().Set("ETag", `"etag"`)
			w.Header().Set("X-Amz-Version-Id", "v1")
		case "GET", "HEAD":
			if !exists {
				w.WriteHeader(404)
				_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
				return
			}
			data := body
			if r.Header.Get("Range") == "bytes=1-3" {
				data = data[1:4]
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 1-3/%d", len(body)))
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Content-Type", contentType)
			w.Header().Set("X-Amz-Meta-Author", author)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", `"etag"`)
			if r.Header.Get("Range") != "" {
				w.WriteHeader(206)
			}
			if r.Method == "GET" {
				_, _ = w.Write(data)
			}
		case "DELETE":
			exists = false
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(400)
		}
	}, nil)
	opts := object.PutOptions{ContentType: "text/plain", Metadata: map[string]string{"author": "sample"}}
	out, err := s.Put(t.Context(), key, strings.NewReader("hello-world"), opts)
	if err != nil || out.Size != 11 || out.ETag != `"etag"` || out.VersionID != "v1" {
		t.Fatalf("Put=%+v %v", out, err)
	}
	info, err := s.Head(t.Context(), key)
	if err != nil || info.Size != 11 || info.Metadata["author"] != "sample" {
		t.Fatalf("Head=%+v %v", info, err)
	}
	get, err := s.Get(t.Context(), key, object.GetOptions{Range: "bytes=1-3"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(get.Body)
	_ = get.Body.Close()
	if err != nil || string(data) != "ell" {
		t.Fatalf("Get=%q %v", data, err)
	}
	signed, err := s.PresignGet(t.Context(), key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(signed.URL)
	if err != nil || u.Query().Get("X-Amz-Expires") != "60" || u.Query().Get("X-Amz-Signature") == "" || !strings.HasPrefix(signed.URL, server.URL) {
		t.Fatalf("presign=%+v %v", signed, err)
	}
	resp, err := http.Get(signed.URL)
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(data) != "hello-world" {
		t.Fatalf("signed Get=%q %v", data, err)
	}
	signedPut, err := s.PresignPut(t.Context(), key, opts, 0)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), signedPut.Method, signedPut.URL, strings.NewReader("new"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = signedPut.Headers.Clone()
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("signed Put=%d", resp.StatusCode)
	}
	if err := s.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(t.Context(), key, object.GetOptions{}); apierror.KindOf(err) != apierror.KindNotFound {
		t.Fatalf("missing key=%v", err)
	}
	if _, err := s.PresignGet(t.Context(), key, 8*24*time.Hour); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("invalid expiry=%v", err)
	}
	if _, err := s.Put(t.Context(), "", strings.NewReader("x"), opts); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("invalid key=%v", err)
	}
}

func TestListPaginationAndPrefixes(t *testing.T) {
	s, _ := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("list-type") != "2" || q.Get("prefix") != "folder/" || q.Get("delimiter") != "/" || q.Get("max-keys") != "1" {
			t.Errorf("query=%v", q)
		}
		w.Header().Set("Content-Type", "application/xml")
		if q.Get("continuation-token") == "" {
			_, _ = io.WriteString(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>opaque+/=</NextContinuationToken><Contents><Key>folder/a</Key><Size>5</Size><ETag>one</ETag></Contents><CommonPrefixes><Prefix>folder/nested/</Prefix></CommonPrefixes></ListBucketResult>`)
		} else {
			if q.Get("continuation-token") != "opaque+/=" {
				t.Errorf("token changed: %q", q.Get("continuation-token"))
			}
			_, _ = io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>folder/b</Key><Size>8</Size></Contents></ListBucketResult>`)
		}
	}, nil)
	opts := object.ListOptions{Prefix: "folder/", Delimiter: "/", Limit: 1}
	first, err := s.List(t.Context(), opts)
	if err != nil || !first.Truncated || len(first.Objects) != 1 || len(first.CommonPrefixes) != 1 {
		t.Fatalf("first=%+v %v", first, err)
	}
	opts.ContinuationToken = first.NextToken
	second, err := s.List(t.Context(), opts)
	if err != nil || second.Truncated || second.Objects[0].Key != "folder/b" {
		t.Fatalf("second=%+v %v", second, err)
	}
}

type readerOnly struct{ io.Reader }

type countingReader struct {
	reader io.Reader
	reads  atomic.Int32
}

func (r *countingReader) Read(p []byte) (int, error) { r.reads.Add(1); return r.reader.Read(p) }

func TestUploadGateRejectsExpiredWaitBeforeBuffering(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	s, _ := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("ETag", "done")
	}, func(c *config.S3Config) { c.MaxConcurrentUploads = 1 })
	defer releaseOnce()
	done := make(chan error, 1)
	go func() {
		_, err := s.Put(t.Context(), "first", strings.NewReader("first"), object.PutOptions{})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first upload did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	input := &countingReader{reader: strings.NewReader("second")}
	if _, err := s.Put(ctx, "second", input, object.PutOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued upload=%v", err)
	}
	if input.reads.Load() != 0 {
		t.Fatal("queued upload allocated/read input before admission")
	}
	releaseOnce()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledMultipartStillAborts(t *testing.T) {
	started := make(chan struct{})
	var startOnce sync.Once
	var aborted atomic.Int32
	s, _ := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == "POST" && q.Has("uploads"):
			_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>cancel-upload</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == "PUT" && q.Get("uploadId") == "cancel-upload":
			_, _ = io.Copy(io.Discard, r.Body)
			startOnce.Do(func() { close(started) })
			<-r.Context().Done()
		case r.Method == "DELETE" && q.Get("uploadId") == "cancel-upload":
			aborted.Add(1)
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected cancellation request %s %s", r.Method, r.URL)
			w.WriteHeader(400)
		}
	}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, "cancel", readerOnly{bytes.NewReader(make([]byte, 6<<20))}, object.PutOptions{})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("part upload did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || aborted.Load() != 1 {
			t.Fatalf("cancel=%v aborts=%d", err, aborted.Load())
		}
	case <-time.After(4 * time.Second):
		t.Fatal("canceled multipart did not finish cleanup")
	}
}

func TestMultipartStreamingAndFailureCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			var mu sync.Mutex
			parts := map[int][]byte{}
			var aborted atomic.Int32
			s, _ := testStore(t, func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				w.Header().Set("Content-Type", "application/xml")
				switch {
				case r.Method == "POST" && q.Has("uploads"):
					_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`)
				case r.Method == "PUT" && q.Get("uploadId") == "upload-1":
					n, _ := strconv.Atoi(q.Get("partNumber"))
					if fail && n == 1 {
						w.WriteHeader(500)
						_, _ = io.WriteString(w, `<Error><Code>InternalError</Code></Error>`)
						return
					}
					data, err := io.ReadAll(r.Body)
					if err != nil {
						return
					}
					mu.Lock()
					parts[n] = data
					mu.Unlock()
					w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, n))
				case r.Method == "POST" && q.Get("uploadId") == "upload-1":
					var request struct {
						Parts []struct {
							Number int `xml:"PartNumber"`
						} `xml:"Part"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if len(request.Parts) != 3 {
						t.Errorf("completed parts=%d", len(request.Parts))
					}
					_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><ETag>multipart-etag</ETag></CompleteMultipartUploadResult>`)
				case r.Method == "DELETE" && q.Get("uploadId") == "upload-1":
					aborted.Add(1)
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected multipart request: %s %s", r.Method, r.URL)
					w.WriteHeader(400)
				}
			}, nil)
			payload := bytes.Repeat([]byte("a"), (10<<20)+123)
			result, err := s.Put(t.Context(), "large", readerOnly{bytes.NewReader(payload)}, object.PutOptions{})
			if fail {
				if err == nil || aborted.Load() != 1 {
					t.Fatalf("failure=%v aborted=%d", err, aborted.Load())
				}
				return
			}
			if err != nil || result.Size != int64(len(payload)) || result.ETag != "multipart-etag" {
				t.Fatalf("multipart=%+v %v", result, err)
			}
			mu.Lock()
			defer mu.Unlock()
			actual := append(append(parts[1], parts[2]...), parts[3]...)
			if !bytes.Equal(actual, payload) {
				t.Fatalf("multipart data corrupted: bytes=%d", len(actual))
			}
		})
	}
}

type testObserver struct{ getDone chan string }

func (o *testObserver) ObserveObjectStorage(op, result string, _ time.Duration) {
	if op == "get" {
		o.getDone <- result
	}
}

func TestDownloadBodyLifetimeAndTimeout(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		t.Run(fmt.Sprint(abandon), func(t *testing.T) {
			closed := make(chan struct{})
			observer := &testObserver{getDone: make(chan string, 2)}
			s, _ := testStore(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "2")
				_, _ = io.WriteString(w, "x")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(closed)
			}, func(c *config.S3Config) { c.RequestTimeout = config.Duration(200 * time.Millisecond) }, WithObserver(observer))
			out, err := s.Get(t.Context(), "stream", object.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-observer.getDone:
				t.Fatalf("recorded at response headers: %s", result)
			default:
			}
			if !abandon {
				var buf [1]byte
				if _, err := out.Body.Read(buf[:]); err != nil {
					t.Fatalf("body canceled before consumption: %v", err)
				}
				_ = out.Body.Close()
				_ = out.Body.Close()
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("body context not canceled")
			}
			select {
			case result := <-observer.getDone:
				if abandon && result != "unavailable" {
					t.Errorf("timeout outcome=%s", result)
				}
			case <-time.After(time.Second):
				t.Fatal("missing completion metric")
			}
			_ = out.Body.Close()
			select {
			case <-observer.getDone:
				t.Fatal("duplicate completion metric")
			default:
			}
		})
	}
}

func TestErrorsClosedStateAndCredentialChain(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "env-test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-test-secret")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	s, _ := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "env-test-key/") {
			t.Error("default credentials not used")
		}
		w.WriteHeader(403)
		_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code></Error>`)
	}, func(c *config.S3Config) { c.AccessKeyID = ""; c.SecretAccessKey = "" })
	if _, err := s.Head(t.Context(), "denied"); apierror.KindOf(err) != apierror.KindPermissionDenied {
		t.Fatalf("403=%v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Get(ctx, "canceled", object.GetOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	_ = s.Close()
	_ = s.Close()
	if _, err := s.Head(t.Context(), "closed"); !errors.Is(err, object.ErrClosed) {
		t.Fatalf("closed=%v", err)
	}
}
