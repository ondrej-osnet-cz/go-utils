// Package s3test is an in-memory S3 server for tests. It serves path-style
// requests to one bucket and checks each request's Signature Version 4
// against what actually arrived, so a path or query sent differently from
// how it was signed fails like on a real service.
package s3test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Credentials the server accepts.
const (
	Region          = "test-region"
	Bucket          = "backups"
	AccessKeyID     = "AKIDTEST"
	SecretAccessKey = "secret-test-key"
)

// Server is a running fake.
type Server struct {
	URL string

	mu       sync.Mutex
	objects  map[string]object
	pageSize int
	failPut  bool
}

type object struct {
	data     []byte
	modified time.Time
}

// New starts a server that is closed when the test ends.
func New(t *testing.T) *Server {
	t.Helper()
	s := &Server{objects: map[string]object{}, pageSize: 1000}
	srv := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(srv.Close)
	s.URL = srv.URL
	return s
}

// Keys returns the stored keys in order.
func (s *Server) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Object returns the stored bytes of key.
func (s *Server) Object(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	return o.data, ok
}

// SetPageSize limits a listing page, to exercise continuation tokens.
func (s *Server) SetPageSize(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageSize = n
}

// SetFailPut switches failing PUTs on or off.
func (s *Server) SetFailPut(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failPut = fail
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}
	if code, msg := verify(r, body); code != "" {
		writeError(w, http.StatusForbidden, code, msg)
		return
	}
	prefix := "/" + Bucket
	if r.URL.Path != prefix && !strings.HasPrefix(r.URL.Path, prefix+"/") {
		writeError(w, http.StatusNotFound, "NoSuchBucket", r.URL.Path)
		return
	}
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, prefix), "/")

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == http.MethodPut && key != "":
		if s.failPut {
			writeError(w, http.StatusInternalServerError, "InternalError", "put disabled")
			return
		}
		s.objects[key] = object{data: body, modified: time.Now().UTC()}
	case r.Method == http.MethodGet && key != "":
		o, ok := s.objects[key]
		if !ok {
			writeError(w, http.StatusNotFound, "NoSuchKey", key)
			return
		}
		_, _ = w.Write(o.data)
	case r.Method == http.MethodDelete && key != "":
		delete(s.objects, key)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		s.list(w, r.URL.Query())
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method+" "+r.URL.Path)
	}
}

func (s *Server) list(w http.ResponseWriter, q url.Values) {
	type content struct {
		Key          string    `xml:"Key"`
		Size         int       `xml:"Size"`
		LastModified time.Time `xml:"LastModified"`
	}
	var res struct {
		XMLName               xml.Name  `xml:"ListBucketResult"`
		IsTruncated           bool      `xml:"IsTruncated"`
		NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
		Contents              []content `xml:"Contents"`
	}
	var keys []string
	for k := range s.objects {
		// The token is the last key of the previous page.
		if strings.HasPrefix(k, q.Get("prefix")) && k > q.Get("continuation-token") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) > s.pageSize {
		keys = keys[:s.pageSize]
		res.IsTruncated = true
		res.NextContinuationToken = keys[len(keys)-1]
	}
	for _, k := range keys {
		res.Contents = append(res.Contents, content{Key: k, Size: len(s.objects[k].data), LastModified: s.objects[k].modified})
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(res)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: code, Message: msg})
}

// verify recomputes the signature from the request as received.
func verify(r *http.Request, body []byte) (code, msg string) {
	sum := sha256.Sum256(body)
	if got := r.Header.Get("X-Amz-Content-Sha256"); got != hex.EncodeToString(sum[:]) {
		return "XAmzContentSHA256Mismatch", "payload hash " + got
	}
	auth := r.Header.Get("Authorization")
	var credential, signedHeaders, signature string
	for _, part := range strings.Split(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "), ", ") {
		k, v, _ := strings.Cut(part, "=")
		switch k {
		case "Credential":
			credential = v
		case "SignedHeaders":
			signedHeaders = v
		case "Signature":
			signature = v
		}
	}
	amzDate := r.Header.Get("X-Amz-Date")
	if len(amzDate) < 8 {
		return "AccessDenied", "no X-Amz-Date"
	}
	scope := amzDate[:8] + "/" + Region + "/s3/aws4_request"
	if credential != AccessKeyID+"/"+scope {
		return "InvalidAccessKeyId", credential
	}

	var headers strings.Builder
	for _, h := range strings.Split(signedHeaders, ";") {
		v := r.Header.Get(h)
		if h == "host" {
			v = r.Host
		}
		headers.WriteString(h + ":" + strings.TrimSpace(v) + "\n")
	}
	canonical := strings.Join([]string{
		r.Method, r.URL.EscapedPath(), canonicalQuery(r.URL.RawQuery), headers.String(), signedHeaders,
		r.Header.Get("X-Amz-Content-Sha256"),
	}, "\n")
	cr := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(cr[:])

	key := []byte("AWS4" + SecretAccessKey)
	for _, part := range []string{amzDate[:8], Region, "s3", "aws4_request", toSign} {
		m := hmac.New(sha256.New, key)
		m.Write([]byte(part))
		key = m.Sum(nil)
	}
	if hex.EncodeToString(key) != signature {
		return "SignatureDoesNotMatch", "canonical request:\n" + canonical
	}
	return "", ""
}

// canonicalQuery sorts the received pairs; the client must already have
// encoded them canonically, as a real service compares the same way.
func canonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	pairs := strings.Split(raw, "&")
	for i, p := range pairs {
		if !strings.Contains(p, "=") {
			pairs[i] = p + "="
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}
