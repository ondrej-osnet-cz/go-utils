// Package s3 is a minimal client for S3 compatible object storage (AWS S3,
// MinIO, Cloudflare R2, Backblaze B2, Wasabi, …). It covers only what the
// backups need: put, get, list and delete of objects, signed with AWS
// Signature Version 4. It uses only the standard library.
package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// emptyHash is the SHA-256 of an empty payload.
const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// MaxPutSize is the largest object a single PUT may upload.
const MaxPutSize = 5 << 30

// Config says where the bucket is and how to sign requests.
type Config struct {
	// Endpoint is the base URL of the service, e.g.
	// https://s3.eu-central-1.amazonaws.com or http://localhost:9000.
	Endpoint string
	Region   string
	Bucket   string
	// PathStyle puts the bucket into the path (endpoint/bucket/key) instead
	// of the host name (bucket.endpoint/key). MinIO usually needs it.
	PathStyle       bool
	AccessKeyID     string
	SecretAccessKey string
}

// Client talks to one bucket.
type Client struct {
	cfg  Config
	base *url.URL
	http *http.Client
	now  func() time.Time
}

// New checks the configuration and returns a client. A nil httpClient means
// http.DefaultClient.
func New(cfg Config, httpClient *http.Client) (*Client, error) {
	base, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" || base.Host == "" {
		return nil, fmt.Errorf("endpoint %q: want http(s)://host", cfg.Endpoint)
	}
	if strings.Trim(base.Path, "/") != "" || base.RawQuery != "" {
		return nil, fmt.Errorf("endpoint %q: want no path or query", cfg.Endpoint)
	}
	switch {
	case cfg.Region == "":
		return nil, errors.New("region is empty")
	case cfg.Bucket == "":
		return nil, errors.New("bucket is empty")
	case cfg.AccessKeyID == "" || cfg.SecretAccessKey == "":
		return nil, errors.New("access key is empty")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{cfg: cfg, base: base, http: httpClient, now: time.Now}, nil
}

// Object is one entry of a listing.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Error is an error response of the service.
type Error struct {
	Status  int
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("s3: HTTP %d", e.Status)
	}
	return fmt.Sprintf("s3: HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// IsNotFound reports whether err says the object does not exist.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

// Put uploads size bytes of body as key. payloadHash is the hex SHA-256 of
// the body; the service rejects the upload if the body does not match it.
func (c *Client) Put(ctx context.Context, key string, body io.Reader, size int64, payloadHash, contentType string) error {
	if size > MaxPutSize {
		return fmt.Errorf("put %s: %d bytes is over the %d bytes of a single PUT", key, size, int64(MaxPutSize))
	}
	req, err := c.request(ctx, http.MethodPut, key, nil, body, payloadHash)
	if err != nil {
		return err
	}
	req.ContentLength = size
	if size == 0 {
		// A zero ContentLength with a non-nil body would be sent chunked.
		req.Body = http.NoBody
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.do(req)
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return resp.Body.Close()
}

// Get downloads key. The caller closes the returned body.
func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	req, err := c.request(ctx, http.MethodGet, key, nil, nil, emptyHash)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return resp.Body, nil
}

// Delete removes key. Deleting a missing key is no error.
func (c *Client) Delete(ctx context.Context, key string) error {
	req, err := c.request(ctx, http.MethodDelete, key, nil, nil, emptyHash)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return resp.Body.Close()
}

// List returns all objects whose key starts with prefix, in key order.
func (c *Client) List(ctx context.Context, prefix string) ([]Object, error) {
	var objects []Object
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		req, err := c.request(ctx, http.MethodGet, "", q, nil, emptyHash)
		if err != nil {
			return nil, err
		}
		page, err := c.listPage(req)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			objects = append(objects, Object{Key: o.Key, Size: o.Size, LastModified: o.LastModified})
		}
		if !page.IsTruncated || page.NextContinuationToken == "" {
			return objects, nil
		}
		token = page.NextContinuationToken
	}
}

type listResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string    `xml:"Key"`
		Size         int64     `xml:"Size"`
		LastModified time.Time `xml:"LastModified"`
	} `xml:"Contents"`
}

func (c *Client) listPage(req *http.Request) (*listResult, error) {
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var page listResult
	if err := xml.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("decode listing: %w", err)
	}
	return &page, nil
}

// do sends a signed request and turns a non-2xx response into *Error.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 == 2 {
		return resp, nil
	}
	defer func() { _ = resp.Body.Close() }()
	e := &Error{Status: resp.StatusCode}
	// The body is informative only; a HEAD or an odd proxy may send none.
	_ = xml.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(e)
	e.Status = resp.StatusCode
	return nil, e
}

// request builds a signed request for key (empty = the bucket itself).
func (c *Client) request(ctx context.Context, method, key string, query url.Values, body io.Reader, payloadHash string) (*http.Request, error) {
	host := c.base.Host
	path := "/" + key
	if c.cfg.PathStyle {
		path = "/" + c.cfg.Bucket + path
	} else {
		host = c.cfg.Bucket + "." + host
	}
	canonicalPath := encodePath(path)
	canonicalQuery := encodeQuery(query)
	raw := c.base.Scheme + "://" + host + canonicalPath
	if canonicalQuery != "" {
		raw += "?" + canonicalQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, raw, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	// net/url keeps canonicalPath as RawPath, so the path goes out as signed.
	c.sign(req, host, canonicalPath, canonicalQuery, payloadHash, c.now().UTC())
	return req, nil
}

// sign adds the Signature Version 4 headers. It signs host, range and the
// x-amz-* headers; others (content-type, user-agent, …) may change on the way.
func (c *Client) sign(req *http.Request, host, canonicalPath, canonicalQuery, payloadHash string, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	day := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	headers := map[string]string{"host": host}
	for name, values := range req.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") || lower == "range" {
			headers[lower] = strings.TrimSpace(strings.Join(values, ","))
		}
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, k := range names {
		canonicalHeaders.WriteString(k + ":" + headers[k] + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	canonicalRequest := strings.Join([]string{
		req.Method, canonicalPath, canonicalQuery, canonicalHeaders.String(), signedHeaders, payloadHash,
	}, "\n")
	scope := day + "/" + c.cfg.Region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(canonicalRequest))

	key := hmacSHA256([]byte("AWS4"+c.cfg.SecretAccessKey), day)
	key = hmacSHA256(key, c.cfg.Region)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(key, stringToSign))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.cfg.AccessKeyID+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
}

// encodePath URI-encodes every byte except unreserved characters and '/'.
func encodePath(p string) string {
	return uriEncode(p, false)
}

// encodeQuery returns the canonical query string: sorted by key, every byte
// except unreserved characters encoded.
func encodeQuery(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		values := append([]string(nil), q[k]...)
		sort.Strings(values)
		for _, v := range values {
			parts = append(parts, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	return strings.Join(parts, "&")
}

func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case 'A' <= ch && ch <= 'Z', 'a' <= ch && ch <= 'z', '0' <= ch && ch <= '9',
			ch == '-', ch == '_', ch == '.', ch == '~':
			b.WriteByte(ch)
		case ch == '/' && !encodeSlash:
			b.WriteByte(ch)
		default:
			b.WriteString("%" + strings.ToUpper(strconv.FormatUint(uint64(ch)|0x100, 16)[1:]))
		}
	}
	return b.String()
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
