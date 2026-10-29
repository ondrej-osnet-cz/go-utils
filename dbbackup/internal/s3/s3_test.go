package s3

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// The examples of the S3 documentation, "Signature Calculations for the
// Authorization Header: Transferring Payload in a Single Chunk".
func TestSign_AWSExamples(t *testing.T) {
	t.Parallel()
	// #nosec G101 -- the published example key of the documentation
	c, err := New(Config{
		Endpoint:        "https://s3.amazonaws.com",
		Region:          "us-east-1",
		Bucket:          "examplebucket",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) }

	tests := []struct {
		name, path, query, rangeHeader, want string
	}{
		{"get object", "/test.txt", "", "bytes=0-9", "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"},
		{"list objects", "/", "max-keys=2&prefix=J", "", "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com"+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.rangeHeader != "" {
				req.Header.Set("Range", tt.rangeHeader)
			}
			c.sign(req, "examplebucket.s3.amazonaws.com", tt.path, tt.query, emptyHash, c.now())
			auth := req.Header.Get("Authorization")
			if !strings.HasSuffix(auth, "Signature="+tt.want) {
				t.Errorf("Authorization = %s, want signature %s", auth, tt.want)
			}
		})
	}
}

func TestEncodePath(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want string }{
		{"/a/b.db/20261028T030000Z.db.gz", "/a/b.db/20261028T030000Z.db.gz"},
		{"/a b+c=d", "/a%20b%2Bc%3Dd"},
		{"/účet~x", "/%C3%BA%C4%8Det~x"},
	}
	for _, tt := range tests {
		if got := encodePath(tt.in); got != tt.want {
			t.Errorf("encodePath(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNew_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	ok := Config{Endpoint: "https://s3.example.com", Region: "r", Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s"}
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"no scheme", func(c *Config) { c.Endpoint = "s3.example.com" }},
		{"path", func(c *Config) { c.Endpoint = "https://s3.example.com/x" }},
		{"no region", func(c *Config) { c.Region = "" }},
		{"no bucket", func(c *Config) { c.Bucket = "" }},
		{"no secret", func(c *Config) { c.SecretAccessKey = "" }},
	}
	if _, err := New(ok, nil); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for _, tt := range tests {
		cfg := ok
		tt.change(&cfg)
		if _, err := New(cfg, nil); err == nil {
			t.Errorf("%s: no error", tt.name)
		}
	}
}
