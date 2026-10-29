package s3_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/s3"
	"github.com/ondrej-osnet-cz/go-utils/dbbackup/internal/s3/s3test"
)

func newClient(t *testing.T, srv *s3test.Server) *s3.Client {
	t.Helper()
	c, err := s3.New(s3.Config{
		Endpoint:        srv.URL,
		Region:          s3test.Region,
		Bucket:          s3test.Bucket,
		PathStyle:       true,
		AccessKeyID:     s3test.AccessKeyID,
		SecretAccessKey: s3test.SecretAccessKey,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func put(t *testing.T, c *s3.Client, key, data string) {
	t.Helper()
	sum := sha256.Sum256([]byte(data))
	if err := c.Put(context.Background(), key, strings.NewReader(data), int64(len(data)), hex.EncodeToString(sum[:]), "text/plain"); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func TestClient_PutGetListDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := s3test.New(t)
	c := newClient(t, srv)

	// Keys with characters that must be encoded the same way when signing and sending.
	keys := []string{"p/a b+c=d.db/1.gz", "p/účet.db/2.gz", "p/x.db/3.gz", "q/y.db/4.gz", "p/empty"}
	for _, k := range keys[:4] {
		put(t, c, k, "data of "+k)
	}
	put(t, c, keys[4], "")

	for _, k := range keys[:4] {
		body, err := c.Get(ctx, k)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		got, _ := io.ReadAll(body)
		_ = body.Close()
		if string(got) != "data of "+k {
			t.Errorf("get %s = %q", k, got)
		}
	}

	srv.SetPageSize(2)
	objects, err := c.List(ctx, "p/")
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, o := range objects {
		listed = append(listed, o.Key)
	}
	want := []string{"p/a b+c=d.db/1.gz", "p/empty", "p/x.db/3.gz", "p/účet.db/2.gz"}
	if !slices.Equal(listed, want) {
		t.Errorf("list = %q, want %q", listed, want)
	}

	if err := c.Delete(ctx, "p/x.db/3.gz"); err != nil {
		t.Fatal(err)
	}
	if _, ok := srv.Object("p/x.db/3.gz"); ok {
		t.Error("object still there after delete")
	}
	if _, err := c.Get(ctx, "p/x.db/3.gz"); !s3.IsNotFound(err) {
		t.Errorf("get deleted object: err = %v, want not found", err)
	}
}

func TestClient_PutRejectsWrongHash(t *testing.T) {
	t.Parallel()
	srv := s3test.New(t)
	c := newClient(t, srv)
	err := c.Put(context.Background(), "k", strings.NewReader("data"), 4, strings.Repeat("0", 64), "text/plain")
	if err == nil || !strings.Contains(err.Error(), "XAmzContentSHA256Mismatch") {
		t.Errorf("err = %v, want hash mismatch", err)
	}
}

func TestClient_WrongSecret(t *testing.T) {
	t.Parallel()
	srv := s3test.New(t)
	c, err := s3.New(s3.Config{
		Endpoint: srv.URL, Region: s3test.Region, Bucket: s3test.Bucket, PathStyle: true,
		AccessKeyID: s3test.AccessKeyID, SecretAccessKey: "wrong",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "SignatureDoesNotMatch") {
		t.Errorf("err = %v, want signature mismatch", err)
	}
}
