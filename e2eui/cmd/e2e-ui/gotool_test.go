package main

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestRunPattern(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tests   []string
		want    string
		matches []string
		misses  []string
	}{
		{tests: nil, want: ""},
		{tests: []string{"TestA"}, want: "^TestA$"},
		{tests: []string{"TestA/sub#01"}, want: `^TestA$/^sub#01$`},
		{tests: []string{"TestA.B(x)"}, want: `^TestA\.B\(x\)$`, matches: []string{"TestA.B(x)"}, misses: []string{"TestAxB(x)"}},
		{tests: []string{"TestH_A", "TestH_B"}, want: `^(?:TestH_A|TestH_B)$`,
			matches: []string{"TestH_A", "TestH_B"}, misses: []string{"TestH_AB", "TestXTestH_A", "TestH_"}},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.tests, ","), func(t *testing.T) {
			t.Parallel()
			got := runPattern(tt.tests)
			if got != tt.want {
				t.Fatalf("runPattern(%q) = %q, want %q", tt.tests, got, tt.want)
			}
			for _, name := range tt.matches {
				if !regexp.MustCompile(got).MatchString(name) {
					t.Errorf("%q does not match %q", got, name)
				}
			}
			for _, name := range tt.misses {
				if regexp.MustCompile(got).MatchString(name) {
					t.Errorf("%q matches %q", got, name)
				}
			}
		})
	}
}

func TestParseList(t *testing.T) {
	t.Parallel()
	out := `{"Action":"start","Package":"example.com/b"}
{"Action":"output","Package":"example.com/b","Output":"TestB2\n"}
{"Action":"output","Package":"example.com/b","Output":"TestB1\n"}
{"Action":"output","Package":"example.com/b","Output":"ExampleB\n"}
{"Action":"output","Package":"example.com/b","Output":"ok  \texample.com/b\t0.01s\n"}
{"Action":"pass","Package":"example.com/b","Elapsed":0.01}
{"Action":"start","Package":"example.com/a"}
{"Action":"output","Package":"example.com/a","Output":"TestA\n"}
{"Action":"skip","Package":"example.com/none"}
`
	got, err := parseList(strings.NewReader(out))
	if err != nil {
		t.Fatal(err)
	}
	want := []pkgTests{
		{Package: "example.com/a", Tests: []string{"TestA"}},
		{Package: "example.com/b", Tests: []string{"TestB2", "TestB1", "ExampleB"}},
	}
	if !slices.EqualFunc(got, want, func(a, b pkgTests) bool { return a.Package == b.Package && slices.Equal(a.Tests, b.Tests) }) {
		t.Errorf("parseList = %+v, want %+v", got, want)
	}
}

func TestParseList_InvalidJSON(t *testing.T) {
	t.Parallel()
	if _, err := parseList(strings.NewReader("not json")); err == nil {
		t.Error("expected an error")
	}
}
