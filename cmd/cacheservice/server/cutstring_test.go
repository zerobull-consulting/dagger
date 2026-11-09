package server

import (
	"testing"
	"unicode/utf8"
)

func TestCutString(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"fits", "abc", 3, "abc"},
		{"cut", "abcdefgh", 6, "abc..."},
		// "é" is two bytes: a cut at byte 3 would split the second one.
		{"rune boundary", "abéééé", 6, "ab..."},
		{"four-byte rune", "a😀😀😀", 7, "a..."},
		{"too small for the mark", "abcdef", 2, "ab"},
		{"too small, rune boundary", "éé", 1, ""},
		{"zero", "abc", 0, ""},
		{"negative", "abc", -1, ""},
		{"empty", "", 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := cutString(tc.in, tc.n)
			if got != tc.want {
				t.Errorf("cutString(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
			}
			if len(got) > max(tc.n, 0) && len(tc.in) > tc.n {
				t.Errorf("%d bytes, over %d", len(got), tc.n)
			}
			if !utf8.ValidString(got) {
				t.Errorf("%q is not valid UTF-8", got)
			}
		})
	}
}
