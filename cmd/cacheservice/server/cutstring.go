package server

import "unicode/utf8"

// cutString cuts s to at most n bytes, on a rune boundary, marking the cut
// with "...". It bounds every string that came from the engine before it is
// logged or quoted. An n too small for the mark gets the cut alone.
func cutString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	const mark = "..."
	if n < 0 {
		n = 0
	}
	cut, suffix := n-len(mark), mark
	if cut < 0 {
		cut, suffix = n, ""
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + suffix
}
