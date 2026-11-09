package server

import "unicode/utf8"

// cutString cuts s to at most n bytes, on a rune boundary, marking the cut
// with "...", so the result is never longer than n. It is the one way the
// server bounds an engine-supplied string it logs or quotes; a call site
// has to use it, nothing applies it for them. An n too small for the mark
// gets the cut alone.
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
