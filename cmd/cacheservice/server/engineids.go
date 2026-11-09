package server

// MaxEngineIDLen bounds the engine cache IDs and session IDs the service
// takes, from OTLP spans and events, an engine's hello and a flush's path.
// An engine names its cache with a UUID (36 characters) and a session with
// crypto/rand.Text (26).
const MaxEngineIDLen = 128

// LogEngineIDsRefused is the message of the log record of what is dropped
// for an invalid engine cache ID or session (refusedEngineIDs).
const LogEngineIDsRefused = "invalid engine cache ID or session: dropped"

// validEngineID reports whether an engine cache ID or session ID is one the
// service may key its state on, log and look up: 1 to MaxEngineIDLen
// letters, digits, '.', '_' or '-'.
func validEngineID(id string) bool {
	if id == "" || len(id) > MaxEngineIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		switch b := id[i]; {
		case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9', b == '.', b == '_', b == '-':
		default:
			return false
		}
	}
	return true
}
