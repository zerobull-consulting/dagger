package main

import (
	"fmt"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// gcModeUsage is the help text of -gc-mode.
const gcModeUsage = "who collects garbage: service (the service, every -gc-interval, and on POST /debug/gc), batch (a 'cacheservice gc' job collects; the service has no schedule, and POST /debug/gc still runs GC), or off (nothing collects, so the bucket grows without bound; POST /debug/gc answers 409). -export=false is off; it refuses an explicit service or batch"

// resolveGCMode reads -gc-mode and -gc-interval into the mode the service
// runs, and refuses the combinations that can't be honored. mode and
// interval are the flags' values; exports is -export; set holds the flags
// set on the command line, by name. It is separate from main so that a
// subcommand reading the same flags uses the same rules.
//
//   - an unknown mode is refused, listing the three;
//   - a negative -gc-interval, or an explicit 0, is refused: 0 used to mean
//     "only POST /debug/gc", which is -gc-mode=batch now;
//   - an explicit -gc-interval with batch or off is refused: it would do
//     nothing;
//   - a read-only instance (-export=false) never deletes from the bucket, so
//     it is off: an explicit service or batch is refused, an explicit off
//     accepted, and so is the default, with no explicit -gc-interval.
func resolveGCMode(mode string, interval time.Duration, exports bool, set map[string]bool) (server.GCMode, error) {
	m, err := server.ParseGCMode(mode)
	if err != nil {
		return "", err
	}
	if interval < 0 {
		return "", fmt.Errorf("-gc-interval=%s: negative; want a positive interval (a schedule needs -gc-mode=service, the default)", interval)
	}
	// A zero interval with an explicit batch or off is the interval-with-mode
	// refusal below: pointing it at the mode it already has would go in a circle.
	explicitNoSchedule := exports && set["gc-mode"] && m != server.GCModeService
	if set["gc-interval"] && interval == 0 && !explicitNoSchedule {
		if !exports {
			return "", fmt.Errorf("-gc-interval=0 and -export=false: a zero interval no longer means 'only POST /debug/gc', and a read-only instance never runs garbage collection, which deletes from the bucket; drop -gc-interval (a read-only instance is -gc-mode=%s already)", server.GCModeOff)
		}
		return "", fmt.Errorf("-gc-interval=0: no longer means 'only POST /debug/gc'; use -gc-mode=%s to run GC on demand (a job collects), or -gc-mode=%s to collect nothing", server.GCModeBatch, server.GCModeOff)
	}
	if !exports {
		if set["gc-mode"] && m != server.GCModeOff {
			return "", fmt.Errorf("-gc-mode=%s and -export=false: a read-only instance never runs garbage collection, which deletes from the bucket; drop -gc-mode, or set it to %s", m, server.GCModeOff)
		}
		m = server.GCModeOff
	}
	if m != server.GCModeService && set["gc-interval"] {
		why := ""
		if !exports {
			why = " (-export=false is -gc-mode=off)"
		}
		return "", fmt.Errorf("-gc-interval=%s and -gc-mode=%s%s: that mode has no schedule, so an interval would do nothing; drop -gc-interval, or use -gc-mode=%s", interval, m, why, server.GCModeService)
	}
	return m, nil
}
