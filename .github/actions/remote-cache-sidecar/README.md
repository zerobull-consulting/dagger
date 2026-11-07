# remote-cache-sidecar (deprecated)

Use [`remote-cache/start`](../remote-cache/README.md) and
[`remote-cache/stop`](../remote-cache/stop/action.yml) instead
([zerobull-consulting/dagger#161](https://github.com/zerobull-consulting/dagger/issues/161)).
They start faster (the CLI comes from `dagger/dagger-for-github`, and both
images are pulled at once), and `start` stops the sidecar by itself at the
end of the job.

This directory keeps two thin aliases for callers pinned to the old paths,
and each warns when it runs:

- `remote-cache-sidecar` takes the same inputs as before and runs the same
  script as `remote-cache/start` ([`sidecar.sh`](../remote-cache/sidecar.sh)),
  but without `SIDECAR_CLI`, so it still copies the CLI out of the engine
  image, and it doesn't stop by itself: keep your `remote-cache-sidecar/stop`
  step.
- `remote-cache-sidecar/stop` is `remote-cache/stop`.

To move over: replace the paths, drop `engine-image` if you gave it (give
`version`), and your stop step may go (or stay, for a stop with a timeout of
its own). The aliases are removed after 2027-02-05 (90 days after `remote-cache/start`
landed); until then they run the current script.
