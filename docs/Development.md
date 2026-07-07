# Development

## Onvif Command Support
Each of the following Onvif Web services has its own directory:
- [Analytics](../analytics)
- [Device](../device)
- [Event](../event)
- [Imaging](../imaging)
- [Media](../media)
- [Media2](../media2)
- [PTZ](../ptz)

Inside each directory there is:
- `types.go`: contains the struct definitions for each onvif command and response
- `function.go`: contains the auto-generated types that implement the `Function` interface providing `Request()` and `Response()` type mappings.

At the root level there is:
- [names.go](../names.go): contains the auto-generated constant names of all the commands
- [mappings.go](../mappings.go): contains the auto-generated mappings for each Onvif WebService from function name to function datatype


### Adding support for additional commands
> **Note:** Currently, the python script looks for types that end with `Response` and work backwards from there.
> This is to prevent creating commands for every struct type defined there, and only the ones that are actually commands.
> It also skips any types ending with `FaultResponse`, as there typically are no `Fault` commands, only responses.

For the respective web service the command belongs to, add the command and response struct definitions
into `<web-service-name>/types.go`, and then run:
```shell
python3 python/gen_commands.py
```

> **Note:** You can also typically run the generator within your IDE thanks to the `//go:generate` lines 
> towards the top of the `types.go` files.

## Higher-level helpers

Some web service directories ship hand-written, higher-level helpers
built on top of the wire-layer commands. These are normal Go packages
— **not** covered by the `gen_commands.py` workflow above and not
expected to be regenerated.

- [event/stream](../event/stream) — channel-based event consumer that
  owns the pull-point subscription lifecycle (Create, Pull, Renew,
  Unsubscribe, reconnect with jittered backoff) and decodes
  notifications into normalized typed Events. Vendor topic strings
  (AXIS, Hikvision, Avigilon, Hanwha, Bosch, Dahua) are classified
  into a small set of `Kind` values. See the package `doc.go` for the
  public surface and usage.
- [event/topic](../event/topic) — topic identifier helpers.

When adding a similar higher-level helper, place it under the relevant
web service directory as a sub-package so consumers find it next to
the wire-layer types it builds on.
