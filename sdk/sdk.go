package sdk

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"

	"github.com/juju/errors"
)

// ReadAndParse reads the body of the given HTTP reply and unmarshals it into
// reply. The tag identifies the ONVIF action for diagnostic purposes.
func ReadAndParse(ctx context.Context, httpReply *http.Response, reply interface{}, tag string) error {
	// TODO(jfsmig): extract the deadline from ctx.Deadline() and apply it on the reply reading
	b, err := io.ReadAll(httpReply.Body)
	if err != nil {
		return errors.Annotate(err, "read")
	}

	httpReply.Body.Close()

	err = xml.Unmarshal(b, reply)
	return errors.Annotate(err, "decode")
}
