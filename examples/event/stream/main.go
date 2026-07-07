// Command streamtest opens an event stream against an ONVIF camera and
// prints decoded events as they arrive. Useful for verifying the
// classifier against real-camera topics; not intended as a production
// tool.
//
// # Usage
//
//	go run ./examples/event/stream \
//	  -xaddr 192.168.1.10 \
//	  -username root \
//	  -duration 60s
//
// # Credentials
//
// The camera password is read, in order of preference:
//
//  1. The ONVIF_PASSWORD environment variable.
//  2. A file pointed at by -password-file (newline stripped).
//  3. Interactive prompt when stdin is a tty.
//
// -password is also accepted but DISCOURAGED — it leaks the credential
// into shell history and the system process listing. Use only for
// throwaway dev cameras.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kerberos-io/onvif"
	"github.com/kerberos-io/onvif/event/stream"
)

func main() {
	xaddr := flag.String("xaddr", "", "camera host or host:port (required)")
	username := flag.String("username", "", "ONVIF user (required)")
	insecurePassword := flag.String("password", "", "INSECURE — leaks into shell history; prefer ONVIF_PASSWORD env or -password-file")
	passwordFile := flag.String("password-file", "", "read password from this file (newline trimmed)")
	deviceID := flag.String("device-id", "", "logical name printed with each event (default: xaddr)")
	filter := flag.String("filter", "", "raw ONVIF ConcreteSet topic filter (empty = all topics, works on AXIS)")
	pullTimeout := flag.Duration("pull-timeout", 5*time.Second, "server-side wait per PullMessages call")
	duration := flag.Duration("duration", 0, "stop after this long (0 = run until Ctrl-C)")
	flag.Parse()

	if *xaddr == "" || *username == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *deviceID == "" {
		*deviceID = *xaddr
	}

	password, err := loadPassword(*insecurePassword, *passwordFile)
	if err != nil {
		log.Fatalf("password: %v", err)
	}

	dev, err := onvif.NewDevice(onvif.DeviceParams{
		Xaddr:    *xaddr,
		Username: *username,
		Password: password,
		AuthMode: onvif.UsernameTokenAuth,
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *duration > 0 {
		var done context.CancelFunc
		ctx, done = context.WithTimeout(ctx, *duration)
		defer done()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	s, err := stream.NewStream(ctx, dev, stream.Options{
		DeviceID:       *deviceID,
		RawTopicFilter: *filter,
		PullTimeout:    *pullTimeout,
	})
	if err != nil {
		log.Fatalf("open stream: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			log.Printf("stream close: %v", err)
		}
	}()

	log.Printf("streaming from %s (device-id=%s, filter=%q)", *xaddr, *deviceID, *filter)
	for {
		select {
		case <-ctx.Done():
			log.Printf("done (%v)", ctx.Err())
			return
		case ev, ok := <-s.Events():
			if !ok {
				return
			}
			fmt.Printf("%s  kind=%-15s state=%-9s op=%-12s topic=%s",
				ev.Timestamp.Format(time.RFC3339), ev.Kind, ev.State, ev.Operation, ev.Topic)
			if ev.AfterReconnect {
				fmt.Print("  [after-reconnect]")
			}
			if len(ev.Source) > 0 {
				fmt.Printf("  source=%v", ev.Source)
			}
			if len(ev.Data) > 0 {
				fmt.Printf("  data=%v", ev.Data)
			}
			fmt.Println()
		case e, ok := <-s.Errors():
			if !ok {
				return
			}
			var pull stream.ErrPullFailed
			var recreate stream.ErrRecreateFailed
			switch {
			case errors.As(e, &recreate):
				log.Printf("RECREATE failed: %v (camera may be offline)", recreate.Err)
			case errors.As(e, &pull):
				log.Printf("pull error (will retry): %v", pull.Err)
			default:
				log.Printf("stream error: %v", e)
			}
		}
	}
}

// loadPassword resolves the camera password from the environment first
// (ONVIF_PASSWORD), then -password-file, then an interactive prompt as
// a last resort. The insecure -password flag is honoured only if
// nothing else is set, and a warning is logged.
func loadPassword(insecure, file string) (string, error) {
	if env := os.Getenv("ONVIF_PASSWORD"); env != "" {
		return env, nil
	}
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", file, err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	if insecure != "" {
		log.Print("WARNING: -password leaks into shell history and process listings; prefer ONVIF_PASSWORD env or -password-file")
		return insecure, nil
	}
	// Interactive prompt — works when stdin is a tty. We use a plain
	// reader (rather than golang.org/x/term hidden input) to keep
	// this example dependency-free; in production, callers should
	// integrate term.ReadPassword.
	fmt.Fprint(os.Stderr, "ONVIF password (visible): ")
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", errors.New("no password supplied (set ONVIF_PASSWORD, -password-file, or pipe input)")
	}
	return strings.TrimRight(line, "\r\n"), nil
}
