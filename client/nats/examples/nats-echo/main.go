// Copyright 2018-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/kubemq-io/broker/client/nats"
)

// NOTE: Can test with demo servers.
// nats-echo -s demo.nats.io <subject>

func usage() {
	log.Printf("Usage: nats-echo [-s server] [-creds file] [-t] <subject>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func printMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Echoing from [%s] to [%s]: %q", i, m.Subject, m.Reply, m.Data)
}

func printStatusMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Sending status from [%s] to [%s]: %q", i, m.Subject, m.Reply, m.Data)
}

type serviceStatus struct {
	Id  string `json:"id"`
	Geo string `json:"geo"`
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var serviceId = flag.String("id", "NATS Echo Service", "Identifier for this service")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var showHelp = flag.Bool("h", false, "Show help message")
	var geoloc = flag.Bool("geo", false, "Display geo location of echo service")
	var geo string = "unknown"

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 1 {
		showUsageAndExit(1)
	}

	// Lookup geo if requested
	if *geoloc {
		geo = lookupGeo()
	}
	// Connect Options.
	opts := []nats.Option{nats.Name(*serviceId)}
	opts = setupConnOptions(opts)

	if *userCreds != "" && *nkeyFile != "" {
		log.Fatal("specify -seed or -creds")
	}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Use Nkey authentication.
	if *nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(*nkeyFile)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, opt)
	}

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

	subj, iEcho, iStatus := args[0], 0, 0
	statusSubj := subj + ".status"

	nc.QueueSubscribe(subj, "echo", func(msg *nats.Msg) {
		iEcho++
		printMsg(msg, iEcho)
		if msg.Reply != "" {
			// Just echo back what they sent us.
			var payload []byte
			if geo != "unknown" {
				payload = []byte(fmt.Sprintf("[%s]: %q", geo, msg.Data))
			} else {
				payload = msg.Data
			}
			nc.Publish(msg.Reply, payload)
		}
	})
	nc.Subscribe(statusSubj, func(msg *nats.Msg) {
		iStatus++
		printStatusMsg(msg, iStatus)
		if msg.Reply != "" {
			payload, _ := json.Marshal(&serviceStatus{Id: *serviceId, Geo: geo})
			nc.Publish(msg.Reply, payload)
		}
	})
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Echo Service ID: [%s]", *serviceId)
	log.Printf("Echo Service listening on [%s]\n", subj)
	log.Printf("Echo Service (Status) listening on [%s]\n", statusSubj)

	// Now handle signal to terminate so we can drain on exit.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)

	go func() {
		// Wait for signal
		<-c
		log.Printf("<caught signal - draining>")
		nc.Drain()
	}()

	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		if !nc.IsClosed() {
			log.Printf("Disconnected due to: %s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
		}
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		if !nc.IsClosed() {
			log.Fatal("Exiting: no servers available")
		} else {
			log.Fatal("Exiting")
		}
	}))
	return opts
}

// We only want region, country
type geo struct {
	// There are others..
	Region  string
	Country string
}

// lookup our current region and country..
func lookupGeo() string {
	c := &http.Client{Timeout: 2 * time.Second}

	url := os.Getenv("ECHO_SVC_GEO_URL")
	if len(url) == 0 {
		url = "https://ipapi.co/json"
	}

	resp, err := c.Get(url)
	if err != nil || resp == nil {
		log.Fatalf("Could not retrieve geo location data: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	g := geo{}
	if err := json.Unmarshal(body, &g); err != nil {
		log.Fatalf("Error unmarshalling geo: %v", err)
	}
	return g.Region + ", " + g.Country
}
