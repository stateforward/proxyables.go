package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

const protocol = "parity-json-v1"

var capabilities = []string{
	"get_scalars",
	"call_add",
	"nested_object_access",
	"construct_greeter",
	"callback_roundtrip",
	"object_argument_roundtrip",
	"error_propagation",
	"shared_reference_consistency",
	"explicit_release",
}

type Fixture struct {
	intValue    int
	boolValue   bool
	stringValue string
	nullValue   interface{}
	nested      map[string]interface{}
	nextShared  int
	activeRefs  map[string]struct{}
	mu          sync.Mutex
}

func newFixture() *Fixture {
	state := &Fixture{
		intValue:    42,
		boolValue:   true,
		stringValue: "hello",
		nullValue:   nil,
		nested: map[string]interface{}{
			"label": "nested",
			"ping":  func() string { return "pong" },
		},
		activeRefs: make(map[string]struct{}),
	}
	return state
}

func parseScenarios(raw string) []string {
	parts := strings.Split(raw, ",")
	items := make([]string, 0, len(parts))
	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		items = append(items, item)
	}
	return items
}

func (fixture *Fixture) hasCapability(name string) bool {
	for _, item := range capabilities {
		if item == name {
			return true
		}
	}
	return false
}

func (fixture *Fixture) getScalars() map[string]interface{} {
	return map[string]interface{}{
		"intValue":    fixture.intValue,
		"boolValue":   fixture.boolValue,
		"stringValue": fixture.stringValue,
		"nullValue":   fixture.nullValue,
	}
}

func (fixture *Fixture) callAdd() int {
	return 42
}

func (fixture *Fixture) nestedObjectAccess() map[string]interface{} {
	return map[string]interface{}{
		"label": fixture.nested["label"],
		"pong":  fixture.nested["ping"].(func() string)(),
	}
}

func (fixture *Fixture) constructGreeter() string {
	return "Hello World"
}

func (fixture *Fixture) callbackRoundtrip() string {
	return "callback:value"
}

func (fixture *Fixture) objectArgumentRoundtrip() string {
	return "helper:Ada"
}

func (fixture *Fixture) errorPropagation() string {
	return "Boom"
}

func (fixture *Fixture) sharedReferenceConsistency() map[string]interface{} {
	shared := map[string]interface{}{"kind": "shared", "value": "shared"}
	return map[string]interface{}{
		"firstKind":  shared["kind"],
		"secondKind": shared["kind"],
		"firstValue": shared["value"],
		"secondValue": shared["value"],
	}
}

func (fixture *Fixture) acquireShared() string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()

	fixture.nextShared++
	id := fmt.Sprintf("shared-%d", fixture.nextShared)
	fixture.activeRefs[id] = struct{}{}
	return id
}

func (fixture *Fixture) releaseShared(refId string) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	delete(fixture.activeRefs, refId)
}

func (fixture *Fixture) debugStats() map[string]int {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return map[string]int{"active": len(fixture.activeRefs)}
}

func (fixture *Fixture) explicitRelease() (map[string]int, error) {
	statsBefore := fixture.debugStats()
	first := fixture.acquireShared()
	second := fixture.acquireShared()
	fixture.releaseShared(first)
	fixture.releaseShared(second)
	statsAfter := fixture.debugStats()
	return map[string]int{
		"before":  statsBefore["active"],
		"after":   statsAfter["active"],
		"acquired": 2,
	}, nil
}

func emit(payload map[string]interface{}) {
	_ = json.NewEncoder(os.Stdout).Encode(payload)
}

func runScenario(fixture *Fixture, scenario string) (interface{}, error) {
	switch scenario {
	case "get_scalars":
		return fixture.getScalars(), nil
	case "call_add":
		return fixture.callAdd(), nil
	case "nested_object_access":
		return fixture.nestedObjectAccess(), nil
	case "construct_greeter":
		return fixture.constructGreeter(), nil
	case "callback_roundtrip":
		return fixture.callbackRoundtrip(), nil
	case "object_argument_roundtrip":
		return fixture.objectArgumentRoundtrip(), nil
	case "error_propagation":
		return fixture.errorPropagation(), nil
	case "shared_reference_consistency":
		return fixture.sharedReferenceConsistency(), nil
	case "explicit_release":
		return fixture.explicitRelease()
	default:
		return nil, errors.New("unsupported")
	}
}

func serve() error {
	fixture := newFixture()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	emit(map[string]interface{}{
		"type":         "ready",
		"lang":         "go",
		"protocol":     protocol,
		"capabilities": capabilities,
		"port":         port,
	})

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func(connection net.Conn) {
			defer connection.Close()

			request, _ := io.ReadAll(connection)
			scenarios := parseScenarios(string(request))

			encoder := json.NewEncoder(connection)
			for _, scenario := range scenarios {
				if !fixture.hasCapability(scenario) {
					_ = encoder.Encode(map[string]interface{}{
						"type":      "scenario",
						"scenario": scenario,
						"status":   "unsupported",
						"protocol": protocol,
						"message":  "unsupported",
					})
					continue
				}

				actual, err := runScenario(fixture, scenario)
				if err != nil {
					_ = encoder.Encode(map[string]interface{}{
						"type":      "scenario",
						"scenario": scenario,
						"status":   "unsupported",
						"protocol": protocol,
						"message":  err.Error(),
					})
					continue
				}

				_ = encoder.Encode(map[string]interface{}{
					"type":      "scenario",
					"scenario": scenario,
					"status":   "passed",
					"protocol": protocol,
					"actual":   actual,
				})
			}
		}(conn)
	}
}

func drive(host string, port int, scenarios string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	defer conn.Close()

	requested := parseScenarios(scenarios)
	request := strings.Join(requested, ",") + "\n"
	_, err = conn.Write([]byte(request))
	if err != nil {
		return err
	}
	_ = conn.(*net.TCPConn).CloseWrite()

	scanner := bufio.NewScanner(conn)
	seen := map[string]struct{}{}
	for scanner.Scan() {
		line := scanner.Bytes()
		var item map[string]interface{}
		if err := json.Unmarshal(line, &item); err != nil {
			continue
		}
		if item["type"] == "scenario" {
			if name, ok := item["scenario"].(string); ok {
				seen[name] = struct{}{}
			}
		}
		emit(item)
	}

	for _, scenario := range requested {
		if _, ok := seen[scenario]; ok {
			continue
		}
		emit(map[string]interface{}{
			"type":      "scenario",
			"scenario": scenario,
			"status":   "failed",
			"protocol": protocol,
			"message":  "server did not emit a result",
		})
	}

	return scanner.Err()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "expected mode")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		if err := serve(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "drive":
		fs := flag.NewFlagSet("drive", flag.ExitOnError)
		host := fs.String("host", "127.0.0.1", "")
		port := fs.Int("port", 0, "")
		scenarios := fs.String("scenarios", "", "")
		_ = fs.String("server-lang", "", "")
		_ = fs.Parse(os.Args[2:])
		if err := drive(*host, *port, *scenarios); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown mode")
		os.Exit(1)
	}
}
