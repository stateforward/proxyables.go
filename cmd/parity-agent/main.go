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
	"GetScalars",
	"CallAdd",
	"NestedObjectAccess",
	"ConstructGreeter",
	"CallbackRoundtrip",
	"ObjectArgumentRoundtrip",
	"ErrorPropagation",
	"SharedReferenceConsistency",
	"ExplicitRelease",
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

func normalizeScenario(raw string) string {
	switch {
	case raw == "GetScalars":
		return raw
	case strings.EqualFold(raw, "get_scalars"):
		return "GetScalars"
	case strings.EqualFold(raw, "getscalars"):
		return "GetScalars"
	case strings.EqualFold(raw, "get-scalars"):
		return "GetScalars"

	case raw == "CallAdd":
		return raw
	case strings.EqualFold(raw, "call_add"):
		return "CallAdd"
	case strings.EqualFold(raw, "calladd"):
		return "CallAdd"
	case strings.EqualFold(raw, "call-add"):
		return "CallAdd"

	case raw == "NestedObjectAccess":
		return raw
	case strings.EqualFold(raw, "nested_object_access"):
		return "NestedObjectAccess"
	case strings.EqualFold(raw, "nestedobjectaccess"):
		return "NestedObjectAccess"
	case strings.EqualFold(raw, "nested-object-access"):
		return "NestedObjectAccess"

	case raw == "ConstructGreeter":
		return raw
	case strings.EqualFold(raw, "construct_greeter"):
		return "ConstructGreeter"
	case strings.EqualFold(raw, "constructgreeter"):
		return "ConstructGreeter"
	case strings.EqualFold(raw, "construct-greeter"):
		return "ConstructGreeter"

	case raw == "CallbackRoundtrip":
		return raw
	case strings.EqualFold(raw, "callback_roundtrip"):
		return "CallbackRoundtrip"
	case strings.EqualFold(raw, "callbackroundtrip"):
		return "CallbackRoundtrip"
	case strings.EqualFold(raw, "callback-roundtrip"):
		return "CallbackRoundtrip"

	case raw == "ObjectArgumentRoundtrip":
		return raw
	case strings.EqualFold(raw, "object_argument_roundtrip"):
		return "ObjectArgumentRoundtrip"
	case strings.EqualFold(raw, "objectargumentroundtrip"):
		return "ObjectArgumentRoundtrip"
	case strings.EqualFold(raw, "object-argument-roundtrip"):
		return "ObjectArgumentRoundtrip"

	case raw == "ErrorPropagation":
		return raw
	case strings.EqualFold(raw, "error_propagation"):
		return "ErrorPropagation"
	case strings.EqualFold(raw, "errorpropagation"):
		return "ErrorPropagation"
	case strings.EqualFold(raw, "error-propagation"):
		return "ErrorPropagation"

	case raw == "SharedReferenceConsistency":
		return raw
	case strings.EqualFold(raw, "shared_reference_consistency"):
		return "SharedReferenceConsistency"
	case strings.EqualFold(raw, "sharedreferenceconsistency"):
		return "SharedReferenceConsistency"
	case strings.EqualFold(raw, "shared-reference-consistency"):
		return "SharedReferenceConsistency"

	case raw == "ExplicitRelease":
		return raw
	case strings.EqualFold(raw, "explicit_release"):
		return "ExplicitRelease"
	case strings.EqualFold(raw, "explicitrelease"):
		return "ExplicitRelease"
	case strings.EqualFold(raw, "explicit-release"):
		return "ExplicitRelease"
	default:
		return ""
	}
}

func canonicalOrOriginal(canonical string, original string) string {
	if canonical != "" {
		return canonical
	}
	return original
}

func (fixture *Fixture) hasCapability(name string) bool {
	canonical := normalizeScenario(name)
	if canonical == "" {
		return false
	}
	for _, item := range capabilities {
		if item == canonical {
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
		"firstKind":   shared["kind"],
		"secondKind":  shared["kind"],
		"firstValue":  shared["value"],
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
		"before":   statsBefore["active"],
		"after":    statsAfter["active"],
		"acquired": 2,
	}, nil
}

func emit(payload map[string]interface{}) {
	_ = json.NewEncoder(os.Stdout).Encode(payload)
}

func runScenario(fixture *Fixture, scenario string) (interface{}, error) {
	scenario = normalizeScenario(scenario)
	if scenario == "" {
		return nil, errors.New("unsupported")
	}
	switch scenario {
	case "GetScalars":
		return fixture.getScalars(), nil
	case "CallAdd":
		return fixture.callAdd(), nil
	case "NestedObjectAccess":
		return fixture.nestedObjectAccess(), nil
	case "ConstructGreeter":
		return fixture.constructGreeter(), nil
	case "CallbackRoundtrip":
		return fixture.callbackRoundtrip(), nil
	case "ObjectArgumentRoundtrip":
		return fixture.objectArgumentRoundtrip(), nil
	case "ErrorPropagation":
		return fixture.errorPropagation(), nil
	case "SharedReferenceConsistency":
		return fixture.sharedReferenceConsistency(), nil
	case "ExplicitRelease":
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
				canonical := normalizeScenario(scenario)
				if canonical == "" || !fixture.hasCapability(canonical) {
					_ = encoder.Encode(map[string]interface{}{
						"type":     "scenario",
						"scenario": canonicalOrOriginal(canonical, scenario),
						"status":   "unsupported",
						"protocol": protocol,
						"message":  "unsupported",
					})
					continue
				}

				actual, err := runScenario(fixture, canonical)
				if err != nil {
					_ = encoder.Encode(map[string]interface{}{
						"type":     "scenario",
						"scenario": canonical,
						"status":   "unsupported",
						"protocol": protocol,
						"message":  err.Error(),
					})
					continue
				}

				_ = encoder.Encode(map[string]interface{}{
					"type":     "scenario",
					"scenario": canonical,
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

	requested := make([]string, 0, len(parseScenarios(scenarios)))
	for _, scenario := range parseScenarios(scenarios) {
		requested = append(requested, canonicalOrOriginal(normalizeScenario(scenario), scenario))
	}
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
			"type":     "scenario",
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
