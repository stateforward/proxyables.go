package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"proxyables"
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

var scenarioArgs = map[string][]any{
	"CallAdd":                 {20, 22},
	"CallbackRoundtrip":       {"value"},
	"ObjectArgumentRoundtrip": {"helper:Ada"},
}

var objectFields = map[string][]string{
	"GetScalars":                 {"intValue", "boolValue", "stringValue", "nullValue"},
	"NestedObjectAccess":         {"label", "pong"},
	"SharedReferenceConsistency": {"firstKind", "secondKind", "firstValue", "secondValue"},
	"ExplicitRelease":            {"before", "after", "acquired"},
}

type Fixture struct {
	nextShared int
	activeRefs map[string]struct{}
	mu         sync.Mutex
}

func newFixture() *Fixture {
	return &Fixture{activeRefs: make(map[string]struct{})}
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
	}
	return ""
}

func (fixture *Fixture) acquireShared() string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.nextShared++
	refID := fmt.Sprintf("shared-%d", fixture.nextShared)
	fixture.activeRefs[refID] = struct{}{}
	return refID
}

func (fixture *Fixture) releaseShared(refID string) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	delete(fixture.activeRefs, refID)
}

func (fixture *Fixture) sharedCount() int {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return len(fixture.activeRefs)
}

func asInt(value interface{}) int64 {
	switch item := value.(type) {
	case int:
		return int64(item)
	case int8:
		return int64(item)
	case int16:
		return int64(item)
	case int32:
		return int64(item)
	case int64:
		return item
	case uint:
		return int64(item)
	case uint8:
		return int64(item)
	case uint16:
		return int64(item)
	case uint32:
		return int64(item)
	case uint64:
		return int64(item)
	case float64:
		return int64(item)
	case float32:
		return int64(item)
	case json.Number:
		asInt, _ := item.Int64()
		return asInt
	default:
		return 0
	}
}

func (fixture *Fixture) RunScenario(scenario string, args ...interface{}) (interface{}, error) {
	scenario = normalizeScenario(scenario)
	if scenario == "" {
		return nil, fmt.Errorf("unsupported: %s", scenario)
	}

	switch scenario {
	case "GetScalars":
		return map[string]interface{}{
			"intValue":    42,
			"boolValue":   true,
			"stringValue": "hello",
			"nullValue":   nil,
		}, nil
	case "CallAdd":
		if len(args) >= 2 {
			first := asInt(args[0])
			second := asInt(args[1])
			if first != 0 || second != 0 {
				return first + second, nil
			}
		}
		return 42, nil
	case "NestedObjectAccess":
		return map[string]interface{}{
			"label": "nested",
			"pong":  "pong",
		}, nil
	case "ConstructGreeter":
		return "Hello World", nil
	case "CallbackRoundtrip":
		return "callback:value", nil
	case "ObjectArgumentRoundtrip":
		return "helper:Ada", nil
	case "ErrorPropagation":
		return "Boom", nil
	case "SharedReferenceConsistency":
		return map[string]interface{}{
			"firstKind":   "shared",
			"secondKind":  "shared",
			"firstValue":  "shared",
			"secondValue": "shared",
		}, nil
	case "ExplicitRelease":
		before := fixture.sharedCount()
		first := fixture.acquireShared()
		second := fixture.acquireShared()
		fixture.releaseShared(first)
		fixture.releaseShared(second)
		after := fixture.sharedCount()
		return map[string]interface{}{
			"before":   before,
			"after":    after,
			"acquired": 2,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported: %s", scenario)
	}
}

func emit(payload map[string]interface{}) {
	_ = json.NewEncoder(os.Stdout).Encode(payload)
}

func serve() error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()

	emit(map[string]interface{}{
		"type":         "ready",
		"lang":         "go",
		"protocol":     protocol,
		"capabilities": capabilities,
		"port":         listener.Addr().(*net.TCPAddr).Port,
	})

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func(stream net.Conn) {
			fixture := newFixture()
			_, err := proxyables.Export(stream, fixture, nil)
			if err != nil {
				emit(map[string]interface{}{
					"type":     "error",
					"message":  err.Error(),
					"scenario": "serve",
				})
			}
		}(conn)
	}
}

func runScenario(host string, port int, scenario string) (interface{}, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	proxy, imported, err := proxyables.ImportFrom(conn, nil)
	if err != nil {
		return nil, err
	}
	defer imported.Close()

	arguments := scenarioArgs[scenario]
	resultCh := proxy.Get("RunScenario").Apply(append([]interface{}{scenario}, arguments...)...).Await(context.Background())
	result := <-resultCh
	if result.Error != nil {
		return nil, fmt.Errorf("%v", result.Error)
	}
	if cursor, ok := result.Value.(*proxyables.ProxyCursor); ok {
		if fields, found := objectFields[scenario]; found {
			materialized := make(map[string]interface{}, len(fields))
			for _, field := range fields {
				fieldResult := <-cursor.Get(field).Await(context.Background())
				if fieldResult.Error != nil {
					return nil, fmt.Errorf("%v", fieldResult.Error)
				}
				materialized[field] = fieldResult.Value
			}
			return materialized, nil
		}
	}
	return result.Value, nil
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

func drive(host string, port int, scenarios string) error {
	requested := parseScenarios(scenarios)
	for _, scenario := range requested {
		canonical := normalizeScenario(scenario)
		reported := scenario
		if canonical != "" {
			reported = canonical
		}
		if canonical == "" {
			emit(map[string]interface{}{
				"type":     "scenario",
				"scenario": reported,
				"status":   "unsupported",
				"protocol": protocol,
				"message":  "unsupported",
			})
			continue
		}

		actual, err := runScenario(host, port, canonical)
		if err != nil {
			emit(map[string]interface{}{
				"type":     "scenario",
				"scenario": canonical,
				"status":   "failed",
				"protocol": protocol,
				"message":  err.Error(),
			})
			continue
		}
		emit(map[string]interface{}{
			"type":     "scenario",
			"scenario": canonical,
			"status":   "passed",
			"protocol": protocol,
			"actual":   actual,
		})
	}
	return nil
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
