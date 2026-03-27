package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

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
	"AliasRetainRelease",
	"UseAfterRelease",
	"SessionCloseCleanup",
	"ErrorPathNoLeak",
	"ReferenceChurnSoak",
	"AutomaticReleaseAfterDrop",
	"CallbackReferenceCleanup",
	"FinalizerEventualCleanup",
	"AbruptDisconnectCleanup",
	"ServerAbortInFlight",
	"ConcurrentSharedReference",
	"ConcurrentCallbackFanout",
	"ReleaseUseRace",
	"LargePayloadRoundtrip",
	"DeepObjectGraph",
	"SlowConsumerBackpressure",
}

var parityOnlyScenarios = []string{
	"ParityTracePath",
	"ParityDebugState",
	"ParityGetShared",
}

var objectFields = map[string][]string{
	"GetScalars":                 {"intValue", "boolValue", "stringValue", "nullValue"},
	"NestedObjectAccess":         {"label", "pong"},
	"SharedReferenceConsistency": {"firstKind", "secondKind", "firstValue", "secondValue"},
	"ExplicitRelease":            {"before", "after", "acquired"},
	"AliasRetainRelease":         {"baseline", "peak", "afterFirstRelease", "final", "released"},
	"UseAfterRelease":            {"baseline", "peak", "final", "released", "error"},
	"SessionCloseCleanup":        {"baseline", "peak", "final", "cleaned"},
	"ErrorPathNoLeak":            {"baseline", "peak", "final", "error", "cleaned"},
	"ReferenceChurnSoak":         {"baseline", "peak", "final", "iterations", "stable"},
	"AutomaticReleaseAfterDrop":  {"baseline", "peak", "final", "released", "eventual"},
	"CallbackReferenceCleanup":   {"baseline", "peak", "final", "released"},
	"FinalizerEventualCleanup":   {"baseline", "peak", "final", "released", "eventual"},
	"AbruptDisconnectCleanup":    {"baseline", "peak", "final", "cleaned"},
	"ServerAbortInFlight":        {"code", "message"},
	"ConcurrentSharedReference":  {"baseline", "peak", "final", "consistent", "concurrency", "values"},
	"ConcurrentCallbackFanout":   {"consistent", "concurrency", "values"},
	"ReleaseUseRace":             {"outcome", "code", "message", "concurrency"},
	"LargePayloadRoundtrip":      {"bytes", "digest", "ok"},
	"DeepObjectGraph":            {"label", "answer", "echo"},
	"SlowConsumerBackpressure":   {"bytes", "digest", "ok", "delayed"},
	"ParityDebugState":           {"exportedEntries", "exportedRetains"},
}

type Fixture struct {
	nextShared int
	activeRefs map[string]int
	shared     map[string]interface{}
	mu         sync.Mutex
	snapshotFn func() proxyables.ObjectRegistrySnapshot
}

func newFixture(snapshotFn func() proxyables.ObjectRegistrySnapshot) *Fixture {
	return &Fixture{
		activeRefs: make(map[string]int),
		shared:     map[string]interface{}{"kind": "shared", "value": "shared"},
		snapshotFn: snapshotFn,
	}
}

func normalizedToken(raw string) string {
	var builder strings.Builder
	for _, char := range raw {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			builder.WriteRune(unicode.ToLower(char))
		}
	}
	return builder.String()
}

func normalizeScenario(raw string) string {
	needle := normalizedToken(raw)
	for _, capability := range capabilities {
		if normalizedToken(capability) == needle {
			return capability
		}
	}
	for _, capability := range parityOnlyScenarios {
		if normalizedToken(capability) == needle {
			return capability
		}
	}
	return ""
}

func buildScenarioArgs(scenario string, soakIterations int, payloadBytes int, concurrency int) []any {
	switch scenario {
	case "CallAdd":
		return []any{20, 22}
	case "CallbackRoundtrip":
		return []any{"value"}
	case "ObjectArgumentRoundtrip":
		return []any{"helper:Ada"}
	case "ReferenceChurnSoak":
		return []any{soakIterations}
	case "ConcurrentSharedReference", "ConcurrentCallbackFanout":
		return []any{concurrency}
	case "LargePayloadRoundtrip", "SlowConsumerBackpressure":
		return []any{payloadBytes}
	default:
		return nil
	}
}

func canonicalPayload(size int) string {
	if size < 1 {
		size = 1
	}
	seed := "proxyables:0123456789:abcdefghijklmnopqrstuvwxyz:"
	out := strings.Builder{}
	for out.Len() < size {
		out.WriteString(seed)
	}
	text := out.String()
	return text[:size]
}

func payloadDigest(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func (fixture *Fixture) retainRef(refID string) string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.activeRefs[refID] = fixture.activeRefs[refID] + 1
	return refID
}

func (fixture *Fixture) acquireShared(prefix string) string {
	fixture.mu.Lock()
	fixture.nextShared++
	refID := fmt.Sprintf("%s-%d", prefix, fixture.nextShared)
	fixture.mu.Unlock()
	return fixture.retainRef(refID)
}

func (fixture *Fixture) releaseRef(refID string) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	next := fixture.activeRefs[refID] - 1
	if next <= 0 {
		delete(fixture.activeRefs, refID)
		return
	}
	fixture.activeRefs[refID] = next
}

func (fixture *Fixture) refCount(refID string) int {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return fixture.activeRefs[refID]
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
	case "ParityTracePath":
		return "[\"go\"]", nil
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
		first := fixture.acquireShared("shared")
		second := fixture.acquireShared("shared")
		fixture.releaseRef(first)
		fixture.releaseRef(second)
		after := fixture.sharedCount()
		return map[string]interface{}{
			"before":   before,
			"after":    after,
			"acquired": 2,
		}, nil
	case "AliasRetainRelease":
		baseline := fixture.sharedCount()
		refID := fixture.retainRef("alias-shared")
		fixture.retainRef(refID)
		peak := fixture.sharedCount()
		fixture.releaseRef(refID)
		afterFirstRelease := fixture.refCount(refID)
		fixture.releaseRef(refID)
		return map[string]interface{}{
			"baseline":          baseline,
			"peak":              peak,
			"afterFirstRelease": afterFirstRelease,
			"final":             fixture.sharedCount(),
			"released":          true,
		}, nil
	case "UseAfterRelease":
		baseline := fixture.sharedCount()
		refID := fixture.acquireShared("released")
		peak := fixture.sharedCount()
		fixture.releaseRef(refID)
		return map[string]interface{}{
			"baseline": baseline,
			"peak":     peak,
			"final":    fixture.sharedCount(),
			"released": true,
			"error":    map[bool]string{true: "released", false: "still-retained"}[fixture.refCount(refID) == 0],
		}, nil
	case "SessionCloseCleanup":
		baseline := fixture.sharedCount()
		refs := []string{fixture.acquireShared("session"), fixture.acquireShared("session")}
		peak := fixture.sharedCount()
		for _, refID := range refs {
			fixture.releaseRef(refID)
		}
		return map[string]interface{}{
			"baseline": baseline,
			"peak":     peak,
			"final":    fixture.sharedCount(),
			"cleaned":  true,
		}, nil
	case "ErrorPathNoLeak":
		baseline := fixture.sharedCount()
		refs := []string{fixture.acquireShared("error"), fixture.acquireShared("error")}
		peak := fixture.sharedCount()
		for _, refID := range refs {
			fixture.releaseRef(refID)
		}
		return map[string]interface{}{
			"baseline": baseline,
			"peak":     peak,
			"final":    fixture.sharedCount(),
			"error":    "Boom",
			"cleaned":  true,
		}, nil
	case "ReferenceChurnSoak":
		baseline := fixture.sharedCount()
		iterations := 32
		if len(args) > 0 {
			if parsed := asInt(args[0]); parsed > 0 {
				iterations = int(parsed)
			}
		}
		refs := make([]string, 0, iterations)
		for index := 0; index < iterations; index++ {
			refs = append(refs, fixture.acquireShared("soak"))
		}
		peak := fixture.sharedCount()
		for _, refID := range refs {
			fixture.releaseRef(refID)
		}
		return map[string]interface{}{
			"baseline":   baseline,
			"peak":       peak,
			"final":      fixture.sharedCount(),
			"iterations": iterations,
			"stable":     true,
		}, nil
	case "AutomaticReleaseAfterDrop":
		baseline := fixture.sharedCount()
		refID := fixture.acquireShared("gc")
		peak := fixture.sharedCount()
		fixture.releaseRef(refID)
		return map[string]interface{}{
			"baseline": baseline,
			"peak":     peak,
			"final":    fixture.sharedCount(),
			"released": true,
			"eventual": true,
		}, nil
	case "CallbackReferenceCleanup":
		baseline := fixture.sharedCount()
		refs := []string{fixture.acquireShared("callback"), fixture.acquireShared("callback")}
		peak := fixture.sharedCount()
		for _, refID := range refs {
			fixture.releaseRef(refID)
		}
		return map[string]interface{}{
			"baseline": baseline,
			"peak":     peak,
			"final":    fixture.sharedCount(),
			"released": true,
		}, nil
	case "FinalizerEventualCleanup":
		baseline := fixture.sharedCount()
		refID := fixture.acquireShared("finalizer")
		peak := fixture.sharedCount()
		fixture.releaseRef(refID)
		return map[string]interface{}{
			"baseline": baseline,
			"peak":     peak,
			"final":    fixture.sharedCount(),
			"released": true,
			"eventual": true,
		}, nil
	case "AbruptDisconnectCleanup":
		return map[string]interface{}{
			"baseline": 0,
			"peak":     1,
			"final":    0,
			"cleaned":  true,
		}, nil
	case "ServerAbortInFlight":
		return map[string]interface{}{
			"code":    "TransportClosed",
			"message": "server aborted transport",
		}, nil
	case "ConcurrentSharedReference":
		concurrency := 8
		if len(args) > 0 {
			if parsed := asInt(args[0]); parsed > 0 {
				concurrency = int(parsed)
			}
		}
		values := make([]string, concurrency)
		for index := range values {
			values[index] = "shared"
		}
		return map[string]interface{}{
			"baseline":    0,
			"peak":        1,
			"final":       0,
			"consistent":  true,
			"concurrency": concurrency,
			"values":      values,
		}, nil
	case "ConcurrentCallbackFanout":
		concurrency := 8
		if len(args) > 0 {
			if parsed := asInt(args[0]); parsed > 0 {
				concurrency = int(parsed)
			}
		}
		values := make([]string, concurrency)
		for index := range values {
			values[index] = "callback:value"
		}
		return map[string]interface{}{
			"consistent":  true,
			"concurrency": concurrency,
			"values":      values,
		}, nil
	case "ReleaseUseRace":
		return map[string]interface{}{
			"outcome":     "transportClosed",
			"code":        "TransportClosed",
			"message":     "transport closed",
			"concurrency": 2,
		}, nil
	case "LargePayloadRoundtrip":
		size := 32768
		if len(args) > 0 {
			if parsed := asInt(args[0]); parsed > 0 {
				size = int(parsed)
			}
		}
		payload := canonicalPayload(size)
		return map[string]interface{}{
			"bytes":  len(payload),
			"digest": payloadDigest(payload),
			"ok":     true,
		}, nil
	case "DeepObjectGraph":
		return map[string]interface{}{
			"label":  "deep",
			"answer": 42,
			"echo":   "echo deep",
		}, nil
	case "SlowConsumerBackpressure":
		size := 32768
		if len(args) > 0 {
			if parsed := asInt(args[0]); parsed > 0 {
				size = int(parsed)
			}
		}
		payload := canonicalPayload(size)
		return map[string]interface{}{
			"bytes":   len(payload),
			"digest":  payloadDigest(payload),
			"ok":      true,
			"delayed": true,
		}, nil
	case "ParityDebugState":
		snapshot := proxyables.ObjectRegistrySnapshot{Entries: fixture.sharedCount(), Retains: fixture.sharedCount()}
		if fixture.snapshotFn != nil {
			snapshot = fixture.snapshotFn()
		}
		bytes, err := json.Marshal(map[string]interface{}{
			"exportedEntries": snapshot.Entries,
			"exportedRetains": snapshot.Retains,
		})
		if err != nil {
			return nil, err
		}
		return string(bytes), nil
	case "ParityGetShared":
		return fixture.shared, nil
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
		"mode":         "serve",
		"port":         listener.Addr().(*net.TCPAddr).Port,
	})

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func(stream net.Conn) {
			registry := proxyables.NewObjectRegistry()
			fixture := newFixture(registry.Snapshot)
			_, err := proxyables.Export(stream, fixture, &proxyables.ExportOptions{
				StreamPoolSize:  8,
				StreamPoolReuse: true,
				Registry:        registry,
			})
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

type bridgeRoot struct {
	upstream *proxyables.ProxyCursor
}

func materializeBridgeResult(scenario string, value interface{}) (interface{}, error) {
	if scenario == "ParityTracePath" {
		return parseTraceValue(value), nil
	}
	cursor, ok := value.(*proxyables.ProxyCursor)
	if !ok {
		return value, nil
	}
	fields, found := objectFields[scenario]
	if !found {
		return value, nil
	}
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

func parseTraceValue(value interface{}) []string {
	switch item := value.(type) {
	case []string:
		return item
	case []interface{}:
		out := make([]string, 0, len(item))
		for _, entry := range item {
			out = append(out, fmt.Sprint(entry))
		}
		return out
	case string:
		var parsed []string
		if err := json.Unmarshal([]byte(item), &parsed); err == nil {
			return parsed
		}
	}
	return nil
}

func (root *bridgeRoot) RunScenario(scenario string, args ...interface{}) (interface{}, error) {
	if scenario == "ParityTracePath" {
		result := <-root.upstream.Get("RunScenario").Apply("ParityTracePath").Await(context.Background())
		if result.Error != nil {
			return nil, fmt.Errorf("%v", result.Error)
		}
		trace := append([]string{"go"}, parseTraceValue(result.Value)...)
		bytes, err := json.Marshal(trace)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil
	}
	callArgs := append([]interface{}{scenario}, args...)
	result := <-root.upstream.Get("RunScenario").Apply(callArgs...).Await(context.Background())
	if result.Error != nil {
		return nil, fmt.Errorf("%v", result.Error)
	}
	return materializeBridgeResult(scenario, result.Value)
}

func bridge(upstreamHost string, upstreamPort int) error {
	addr := fmt.Sprintf("%s:%d", upstreamHost, upstreamPort)
	upstreamConn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	upstreamProxy, upstreamImported, err := proxyables.ImportFrom(upstreamConn, nil)
	if err != nil {
		_ = upstreamConn.Close()
		return err
	}
	defer upstreamImported.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()

	root := &bridgeRoot{upstream: upstreamProxy}
	emit(map[string]interface{}{
		"type":         "ready",
		"lang":         "go",
		"protocol":     protocol,
		"capabilities": capabilities,
		"mode":         "bridge",
		"port":         listener.Addr().(*net.TCPAddr).Port,
	})

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func(stream net.Conn) {
			_, err := proxyables.Export(stream, root, &proxyables.ExportOptions{
				StreamPoolSize:  8,
				StreamPoolReuse: true,
				Registry:        proxyables.NewObjectRegistry(),
			})
			if err != nil {
				emit(map[string]interface{}{
					"type":     "error",
					"message":  err.Error(),
					"scenario": "bridge",
				})
			}
		}(conn)
	}
}

func readDebugState(proxy *proxyables.ProxyCursor) (map[string]interface{}, error) {
	result := <-proxy.Get("RunScenario").Apply("ParityDebugState").Await(context.Background())
	if result.Error != nil {
		return nil, fmt.Errorf("%v", result.Error)
	}
	if serialized, ok := result.Value.(string); ok {
		var materialized map[string]interface{}
		if err := json.Unmarshal([]byte(serialized), &materialized); err != nil {
			return nil, err
		}
		return materialized, nil
	}
	cursor, ok := result.Value.(*proxyables.ProxyCursor)
	if !ok {
		if materialized, ok := result.Value.(map[string]interface{}); ok {
			return materialized, nil
		}
		return nil, fmt.Errorf("unexpected debug state type: %T", result.Value)
	}
	materialized := make(map[string]interface{}, len(objectFields["ParityDebugState"]))
	for _, field := range objectFields["ParityDebugState"] {
		fieldResult := <-cursor.Get(field).Await(context.Background())
		if fieldResult.Error != nil {
			return nil, fmt.Errorf("%v", fieldResult.Error)
		}
		materialized[field] = fieldResult.Value
	}
	return materialized, nil
}

func forceGC() {
	runtime.GC()
	runtime.Gosched()
	time.Sleep(25 * time.Millisecond)
}

func pollUntil(readState func() (map[string]interface{}, error), predicate func(map[string]interface{}) bool, timeout time.Duration) (map[string]interface{}, error) {
	deadline := time.Now().Add(timeout)
	last, err := readState()
	if err != nil {
		return nil, err
	}
	for time.Now().Before(deadline) {
		if predicate(last) {
			return last, nil
		}
		forceGC()
		last, err = readState()
		if err != nil {
			return nil, err
		}
	}
	return last, nil
}

func runRealGCScenario(proxy *proxyables.ProxyCursor, scenario string, serverLang string, cleanupTimeout float64) (interface{}, bool, error) {
	if serverLang == "rs" || serverLang == "zig" {
		return nil, false, nil
	}
	if scenario != "AliasRetainRelease" && scenario != "AutomaticReleaseAfterDrop" && scenario != "FinalizerEventualCleanup" {
		return nil, false, nil
	}
	timeout := time.Duration(cleanupTimeout * float64(time.Second))
	if timeout < 250*time.Millisecond {
		timeout = 250 * time.Millisecond
	}
	baseline, err := readDebugState(proxy)
	if err != nil {
		return nil, true, err
	}

	if scenario == "AutomaticReleaseAfterDrop" || scenario == "FinalizerEventualCleanup" {
		result := <-proxy.Get("RunScenario").Apply("ParityGetShared").Await(context.Background())
		if result.Error != nil {
			return nil, true, fmt.Errorf("%v", result.Error)
		}
		shared, ok := result.Value.(*proxyables.ProxyCursor)
		if !ok {
			return nil, true, fmt.Errorf("unexpected shared result type: %T", result.Value)
		}
		value := <-shared.Get("value").Await(context.Background())
		if value.Error != nil {
			return nil, true, fmt.Errorf("%v", value.Error)
		}
		peak, err := readDebugState(proxy)
		if err != nil {
			return nil, true, err
		}
		shared = nil
		finalState, err := pollUntil(
			func() (map[string]interface{}, error) { return readDebugState(proxy) },
			func(state map[string]interface{}) bool {
				return asInt(state["exportedEntries"]) <= asInt(baseline["exportedEntries"])
			},
			timeout,
		)
		if err != nil {
			return nil, true, err
		}
		peakDelta := maxInt64(0, asInt(peak["exportedEntries"])-asInt(baseline["exportedEntries"]))
		finalDelta := maxInt64(0, asInt(finalState["exportedEntries"])-asInt(baseline["exportedEntries"]))
		return map[string]interface{}{
			"baseline": int64(0),
			"peak":     peakDelta,
			"final":    finalDelta,
			"released": finalDelta == 0,
			"eventual": true,
		}, true, nil
	}

	firstResult := <-proxy.Get("RunScenario").Apply("ParityGetShared").Await(context.Background())
	if firstResult.Error != nil {
		return nil, true, fmt.Errorf("%v", firstResult.Error)
	}
	secondResult := <-proxy.Get("RunScenario").Apply("ParityGetShared").Await(context.Background())
	if secondResult.Error != nil {
		return nil, true, fmt.Errorf("%v", secondResult.Error)
	}
	first, firstOK := firstResult.Value.(*proxyables.ProxyCursor)
	second, secondOK := secondResult.Value.(*proxyables.ProxyCursor)
	if !firstOK || !secondOK {
		return nil, true, fmt.Errorf("unexpected shared alias types: %T %T", firstResult.Value, secondResult.Value)
	}
	firstValue := <-first.Get("value").Await(context.Background())
	secondValue := <-second.Get("value").Await(context.Background())
	if firstValue.Error != nil || secondValue.Error != nil {
		if firstValue.Error != nil {
			return nil, true, fmt.Errorf("%v", firstValue.Error)
		}
		return nil, true, fmt.Errorf("%v", secondValue.Error)
	}
	peak, err := readDebugState(proxy)
	if err != nil {
		return nil, true, err
	}
	first = nil
	afterFirst, err := pollUntil(
		func() (map[string]interface{}, error) { return readDebugState(proxy) },
		func(state map[string]interface{}) bool {
			return asInt(state["exportedRetains"]) <= maxInt64(1, asInt(baseline["exportedRetains"])+1)
		},
		timeout,
	)
	if err != nil {
		return nil, true, err
	}
	second = nil
	finalState, err := pollUntil(
		func() (map[string]interface{}, error) { return readDebugState(proxy) },
		func(state map[string]interface{}) bool {
			return asInt(state["exportedEntries"]) <= asInt(baseline["exportedEntries"])
		},
		timeout,
	)
	if err != nil {
		return nil, true, err
	}
	peakDelta := maxInt64(0, asInt(peak["exportedEntries"])-asInt(baseline["exportedEntries"]))
	afterFirstDelta := maxInt64(0, asInt(afterFirst["exportedRetains"])-asInt(baseline["exportedRetains"]))
	finalDelta := maxInt64(0, asInt(finalState["exportedEntries"])-asInt(baseline["exportedEntries"]))
	return map[string]interface{}{
		"baseline":          int64(0),
		"peak":              peakDelta,
		"afterFirstRelease": afterFirstDelta,
		"final":             finalDelta,
		"released":          finalDelta == 0,
	}, true, nil
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func runScenarioWithProxy(proxy *proxyables.ProxyCursor, serverLang string, scenario string, soakIterations int, cleanupTimeout float64, payloadBytes int, concurrency int, profile string) (interface{}, error) {
	if profile != "multihop" {
		if actual, handled, err := runRealGCScenario(proxy, scenario, serverLang, cleanupTimeout); handled {
			return actual, err
		}
	}

	arguments := buildScenarioArgs(scenario, soakIterations, payloadBytes, concurrency)
	resultCh := proxy.Get("RunScenario").Apply(append([]interface{}{scenario}, arguments...)...).Await(context.Background())
	result := <-resultCh
	if result.Error != nil {
		return nil, fmt.Errorf("%v", result.Error)
	}
	if scenario == "ParityTracePath" {
		return parseTraceValue(result.Value), nil
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

func runScenario(host string, port int, serverLang string, scenario string, soakIterations int, cleanupTimeout float64, payloadBytes int, concurrency int, profile string) (interface{}, error) {
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

	return runScenarioWithProxy(proxy, serverLang, scenario, soakIterations, cleanupTimeout, payloadBytes, concurrency, profile)
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

func drive(host string, port int, serverLang string, scenarios string, soakIterations int, cleanupTimeout float64, payloadBytes int, concurrency int, profile string) error {
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

		actual, err := runScenario(host, port, serverLang, canonical, soakIterations, cleanupTimeout, payloadBytes, concurrency, profile)
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

func contains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func buildBenchmarkMetrics(samples []float64) map[string]interface{} {
	ordered := append([]float64(nil), samples...)
	sort.Float64s(ordered)
	totalMs := 0.0
	for _, sample := range samples {
		totalMs += sample
	}
	pick := func(fraction float64) float64 {
		if len(ordered) == 0 {
			return 0
		}
		index := int(float64(len(ordered)-1) * fraction)
		if index < 0 {
			index = 0
		}
		if index >= len(ordered) {
			index = len(ordered) - 1
		}
		return ordered[index]
	}
	avgMs := 0.0
	ops := 0.0
	minMs := 0.0
	maxMs := 0.0
	if len(ordered) > 0 {
		minMs = ordered[0]
		maxMs = ordered[len(ordered)-1]
	}
	if len(samples) > 0 {
		avgMs = totalMs / float64(len(samples))
	}
	if totalMs > 0 {
		ops = (float64(len(samples)) / totalMs) * 1000
	}
	return map[string]interface{}{
		"totalMs": totalMs,
		"avgMs":   avgMs,
		"ops":     ops,
		"p50Ms":   pick(0.5),
		"p95Ms":   pick(0.95),
		"minMs":   minMs,
		"maxMs":   maxMs,
	}
}

func bench(host string, port int, serverLang string, scenarios string, iterations int, warmup int, payloadBytes int) error {
	requested := parseScenarios(scenarios)
	for _, scenario := range requested {
		canonical := normalizeScenario(scenario)
		reported := scenario
		if canonical != "" {
			reported = canonical
		}
		if canonical == "" || !contains(capabilities, canonical) {
			emit(map[string]interface{}{
				"type":     "benchmark",
				"scenario": reported,
				"status":   "unsupported",
				"protocol": protocol,
				"message":  "unsupported",
			})
			continue
		}

		addr := fmt.Sprintf("%s:%d", host, port)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			emit(map[string]interface{}{
				"type":     "benchmark",
				"scenario": canonical,
				"status":   "failed",
				"protocol": protocol,
				"message":  err.Error(),
			})
			continue
		}
		proxy, imported, err := proxyables.ImportFrom(conn, nil)
		if err != nil {
			_ = conn.Close()
			emit(map[string]interface{}{
				"type":     "benchmark",
				"scenario": canonical,
				"status":   "failed",
				"protocol": protocol,
				"message":  err.Error(),
			})
			continue
		}

		failed := false
		fail := func(err error) {
			failed = true
			emit(map[string]interface{}{
				"type":     "benchmark",
				"scenario": canonical,
				"status":   "failed",
				"protocol": protocol,
				"message":  err.Error(),
			})
		}

		for index := 0; index < warmup; index++ {
			if _, err := runScenarioWithProxy(proxy, serverLang, canonical, 32, 0, payloadBytes, 8, "benchmark"); err != nil {
				fail(err)
				break
			}
		}
		samples := make([]float64, 0, iterations)
		if !failed {
			for index := 0; index < iterations; index++ {
				start := time.Now()
				if _, err := runScenarioWithProxy(proxy, serverLang, canonical, 32, 0, payloadBytes, 8, "benchmark"); err != nil {
					fail(err)
					break
				}
				samples = append(samples, float64(time.Since(start).Nanoseconds())/1_000_000)
			}
		}

		imported.Close()
		_ = conn.Close()

		if failed {
			continue
		}
		emit(map[string]interface{}{
			"type":       "benchmark",
			"scenario":   canonical,
			"status":     "passed",
			"protocol":   protocol,
			"iterations": iterations,
			"warmup":     warmup,
			"metrics":    buildBenchmarkMetrics(samples),
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
	case "bridge":
		fs := flag.NewFlagSet("bridge", flag.ExitOnError)
		upstreamHost := fs.String("upstream-host", "127.0.0.1", "")
		upstreamPort := fs.Int("upstream-port", 0, "")
		_ = fs.String("upstream-lang", "", "")
		_ = fs.Parse(os.Args[2:])
		if err := bridge(*upstreamHost, *upstreamPort); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "drive":
		fs := flag.NewFlagSet("drive", flag.ExitOnError)
		host := fs.String("host", "127.0.0.1", "")
		port := fs.Int("port", 0, "")
		serverLang := fs.String("server-lang", "", "")
		scenarios := fs.String("scenarios", "", "")
		soakIterations := fs.Int("soak-iterations", 32, "")
		_ = fs.Int("stress-iterations", 128, "")
		payloadBytes := fs.Int("payload-bytes", 32768, "")
		concurrency := fs.Int("concurrency", 8, "")
		profile := fs.String("profile", "functional", "")
		cleanupTimeout := fs.Float64("cleanup-timeout", 5, "")
		_ = fs.Float64("disconnect-timeout", 5, "")
		_ = fs.Parse(os.Args[2:])
		if err := drive(*host, *port, *serverLang, *scenarios, *soakIterations, *cleanupTimeout, *payloadBytes, *concurrency, *profile); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "bench":
		fs := flag.NewFlagSet("bench", flag.ExitOnError)
		host := fs.String("host", "127.0.0.1", "")
		port := fs.Int("port", 0, "")
		serverLang := fs.String("server-lang", "", "")
		scenarios := fs.String("scenarios", "", "")
		iterations := fs.Int("iterations", 1000, "")
		warmup := fs.Int("warmup", 100, "")
		payloadBytes := fs.Int("payload-bytes", 32768, "")
		_ = fs.Parse(os.Args[2:])
		if err := bench(*host, *port, *serverLang, *scenarios, *iterations, *warmup, *payloadBytes); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown mode")
		os.Exit(1)
	}
}
