package proxyables

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func isPrimitive(value interface{}) bool {
	switch value.(type) {
	case nil:
		return true
	case bool, string:
		return true
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
	case []byte:
		return true
	default:
		return false
	}
}

func createPrimitiveValue(value interface{}) ProxyInstruction {
	switch value.(type) {
	case nil:
		return createNullValue()
	case bool:
		return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindBoolean), Data: value}
	case string:
		return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindString), Data: value}
	case []byte:
		return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindUnknown), Data: value}
	case int, int8, int16, int32, int64:
		return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindNumber), Data: toInt64(value)}
	case uint, uint8, uint16, uint32, uint64:
		return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindNumber), Data: toUint64(value)}
	case float32, float64:
		return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindNumber), Data: toFloat64(value)}
	default:
		return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindUnknown), Data: value}
	}
}

func createNullValue() ProxyInstruction {
	return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindNull), Data: nil}
}

func createUndefinedValue() ProxyInstruction {
	return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindUndefined), Data: nil}
}

func parseInstructionSlice(data interface{}) ([]ProxyInstruction, *ProxyError) {
	switch v := data.(type) {
	case []ProxyInstruction:
		return v, nil
	case []interface{}:
		out := make([]ProxyInstruction, 0, len(v))
		for _, item := range v {
			instr, err := parseInstruction(item)
			if err != nil {
				return nil, err
			}
			out = append(out, instr)
		}
		return out, nil
	default:
		return nil, &ProxyError{Message: "invalid execute data"}
	}
}

func parseInstruction(data interface{}) (ProxyInstruction, *ProxyError) {
	switch v := data.(type) {
	case ProxyInstruction:
		return v, nil
	case map[string]interface{}:
		return instructionFromStringMap(v)
	case map[interface{}]interface{}:
		return instructionFromInterfaceMap(v)
	default:
		return ProxyInstruction{}, &ProxyError{Message: "invalid instruction"}
	}
}

func instructionFromStringMap(data map[string]interface{}) (ProxyInstruction, *ProxyError) {
	kind, ok := asUint32(data["kind"])
	if !ok {
		return ProxyInstruction{}, &ProxyError{Message: "invalid instruction kind"}
	}
	instr := ProxyInstruction{Kind: kind}
	if id, ok := data["id"].(string); ok {
		instr.ID = id
	}
	instr.Data = data["data"]
	if meta, ok := data["metadata"]; ok {
		instr.Metadata = meta
	}
	return instr, nil
}

func instructionFromInterfaceMap(data map[interface{}]interface{}) (ProxyInstruction, *ProxyError) {
	var kindVal interface{}
	var idVal interface{}
	var dataVal interface{}
	var metaVal interface{}
	for k, v := range data {
		ks, ok := k.(string)
		if !ok {
			continue
		}
		switch ks {
		case "kind":
			kindVal = v
		case "id":
			idVal = v
		case "data":
			dataVal = v
		case "metadata":
			metaVal = v
		}
	}
	kind, ok := asUint32(kindVal)
	if !ok {
		return ProxyInstruction{}, &ProxyError{Message: "invalid instruction kind"}
	}
	instr := ProxyInstruction{Kind: kind, Data: dataVal, Metadata: metaVal}
	if id, ok := idVal.(string); ok {
		instr.ID = id
	}
	return instr, nil
}

func asUint32(value interface{}) (uint32, bool) {
	switch v := value.(type) {
	case uint8:
		return uint32(v), true
	case uint16:
		return uint32(v), true
	case uint32:
		return v, true
	case uint64:
		return uint32(v), true
	case int:
		return uint32(v), true
	case int8:
		return uint32(v), true
	case int16:
		return uint32(v), true
	case int32:
		return uint32(v), true
	case int64:
		return uint32(v), true
	case float32:
		return uint32(v), true
	case float64:
		return uint32(v), true
	default:
		return 0, false
	}
}

func parseGetKey(data interface{}) (string, *ProxyError) {
	switch v := data.(type) {
	case []interface{}:
		if len(v) == 0 {
			return "", &ProxyError{Message: "invalid get data"}
		}
		if key, ok := v[0].(string); ok {
			return key, nil
		}
		return fmt.Sprint(v[0]), nil
	case []string:
		if len(v) == 0 {
			return "", &ProxyError{Message: "invalid get data"}
		}
		return v[0], nil
	default:
		return "", &ProxyError{Message: "invalid get data"}
	}
}

func parseArgs(data interface{}) ([]interface{}, *ProxyError) {
	switch v := data.(type) {
	case []interface{}:
		return v, nil
	case nil:
		return nil, nil
	default:
		return nil, &ProxyError{Message: "invalid apply data"}
	}
}

func parseReleaseID(data interface{}) (string, bool) {
	switch v := data.(type) {
	case []interface{}:
		if len(v) == 0 {
			return "", false
		}
		if id, ok := v[0].(string); ok {
			return id, true
		}
	case []string:
		if len(v) == 0 {
			return "", false
		}
		return v[0], true
	}
	return "", false
}

func getProperty(target interface{}, key string) (interface{}, bool, *ProxyError) {
	if target == nil {
		return nil, false, &ProxyError{Message: "target is nil"}
	}
	v := reflect.ValueOf(target)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, false, &ProxyError{Message: "target is nil"}
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		index, err := strconv.Atoi(key)
		if err != nil {
			return nil, false, &ProxyError{Message: "invalid index"}
		}
		if index < 0 || index >= v.Len() {
			return nil, false, nil
		}
		return v.Index(index).Interface(), true, nil
	case reflect.Map:
		keyValue, ok := convertKey(key, v.Type().Key())
		if !ok {
			return nil, false, &ProxyError{Message: "invalid map key"}
		}
		val := v.MapIndex(keyValue)
		if !val.IsValid() {
			return nil, false, nil
		}
		return val.Interface(), true, nil
	case reflect.Struct:
		field := v.FieldByName(key)
		if field.IsValid() && field.CanInterface() {
			return field.Interface(), true, nil
		}
		method := v.Addr().MethodByName(key)
		if method.IsValid() {
			return method.Interface(), true, nil
		}
		method = v.MethodByName(key)
		if method.IsValid() {
			return method.Interface(), true, nil
		}
		return nil, false, nil
	default:
		method := reflect.ValueOf(target).MethodByName(key)
		if method.IsValid() {
			return method.Interface(), true, nil
		}
	}
	return nil, false, nil
}

func convertKey(key string, t reflect.Type) (reflect.Value, bool) {
	switch t.Kind() {
	case reflect.String:
		return reflect.ValueOf(key).Convert(t), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		iv, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(iv).Convert(t), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uv, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(uv).Convert(t), true
	}
	return reflect.Value{}, false
}

func callValue(target interface{}, args []interface{}, executor Executor) (interface{}, *ProxyError) {
	if target == nil {
		return nil, &ProxyError{Message: "target is nil"}
	}
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Func {
		return nil, &ProxyError{Message: "target is not a function"}
	}
	fnType := v.Type()
	if fnType.IsVariadic() {
		minArgs := fnType.NumIn() - 1
		if len(args) < minArgs {
			return nil, &ProxyError{Message: "too few arguments"}
		}
	}
	if !fnType.IsVariadic() && len(args) != fnType.NumIn() {
		return nil, &ProxyError{Message: "argument count mismatch"}
	}

	callArgs := make([]reflect.Value, 0, len(args))
	if fnType.IsVariadic() {
		fixedCount := fnType.NumIn() - 1
		if fixedCount > 0 {
			for i := 0; i < fixedCount; i++ {
				argValue, err := convertArg(args[i], fnType.In(i), executor)
				if err != nil {
					return nil, err
				}
				callArgs = append(callArgs, argValue)
			}
		}

		if fnType.NumIn() > 0 {
			varArgType := fnType.In(fnType.NumIn() - 1)
			varArgElementType := varArgType.Elem()
			var varArgs reflect.Value
			if len(args) <= fixedCount {
				varArgs = reflect.MakeSlice(varArgType, 0, 0)
			} else {
				varArgs = reflect.MakeSlice(varArgType, 0, len(args)-fixedCount)
				for i := fixedCount; i < len(args); i++ {
					argValue, err := convertArg(args[i], varArgElementType, executor)
					if err != nil {
						return nil, err
					}
					varArgs = reflect.Append(varArgs, argValue)
				}
			}
			callArgs = append(callArgs, varArgs)
		}
	} else {
		for i := 0; i < len(args); i++ {
			argValue, err := convertArg(args[i], fnType.In(i), executor)
			if err != nil {
				return nil, err
			}
			callArgs = append(callArgs, argValue)
		}
	}

	results := func() []reflect.Value {
		if fnType.IsVariadic() {
			return v.CallSlice(callArgs)
		}
		return v.Call(callArgs)
	}()
	if len(results) == 0 {
		return nil, nil
	}
	if len(results) == 1 {
		return results[0].Interface(), nil
	}
	if last := results[len(results)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		if !last.IsNil() {
			return nil, &ProxyError{Message: last.Interface().(error).Error()}
		}
		results = results[:len(results)-1]
	}
	if len(results) == 1 {
		return results[0].Interface(), nil
	}
	out := make([]interface{}, len(results))
	for i, res := range results {
		out[i] = res.Interface()
	}
	return out, nil
}

func convertArg(arg interface{}, targetType reflect.Type, executor Executor) (reflect.Value, *ProxyError) {
	if instr, ok := parseReferenceValue(arg); ok {
		if targetType.Kind() == reflect.Func {
			return makeCallback(targetType, instr.Data.(string), executor)
		}
		if targetType.Kind() == reflect.Interface {
			cursor := NewProxyCursor(executor, []ProxyInstruction{instr})
			return reflect.ValueOf(cursor).Convert(targetType), nil
		}
		return reflect.Zero(targetType), &ProxyError{Message: "cannot assign reference to parameter"}
	}

	if instr, err := parseInstruction(arg); err == nil {
		switch ProxyValueKind(instr.Kind) {
		case ProxyValueKindUndefined, ProxyValueKindNull:
			arg = nil
		case ProxyValueKindString, ProxyValueKindNumber, ProxyValueKindBoolean, ProxyValueKindObject, ProxyValueKindArray:
			arg = instr.Data
		}
	}

	value := reflect.ValueOf(arg)
	if !value.IsValid() {
		return reflect.Zero(targetType), nil
	}
	if value.Type().AssignableTo(targetType) {
		return value, nil
	}
	if value.Type().ConvertibleTo(targetType) {
		return value.Convert(targetType), nil
	}
	return reflect.Zero(targetType), &ProxyError{Message: "argument type mismatch"}
}

func parseReferenceValue(value interface{}) (ProxyInstruction, bool) {
	instr, err := parseInstruction(value)
	if err != nil {
		return ProxyInstruction{}, false
	}
	if ProxyValueKind(instr.Kind) == ProxyValueKindReference {
		if _, ok := instr.Data.(string); ok {
			return instr, true
		}
	}
	return ProxyInstruction{}, false
}

func prepareInstructionsForSend(instructions []ProxyInstruction, registry *ObjectRegistry) []ProxyInstruction {
	if len(instructions) == 0 {
		return instructions
	}
	prepared := make([]ProxyInstruction, 0, len(instructions))
	for _, instr := range instructions {
		kind := ProxyInstructionKind(instr.Kind)
		switch kind {
		case ProxyInstructionKindApply, ProxyInstructionKindConstruct:
			args, _ := parseArgs(instr.Data)
			converted := make([]interface{}, len(args))
			for i, arg := range args {
				converted[i] = transformArgForSend(arg, registry)
			}
			prepared = append(prepared, CreateInstructionUnsafe(kind, converted))
		default:
			prepared = append(prepared, instr)
		}
	}
	return prepared
}

func transformArgForSend(arg interface{}, registry *ObjectRegistry) interface{} {
	if arg == nil {
		return nil
	}
	if cursor, ok := arg.(*ProxyCursor); ok {
		if refID, ok := cursor.referenceID(); ok {
			return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindReference), Data: refID}
		}
	}
	if instr, ok := parseReferenceValue(arg); ok {
		return instr
	}
	if isPrimitive(arg) {
		return arg
	}
	refID := registry.Register(arg)
	return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindReference), Data: refID}
}

func makeCallback(fnType reflect.Type, refID string, executor Executor) (reflect.Value, *ProxyError) {
	if fnType.Kind() != reflect.Func {
		return reflect.Zero(fnType), &ProxyError{Message: "callback type is not func"}
	}

	fn := reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		callArgs := make([]interface{}, len(args))
		for i, arg := range args {
			callArgs[i] = arg.Interface()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		instructions := []ProxyInstruction{
			{ID: MakeID(), Kind: uint32(ProxyValueKindReference), Data: refID},
			CreateInstructionUnsafe(ProxyInstructionKindApply, callArgs),
		}
		value, err := executor.Execute(ctx, instructions)
		return buildCallbackResults(fnType, value, err)
	})
	return fn, nil
}

func buildCallbackResults(fnType reflect.Type, value interface{}, err *ProxyError) []reflect.Value {
	outCount := fnType.NumOut()
	results := make([]reflect.Value, outCount)
	lastIsError := outCount > 0 && fnType.Out(outCount-1).Implements(reflect.TypeOf((*error)(nil)).Elem())

	if err != nil {
		for i := 0; i < outCount; i++ {
			results[i] = reflect.Zero(fnType.Out(i))
		}
		if lastIsError {
			results[outCount-1] = reflect.ValueOf(errors.New(err.Message))
		}
		return results
	}

	if outCount == 0 {
		return results
	}

	if outCount == 1 {
		results[0] = coerceValue(value, fnType.Out(0))
		return results
	}

	if values, ok := value.([]interface{}); ok && len(values) >= outCount {
		for i := 0; i < outCount; i++ {
			results[i] = coerceValue(values[i], fnType.Out(i))
		}
		return results
	}

	results[0] = coerceValue(value, fnType.Out(0))
	for i := 1; i < outCount; i++ {
		results[i] = reflect.Zero(fnType.Out(i))
	}
	return results
}

func coerceValue(value interface{}, targetType reflect.Type) reflect.Value {
	if value == nil {
		return reflect.Zero(targetType)
	}
	v := reflect.ValueOf(value)
	if v.IsValid() && v.Type().AssignableTo(targetType) {
		return v
	}
	if v.IsValid() && v.Type().ConvertibleTo(targetType) {
		return v.Convert(targetType)
	}
	return reflect.Zero(targetType)
}

func executeRemote(ctx context.Context, pool *StreamPool, instructions []ProxyInstruction, registry *ObjectRegistry) (ProxyInstruction, *ProxyError) {
	if ctx == nil {
		ctx = context.Background()
	}
	stream, err := pool.Acquire()
	if err != nil {
		return ProxyInstruction{}, &ProxyError{Message: err.Error()}
	}

	enc := msgpack.NewEncoder(stream)
	dec := msgpack.NewDecoder(stream)

	if registry != nil {
		instructions = prepareInstructionsForSend(instructions, registry)
	}
	execInstr := CreateInstructionUnsafe(ProxyInstructionKindExecute, instructions)
	if err := enc.Encode(execInstr); err != nil {
		_ = stream.Close()
		return ProxyInstruction{}, &ProxyError{Message: err.Error()}
	}

	respCh := make(chan struct {
		instr ProxyInstruction
		err   error
	}, 1)

	go func() {
		var resp ProxyInstruction
		err := dec.Decode(&resp)
		respCh <- struct {
			instr ProxyInstruction
			err   error
		}{instr: resp, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = stream.Close()
		return ProxyInstruction{}, &ProxyError{Message: ctx.Err().Error()}
	case resp := <-respCh:
		if resp.err != nil {
			_ = stream.Close()
			return ProxyInstruction{}, &ProxyError{Message: resp.err.Error()}
		}
		pool.Release(stream)
		return resp.instr, nil
	}
}

func unwrapReturn(resp ProxyInstruction, executor Executor) (interface{}, *ProxyError) {
	kind := ProxyInstructionKind(resp.Kind)
	switch kind {
	case ProxyInstructionKindThrow:
		return nil, parseProxyError(resp.Data)
	case ProxyInstructionKindReturn:
		return unwrapValue(resp.Data, executor)
	default:
		return nil, &ProxyError{Message: "unexpected response kind"}
	}
}

func unwrapValue(value interface{}, executor Executor) (interface{}, *ProxyError) {
	instr, err := parseInstruction(value)
	if err == nil {
		switch ProxyValueKind(instr.Kind) {
		case ProxyValueKindReference:
			refID, ok := instr.Data.(string)
			if !ok {
				return nil, &ProxyError{Message: "invalid reference id"}
			}
			cursor := NewProxyCursor(executor, []ProxyInstruction{instr})
			if imp, ok := executor.(*ImportedProxyable); ok {
				imp.registerFinalizer(cursor, refID)
			}
			return cursor, nil
		case ProxyValueKindUndefined:
			return nil, nil
		case ProxyValueKindNull:
			return nil, nil
		default:
			return instr.Data, nil
		}
	}
	return value, nil
}

func parseProxyError(data interface{}) *ProxyError {
	if data == nil {
		return &ProxyError{Message: "unknown error"}
	}
	if pe, ok := data.(*ProxyError); ok {
		return pe
	}
	if pe, ok := data.(ProxyError); ok {
		return &pe
	}
	switch v := data.(type) {
	case map[string]interface{}:
		msg, _ := v["message"].(string)
		return &ProxyError{Message: msg}
	case map[interface{}]interface{}:
		if msg, ok := v["message"].(string); ok {
			return &ProxyError{Message: msg}
		}
	}
	return &ProxyError{Message: fmt.Sprint(data)}
}

func toInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func toUint64(value interface{}) uint64 {
	switch v := value.(type) {
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	default:
		return 0
	}
}

func toFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0
	}
}
