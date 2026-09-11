package compiler

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
)

var (
	activeContexts sync.Map // map[uint64]context.Context
)

func getGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := bytes.Fields(buf[:n])
	if len(fields) > 1 {
		id, _ := strconv.ParseUint(string(fields[1]), 10, 64)
		return id
	}
	return 0
}

// SetCurrentContext registers the active request context for the current goroutine.
func SetCurrentContext(ctx context.Context) uint64 {
	if ctx == nil {
		return 0
	}
	gid := getGoroutineID()
	if gid != 0 {
		activeContexts.Store(gid, ctx)
	}
	return gid
}

// ClearCurrentContext removes the registered request context for the specified goroutine.
func ClearCurrentContext(gid uint64) {
	if gid != 0 {
		activeContexts.Delete(gid)
	}
}

// GetCurrentContext retrieves the active request context for the current goroutine.
func GetCurrentContext() context.Context {
	gid := getGoroutineID()
	if gid != 0 {
		if val, ok := activeContexts.Load(gid); ok {
			if ctx, ok := val.(context.Context); ok {
				return ctx
			}
		}
	}
	return nil
}
