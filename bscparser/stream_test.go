package bscparser

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestOrderedStreamEventBufferFlushesByBlockAndIndex(t *testing.T) {
	buffer := NewOrderedStreamEventBuffer(StreamOrderOrdered, time.Second, time.Millisecond)
	var emitted []StreamEvent
	emit := func(event StreamEvent) {
		emitted = append(emitted, event)
	}

	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 1, LogIndex: 1, Sequence: 2}, emit)
	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 0, LogIndex: 3, Sequence: 1}, emit)
	if len(emitted) != 0 {
		t.Fatalf("ordered buffer emitted before next block: %d", len(emitted))
	}

	buffer.Push(StreamEvent{BlockNumber: 11, TransactionIndex: 0, LogIndex: 0, Sequence: 3}, emit)
	buffer.FlushAll(emit)

	got := []uint64{emitted[0].Sequence, emitted[1].Sequence, emitted[2].Sequence}
	if !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected order: %#v", got)
	}
}

func TestOrderedStreamEventBufferMicroBatchSorts(t *testing.T) {
	buffer := NewOrderedStreamEventBuffer(StreamOrderMicroBatch, time.Second, time.Hour)
	var emitted []StreamEvent
	emit := func(event StreamEvent) {
		emitted = append(emitted, event)
	}

	buffer.Push(StreamEvent{BlockNumber: 3, TransactionIndex: 1, Sequence: 2}, emit)
	buffer.Push(StreamEvent{BlockNumber: 3, TransactionIndex: 0, Sequence: 1}, emit)
	if len(emitted) != 0 {
		t.Fatalf("microbatch emitted before flush: %d", len(emitted))
	}
	buffer.FlushAll(emit)

	got := []uint64{emitted[0].Sequence, emitted[1].Sequence}
	if !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("unexpected microbatch order: %#v", got)
	}
}

func TestOrderedStreamEventBufferStreamingDrainsContinuousIndexes(t *testing.T) {
	buffer := NewOrderedStreamEventBuffer(StreamOrderStreaming, time.Second, time.Millisecond)
	var emitted []uint64
	emit := func(event StreamEvent) {
		emitted = append(emitted, event.Sequence)
	}

	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 2, Sequence: 3}, emit)
	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 0, Sequence: 1}, emit)
	if !reflect.DeepEqual(emitted, []uint64{1}) {
		t.Fatalf("streaming buffer did not emit index zero immediately: %#v", emitted)
	}
	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 1, Sequence: 2}, emit)
	if !reflect.DeepEqual(emitted, []uint64{1, 2, 3}) {
		t.Fatalf("streaming buffer did not drain continuous indexes: %#v", emitted)
	}

	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 1, Sequence: 4}, emit)
	buffer.Push(StreamEvent{BlockNumber: 11, TransactionIndex: 0, Sequence: 5}, emit)
	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 3, Sequence: 6}, emit)
	buffer.FlushAll(emit)
	if !reflect.DeepEqual(emitted, []uint64{1, 2, 3, 5}) {
		t.Fatalf("streaming buffer replayed a duplicate or late event: %#v", emitted)
	}
}

func TestOrderedStreamEventBufferStreamingPreservesWatermarkAfterFlush(t *testing.T) {
	buffer := NewOrderedStreamEventBuffer(StreamOrderStreaming, time.Second, time.Millisecond)
	var emitted []uint64
	emit := func(event StreamEvent) {
		emitted = append(emitted, event.Sequence)
	}

	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 2, Sequence: 1}, emit)
	buffer.FlushAll(emit)
	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 0, Sequence: 2}, emit)
	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 2, Sequence: 3}, emit)
	buffer.Push(StreamEvent{BlockNumber: 10, TransactionIndex: 3, Sequence: 4}, emit)
	buffer.FlushAll(emit)
	if !reflect.DeepEqual(emitted, []uint64{1, 4}) {
		t.Fatalf("streaming watermark did not survive a forced flush: %#v", emitted)
	}
}

func TestOrderedStreamEventBufferStreamingUsesLogIndex(t *testing.T) {
	buffer := NewOrderedStreamEventBuffer(StreamOrderStreaming, time.Second, time.Millisecond)
	var emitted []uint64
	emit := func(event StreamEvent) {
		emitted = append(emitted, event.Sequence)
	}

	buffer.Push(StreamEvent{EventType: StreamEventLog, BlockNumber: 10, TransactionIndex: 0, LogIndex: 1, Sequence: 2}, emit)
	buffer.Push(StreamEvent{EventType: StreamEventLog, BlockNumber: 10, TransactionIndex: 0, LogIndex: 0, Sequence: 1}, emit)
	if !reflect.DeepEqual(emitted, []uint64{1, 2}) {
		t.Fatalf("streaming buffer did not order multiple logs in one transaction: %#v", emitted)
	}
}

func TestOrderedStreamEventBufferSnapshotsMutableFields(t *testing.T) {
	buffer := NewOrderedStreamEventBuffer(StreamOrderOrdered, time.Second, time.Millisecond)
	topics := []common.Hash{common.HexToHash("0x01")}
	data := []byte{1, 2, 3}
	raw := json.RawMessage(`{"source":"original"}`)
	nestedBytes := []byte{4, 5, 6}
	nestedRaw := json.RawMessage(`{"status":"original"}`)
	nestedMap := map[string]any{"provider": "original"}
	metadata := map[string]any{
		"nested": map[string]any{
			"bytes": nestedBytes,
			"raw":   nestedRaw,
			"items": []any{nestedMap},
		},
	}
	metadata["self"] = metadata
	var emitted StreamEvent

	buffer.Push(StreamEvent{
		BlockNumber: 1,
		Raw:         raw,
		Topics:      topics,
		Data:        data,
		Metadata:    metadata,
	}, func(event StreamEvent) {
		emitted = event
	})
	topics[0] = common.HexToHash("0x02")
	data[0] = 9
	raw[11] = 'm'
	nestedBytes[0] = 9
	nestedRaw[11] = 'm'
	nestedMap["provider"] = "mutated"
	metadata["addedAfterPush"] = true
	buffer.FlushAll(func(event StreamEvent) {
		emitted = event
	})

	nested := emitted.Metadata["nested"].(map[string]any)
	items := nested["items"].([]any)
	self := emitted.Metadata["self"].(map[string]any)
	self["copiedCycle"] = true
	if emitted.Topics[0] != common.HexToHash("0x01") ||
		emitted.Data[0] != 1 ||
		string(emitted.Raw.(json.RawMessage)) != `{"source":"original"}` ||
		nested["bytes"].([]byte)[0] != 4 ||
		string(nested["raw"].(json.RawMessage)) != `{"status":"original"}` ||
		items[0].(map[string]any)["provider"] != "original" ||
		emitted.Metadata["copiedCycle"] != true {
		t.Fatal("buffered event was mutated through caller-owned memory")
	}
	if _, exists := self["addedAfterPush"]; exists {
		t.Fatal("metadata cycle still referenced the caller-owned map")
	}
}

func TestJSONValueDecoderHandlesPartialAndConcatenatedValues(t *testing.T) {
	var decoder JSONValueDecoder
	values, err := decoder.Push([]byte(`{"a":`))
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 0 {
		t.Fatalf("expected partial JSON to stay buffered, got %d values", len(values))
	}

	values, err = decoder.Push([]byte(`1}{"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("expected two JSON values, got %d", len(values))
	}
	var first map[string]int
	if err := json.Unmarshal(values[0], &first); err != nil {
		t.Fatal(err)
	}
	if first["a"] != 1 {
		t.Fatalf("unexpected first value: %#v", first)
	}
	if decoder.Buffered() != 0 {
		t.Fatalf("expected empty decoder buffer, got %d", decoder.Buffered())
	}
	if decoder.buffer != nil {
		t.Fatal("expected the decoder to release its consumed buffer")
	}
}

func TestStreamManagerRunsAdapterSource(t *testing.T) {
	manager := NewStreamManager(StreamManagerConfig{
		OrderMode: StreamOrderOrdered,
		Sources: []StreamSource{
			AdapterStreamSource{
				SourceName: "grpc-provider",
				SourceType: StreamSourceGRPC,
				Run: func(_ context.Context, emit func(StreamEvent)) error {
					emit(StreamEvent{
						EventType:        StreamEventLog,
						BlockNumber:      100,
						TransactionIndex: 2,
						LogIndex:         0,
						Address:          common.HexToAddress("0x1111111111111111111111111111111111111111"),
					})
					emit(StreamEvent{
						EventType:        StreamEventLog,
						BlockNumber:      100,
						TransactionIndex: 1,
						LogIndex:         0,
						Address:          common.HexToAddress("0x2222222222222222222222222222222222222222"),
					})
					return nil
				},
			},
		},
	})

	var emitted []StreamEvent
	manager.OnEvent(func(event StreamEvent) {
		emitted = append(emitted, event)
	})
	if err := manager.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(emitted) != 2 {
		t.Fatalf("expected two events, got %d", len(emitted))
	}
	if emitted[0].TransactionIndex != 1 || emitted[1].TransactionIndex != 2 {
		t.Fatalf("events were not sorted: %#v", emitted)
	}
	if emitted[0].Source != "grpc-provider" || emitted[0].SourceType != StreamSourceGRPC {
		t.Fatalf("source metadata was not filled: %#v", emitted[0])
	}

	stats := manager.Stats()
	if len(stats) != 1 || stats[0].Received != 2 || stats[0].Emitted != 2 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func TestStreamManagerConcurrentSources(t *testing.T) {
	const (
		sourceCount     = 8
		eventsPerSource = 250
	)
	start := make(chan struct{})
	sources := make([]StreamSource, 0, sourceCount)
	for i := 0; i < sourceCount; i++ {
		sources = append(sources, AdapterStreamSource{
			SourceName: "concurrent-" + string(rune('a'+i)),
			Run: func(_ context.Context, emit func(StreamEvent)) error {
				<-start
				for j := 0; j < eventsPerSource; j++ {
					emit(StreamEvent{EventType: StreamEventLog, BlockNumber: 100})
				}
				return nil
			},
		})
	}

	manager := NewStreamManager(StreamManagerConfig{
		OrderMode: StreamOrderOrdered,
		Sources:   sources,
	})
	var emitted atomic.Uint64
	manager.OnEvent(func(StreamEvent) {
		emitted.Add(1)
	})

	done := make(chan error, 1)
	go func() {
		done <- manager.Run(context.Background())
	}()
	close(start)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, want := emitted.Load(), uint64(sourceCount*eventsPerSource); got != want {
		t.Fatalf("unexpected emitted event count: got %d want %d", got, want)
	}
}

func TestStreamManagerAutomaticallyFlushesMicroBatch(t *testing.T) {
	release := make(chan struct{})
	manager := NewStreamManager(StreamManagerConfig{
		OrderMode:       StreamOrderMicroBatch,
		MicroBatchDelay: 10 * time.Millisecond,
		Sources: []StreamSource{
			AdapterStreamSource{
				Run: func(_ context.Context, emit func(StreamEvent)) error {
					emit(StreamEvent{EventType: StreamEventLog, BlockNumber: 100})
					<-release
					return nil
				},
			},
		},
	})
	emitted := make(chan StreamEvent, 1)
	manager.OnEvent(func(event StreamEvent) {
		emitted <- event
	})
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(context.Background())
	}()

	var received bool
	select {
	case <-emitted:
		received = true
	case <-time.After(time.Second):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !received {
		t.Fatal("micro-batch was not flushed while the source was still running")
	}
}

func TestStreamManagerCancelsOtherSourcesOnError(t *testing.T) {
	sourceErr := errors.New("source failed")
	blockedStarted := make(chan struct{})
	blockedStopped := make(chan struct{})
	manager := NewStreamManager(StreamManagerConfig{
		Sources: []StreamSource{
			AdapterStreamSource{
				SourceName: "blocked",
				Run: func(ctx context.Context, _ func(StreamEvent)) error {
					close(blockedStarted)
					<-ctx.Done()
					close(blockedStopped)
					return ctx.Err()
				},
			},
			AdapterStreamSource{
				SourceName: "failing",
				Run: func(_ context.Context, _ func(StreamEvent)) error {
					<-blockedStarted
					return sourceErr
				},
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, sourceErr) {
			t.Fatalf("unexpected run error: %v", err)
		}
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("Run did not cancel the remaining source after an error")
	}
	select {
	case <-blockedStopped:
	default:
		t.Fatal("blocked source did not observe cancellation")
	}
}

func TestStreamManagerClosesSourceToUnblockCanceledStart(t *testing.T) {
	started := make(chan struct{})
	released := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(released)
		})
	}
	var closeCalls atomic.Uint64
	manager := NewStreamManager(StreamManagerConfig{
		Sources: []StreamSource{
			AdapterStreamSource{
				SourceName: "close-only",
				Run: func(_ context.Context, _ func(StreamEvent)) error {
					close(started)
					<-released
					return net.ErrClosed
				},
				CloseFunc: func() error {
					closeCalls.Add(1)
					release()
					return nil
				},
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx)
	}()
	<-started
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected Run error: %v", err)
		}
	case <-time.After(time.Second):
		release()
		<-done
		t.Fatal("Run did not close a source whose Start ignored context cancellation")
	}
	if calls := closeCalls.Load(); calls != 1 {
		t.Fatalf("unexpected source Close call count: %d", calls)
	}
	stats := manager.Stats()
	if len(stats) != 1 || stats[0].LastError != "" {
		t.Fatalf("close-induced Start error was recorded in source stats: %#v", stats)
	}
}

func TestStreamManagerSkipsStartWhenContextAlreadyCanceled(t *testing.T) {
	var startCalls atomic.Uint64
	var closeCalls atomic.Uint64
	manager := NewStreamManager(StreamManagerConfig{
		Sources: []StreamSource{
			AdapterStreamSource{
				Run: func(_ context.Context, _ func(StreamEvent)) error {
					startCalls.Add(1)
					return nil
				},
				CloseFunc: func() error {
					closeCalls.Add(1)
					return nil
				},
			},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := manager.Run(ctx); err != nil {
		t.Fatalf("unexpected Run error: %v", err)
	}
	if calls := startCalls.Load(); calls != 0 {
		t.Fatalf("Start was called with an already canceled context: %d", calls)
	}
	if calls := closeCalls.Load(); calls != 1 {
		t.Fatalf("unexpected source Close call count: %d", calls)
	}
}

func TestStreamManagerSnapshotsMutableEventFields(t *testing.T) {
	manager := NewStreamManager(StreamManagerConfig{OrderMode: StreamOrderOrdered})
	topics := []common.Hash{common.HexToHash("0x01")}
	data := []byte{1, 2, 3}
	metadata := map[string]any{"provider": "original"}
	var emitted StreamEvent
	manager.OnEvent(func(event StreamEvent) {
		emitted = event
	})

	manager.Emit(StreamEvent{
		BlockNumber: 1,
		Topics:      topics,
		Data:        data,
		Metadata:    metadata,
	})
	topics[0] = common.HexToHash("0x02")
	data[0] = 9
	metadata["provider"] = "mutated"
	manager.Flush()

	if emitted.Topics[0] != common.HexToHash("0x01") || emitted.Data[0] != 1 || emitted.Metadata["provider"] != "original" {
		t.Fatalf("manager event was mutated through caller-owned memory: %#v", emitted)
	}
}

func TestStreamManagerSnapshotPreventsSourceBufferReuseRace(t *testing.T) {
	manager := NewStreamManager(StreamManagerConfig{OrderMode: StreamOrderOrdered})
	data := []byte{1}
	handlerReady := make(chan struct{})
	begin := make(chan struct{})
	handlerDone := make(chan byte, 1)
	manager.OnEvent(func(event StreamEvent) {
		close(handlerReady)
		<-begin
		var sum byte
		for i := 0; i < 100000; i++ {
			sum ^= event.Data[0]
		}
		handlerDone <- sum
	})
	manager.Emit(StreamEvent{BlockNumber: 1, Data: data})

	flushDone := make(chan struct{})
	go func() {
		manager.Flush()
		close(flushDone)
	}()
	<-handlerReady
	close(begin)
	for i := 0; i < 100000; i++ {
		data[0] = byte(i)
	}
	<-handlerDone
	<-flushDone
}

func TestStreamManagerReturnsSourceCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	manager := NewStreamManager(StreamManagerConfig{
		Sources: []StreamSource{
			AdapterStreamSource{
				Run: func(_ context.Context, _ func(StreamEvent)) error {
					return nil
				},
				CloseFunc: func() error {
					return closeErr
				},
			},
		},
	})

	if err := manager.Run(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("Run did not return the source close error: %v", err)
	}
	stats := manager.Stats()
	if len(stats) != 1 || stats[0].LastError != closeErr.Error() {
		t.Fatalf("close error was not recorded in source stats: %#v", stats)
	}
}

func TestStreamManagerHandlerCanEmit(t *testing.T) {
	manager := NewStreamManager(StreamManagerConfig{})
	var sequences []uint64
	manager.OnEvent(func(event StreamEvent) {
		sequences = append(sequences, event.Sequence)
		if len(sequences) == 1 {
			manager.Emit(StreamEvent{EventType: StreamEventMessage})
		}
	})

	manager.Emit(StreamEvent{EventType: StreamEventMessage})
	if !reflect.DeepEqual(sequences, []uint64{1, 2}) {
		t.Fatalf("unexpected reentrant delivery: %#v", sequences)
	}
	stats := manager.Stats()
	if len(stats) != 1 || stats[0].Name != "unknown" || stats[0].Received != 2 || stats[0].Emitted != 2 {
		t.Fatalf("unexpected stats for events without a source: %#v", stats)
	}
}

func TestStreamManagerUnsubscribeRemovesHandler(t *testing.T) {
	manager := NewStreamManager(StreamManagerConfig{})
	called := 0
	for range 1_000 {
		unsubscribe := manager.OnEvent(func(StreamEvent) { called++ })
		unsubscribe()
		unsubscribe()
	}

	if len(manager.handlers) != 0 {
		t.Fatalf("unsubscribed handlers were retained: %d", len(manager.handlers))
	}
	manager.Emit(StreamEvent{EventType: StreamEventMessage})
	if called != 0 {
		t.Fatalf("unsubscribed handler was called %d times", called)
	}
}
