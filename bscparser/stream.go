package bscparser

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type StreamSourceType string

const (
	StreamSourceAdapter StreamSourceType = "adapter"
	StreamSourceGRPC    StreamSourceType = "grpc"
	StreamSourceWS      StreamSourceType = "websocket"
	StreamSourceHTTP    StreamSourceType = "http-json"
	StreamSourceRelay   StreamSourceType = "relay"
)

type StreamEventType string

const (
	StreamEventTransaction StreamEventType = "transaction"
	StreamEventLog         StreamEventType = "log"
	StreamEventBlock       StreamEventType = "block"
	StreamEventMessage     StreamEventType = "message"
)

type StreamOrderMode string

const (
	StreamOrderNone       StreamOrderMode = "none"
	StreamOrderOrdered    StreamOrderMode = "ordered"
	StreamOrderStreaming  StreamOrderMode = "streaming"
	StreamOrderMicroBatch StreamOrderMode = "microbatch"
)

type StreamEvent struct {
	Source            string
	SourceType        StreamSourceType
	EventType         StreamEventType
	Raw               any
	ReceivedAt        time.Time
	Latency           time.Duration
	TxHash            common.Hash
	SignedTransaction string
	BlockNumber       uint64
	BlockHash         common.Hash
	TransactionIndex  uint
	LogIndex          uint
	Address           common.Address
	Topics            []common.Hash
	Data              []byte
	Sequence          uint64
	Timestamp         time.Time
	Metadata          map[string]any
}

type OrderedStreamEventBuffer struct {
	mode            StreamOrderMode
	timeout         time.Duration
	microBatchDelay time.Duration
	currentSlot     uint64
	slots           map[uint64][]StreamEvent
	streamWatermark map[uint64]uint64
	microBatch      []StreamEvent
	microBatchStart time.Time
	lastFlush       time.Time
}

func NewOrderedStreamEventBuffer(mode StreamOrderMode, timeout time.Duration, microBatchDelay time.Duration) *OrderedStreamEventBuffer {
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	if microBatchDelay <= 0 {
		microBatchDelay = time.Millisecond
	}
	return &OrderedStreamEventBuffer{
		mode:            mode,
		timeout:         timeout,
		microBatchDelay: microBatchDelay,
		slots:           make(map[uint64][]StreamEvent),
		streamWatermark: make(map[uint64]uint64),
		lastFlush:       time.Now(),
	}
}

func (b *OrderedStreamEventBuffer) Push(event StreamEvent, emit func(StreamEvent)) {
	event = snapshotStreamEvent(event)
	switch b.mode {
	case StreamOrderNone, "":
		emit(event)
	case StreamOrderOrdered:
		b.pushOrdered(event, emit)
	case StreamOrderStreaming:
		b.pushStreaming(event, emit)
	case StreamOrderMicroBatch:
		b.pushMicroBatch(event, emit)
	default:
		emit(event)
	}
}

func (b *OrderedStreamEventBuffer) FlushDue(emit func(StreamEvent)) {
	now := time.Now()
	if (b.mode == StreamOrderOrdered || b.mode == StreamOrderStreaming) && now.Sub(b.lastFlush) >= b.timeout {
		b.FlushAll(emit)
	}
	if b.mode == StreamOrderMicroBatch && len(b.microBatch) > 0 && now.Sub(b.microBatchStart) >= b.microBatchDelay {
		b.flushMicroBatch(emit)
	}
}

func (b *OrderedStreamEventBuffer) FlushAll(emit func(StreamEvent)) {
	if len(b.slots) > 0 {
		slots := make([]uint64, 0, len(b.slots))
		for slot := range b.slots {
			slots = append(slots, slot)
		}
		sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
		for _, slot := range slots {
			b.flushSlot(slot, emit)
		}
	}
	b.flushMicroBatch(emit)
	b.lastFlush = time.Now()
}

func (b *OrderedStreamEventBuffer) pushOrdered(event StreamEvent, emit func(StreamEvent)) {
	slot := eventSlot(event)
	if slot == 0 {
		emit(event)
		return
	}
	if b.currentSlot > 0 && slot > b.currentSlot {
		b.flushBefore(slot, emit)
	}
	if slot > b.currentSlot {
		b.currentSlot = slot
	}
	b.slots[slot] = append(b.slots[slot], event)
}

func (b *OrderedStreamEventBuffer) pushStreaming(event StreamEvent, emit func(StreamEvent)) {
	slot := event.BlockNumber
	if slot == 0 {
		emit(event)
		return
	}
	if b.currentSlot > 0 && slot < b.currentSlot {
		return
	}
	if b.currentSlot > 0 && slot > b.currentSlot {
		b.flushBefore(slot, emit)
	}
	if slot > b.currentSlot {
		b.currentSlot = slot
	}

	index := streamingEventIndex(event)
	expected := b.streamWatermark[slot]
	if index < expected {
		return
	}
	if index == expected {
		emit(event)
		b.streamWatermark[slot] = b.drainStreaming(slot, expected+1, emit)
		return
	}
	for _, buffered := range b.slots[slot] {
		if streamingEventIndex(buffered) == index {
			return
		}
	}
	b.slots[slot] = append(b.slots[slot], event)
}

func (b *OrderedStreamEventBuffer) drainStreaming(slot uint64, next uint64, emit func(StreamEvent)) uint64 {
	events := b.slots[slot]
	for {
		position := -1
		for i, event := range events {
			if streamingEventIndex(event) == next {
				position = i
				break
			}
		}
		if position < 0 {
			break
		}
		event := events[position]
		events = append(events[:position], events[position+1:]...)
		emit(event)
		next++
	}
	if len(events) == 0 {
		delete(b.slots, slot)
	} else {
		b.slots[slot] = events
	}
	return next
}

func (b *OrderedStreamEventBuffer) pushMicroBatch(event StreamEvent, emit func(StreamEvent)) {
	if len(b.microBatch) == 0 {
		b.microBatchStart = time.Now()
	}
	b.microBatch = append(b.microBatch, event)
	if time.Since(b.microBatchStart) >= b.microBatchDelay {
		b.flushMicroBatch(emit)
	}
}

func (b *OrderedStreamEventBuffer) flushBefore(slot uint64, emit func(StreamEvent)) {
	slots := make([]uint64, 0, len(b.slots))
	for existing := range b.slots {
		if existing < slot {
			slots = append(slots, existing)
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	for _, existing := range slots {
		b.flushSlot(existing, emit)
	}
	for existing := range b.streamWatermark {
		if existing < slot {
			delete(b.streamWatermark, existing)
		}
	}
	b.lastFlush = time.Now()
}

func (b *OrderedStreamEventBuffer) flushSlot(slot uint64, emit func(StreamEvent)) {
	events := b.slots[slot]
	if len(events) == 0 {
		delete(b.slots, slot)
		return
	}
	sortStreamEvents(events)
	for _, event := range events {
		emit(event)
	}
	delete(b.slots, slot)
	if b.mode == StreamOrderStreaming && slot == b.currentSlot {
		next := b.streamWatermark[slot]
		for _, event := range events {
			if index := streamingEventIndex(event) + 1; index > next {
				next = index
			}
		}
		b.streamWatermark[slot] = next
	} else {
		delete(b.streamWatermark, slot)
	}
}

func (b *OrderedStreamEventBuffer) flushMicroBatch(emit func(StreamEvent)) {
	if len(b.microBatch) == 0 {
		return
	}
	events := b.microBatch
	b.microBatch = nil
	b.microBatchStart = time.Time{}
	sortStreamEvents(events)
	for _, event := range events {
		emit(event)
	}
	b.lastFlush = time.Now()
}

func (b *OrderedStreamEventBuffer) flushCheckInterval() time.Duration {
	var interval time.Duration
	switch b.mode {
	case StreamOrderOrdered, StreamOrderStreaming:
		interval = b.timeout
	case StreamOrderMicroBatch:
		interval = b.microBatchDelay
	default:
		return 0
	}
	if interval > time.Millisecond {
		return time.Millisecond
	}
	return interval
}

type StreamSource interface {
	Name() string
	Type() StreamSourceType
	Start(ctx context.Context, emit func(StreamEvent)) error
	Close() error
}

type AdapterStreamSource struct {
	SourceName string
	SourceType StreamSourceType
	Run        func(ctx context.Context, emit func(StreamEvent)) error
	CloseFunc  func() error
}

func (s AdapterStreamSource) Name() string {
	if s.SourceName != "" {
		return s.SourceName
	}
	return "adapter"
}

func (s AdapterStreamSource) Type() StreamSourceType {
	if s.SourceType != "" {
		return s.SourceType
	}
	return StreamSourceAdapter
}

func (s AdapterStreamSource) Start(ctx context.Context, emit func(StreamEvent)) error {
	if s.Run == nil {
		return errors.New("adapter stream source requires Run")
	}
	return s.Run(ctx, emit)
}

func (s AdapterStreamSource) Close() error {
	if s.CloseFunc == nil {
		return nil
	}
	return s.CloseFunc()
}

type StreamEventHandler func(StreamEvent)

type streamHandlerRegistration struct {
	id      uint64
	handler StreamEventHandler
}

type StreamManagerConfig struct {
	OrderMode       StreamOrderMode
	OrderTimeout    time.Duration
	MicroBatchDelay time.Duration
	Sources         []StreamSource
}

type StreamSourceStats struct {
	Name      string
	Type      StreamSourceType
	Connected bool
	Received  uint64
	Emitted   uint64
	LastEvent time.Time
	LastError string
}

type StreamManager struct {
	mu          sync.RWMutex
	sources     []StreamSource
	handlers    []streamHandlerRegistration
	nextHandler uint64
	stats       map[string]*StreamSourceStats
	buffer      *OrderedStreamEventBuffer
	bufferMu    sync.Mutex
	pending     []StreamEvent
	dispatching bool
	sequence    atomic.Uint64
}

type streamSourceRun struct {
	source     StreamSource
	name       string
	sourceType StreamSourceType
	closeOnce  sync.Once
	started    chan struct{}
	closeDone  chan struct{}
	closeErr   error
}

func newStreamSourceRun(source StreamSource) *streamSourceRun {
	return &streamSourceRun{
		source:     source,
		name:       source.Name(),
		sourceType: source.Type(),
		started:    make(chan struct{}),
		closeDone:  make(chan struct{}),
	}
}

func (s *streamSourceRun) close() error {
	s.closeOnce.Do(func() {
		defer close(s.closeDone)
		s.closeErr = s.source.Close()
	})
	<-s.closeDone
	return s.closeErr
}

func (s *streamSourceRun) closeAfterStart() error {
	<-s.started
	return s.close()
}

func closeStreamSourceRuns(sources []*streamSourceRun) {
	var wg sync.WaitGroup
	for _, source := range sources {
		source := source
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = source.closeAfterStart()
		}()
	}
	wg.Wait()
}

func NewStreamManager(config StreamManagerConfig) *StreamManager {
	manager := &StreamManager{
		sources: append([]StreamSource{}, config.Sources...),
		stats:   make(map[string]*StreamSourceStats),
		buffer:  NewOrderedStreamEventBuffer(config.OrderMode, config.OrderTimeout, config.MicroBatchDelay),
	}
	for _, source := range manager.sources {
		manager.ensureStats(source.Name(), source.Type())
	}
	return manager
}

func (m *StreamManager) AddSource(source StreamSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sources = append(m.sources, source)
	m.ensureStatsLocked(source.Name(), source.Type())
}

func (m *StreamManager) OnEvent(handler StreamEventHandler) func() {
	m.mu.Lock()
	m.nextHandler++
	id := m.nextHandler
	m.handlers = append(m.handlers, streamHandlerRegistration{id: id, handler: handler})
	m.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			for index, registration := range m.handlers {
				if registration.id == id {
					m.handlers = append(m.handlers[:index], m.handlers[index+1:]...)
					return
				}
			}
		})
	}
}

func (m *StreamManager) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.RLock()
	sources := append([]StreamSource{}, m.sources...)
	m.mu.RUnlock()
	if len(sources) == 0 {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runs := make([]*streamSourceRun, 0, len(sources))
	for _, source := range sources {
		runs = append(runs, newStreamSourceRun(source))
	}
	sourcesDone := make(chan struct{})
	var closeWatcherWG sync.WaitGroup
	closeWatcherWG.Add(1)
	go func() {
		defer closeWatcherWG.Done()
		select {
		case <-runCtx.Done():
			closeStreamSourceRuns(runs)
		case <-sourcesDone:
		}
	}()

	flushDone := make(chan struct{})
	var flushWG sync.WaitGroup
	if interval := m.buffer.flushCheckInterval(); interval > 0 {
		flushWG.Add(1)
		go func() {
			defer flushWG.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					m.FlushDue()
				case <-runCtx.Done():
					return
				case <-flushDone:
					return
				}
			}
		}()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(runs))
	for _, source := range runs {
		source := source
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.setConnected(source.name, source.sourceType, true)
			defer m.setConnected(source.name, source.sourceType, false)
			defer source.close()
			close(source.started)
			var startErr error
			if runCtx.Err() == nil {
				startErr = source.source.Start(runCtx, func(event StreamEvent) {
					if event.Source == "" {
						event.Source = source.name
					}
					if event.SourceType == "" {
						event.SourceType = source.sourceType
					}
					m.Emit(event)
				})
			}

			runErr := startErr
			if runCtx.Err() != nil || errors.Is(runErr, context.Canceled) {
				runErr = nil
			}
			if runErr != nil {
				cancel()
			}
			closeErr := source.close()
			if closeErr != nil {
				if runErr == nil {
					runErr = closeErr
				} else {
					runErr = errors.Join(runErr, closeErr)
				}
			}
			if runErr != nil {
				m.setError(source.name, source.sourceType, runErr)
				errCh <- runErr
				cancel()
			}
		}()
	}
	wg.Wait()
	close(sourcesDone)
	closeWatcherWG.Wait()
	close(flushDone)
	flushWG.Wait()
	close(errCh)
	m.Flush()

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}
	return nil
}

func (m *StreamManager) Emit(event StreamEvent) {
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = time.Now()
	}
	if event.Sequence == 0 {
		event.Sequence = m.sequence.Add(1)
	}
	m.incrementReceived(event.Source, event.SourceType)
	m.updateBuffer(func(emit func(StreamEvent)) {
		m.buffer.Push(event, emit)
		m.buffer.FlushDue(emit)
	})
}

func (m *StreamManager) FlushDue() {
	m.updateBuffer(m.buffer.FlushDue)
}

func (m *StreamManager) Flush() {
	m.updateBuffer(m.buffer.FlushAll)
}

func (m *StreamManager) updateBuffer(operation func(func(StreamEvent))) {
	m.bufferMu.Lock()
	operation(func(event StreamEvent) {
		m.pending = append(m.pending, event)
	})
	shouldDispatch := len(m.pending) > 0 && !m.dispatching
	if shouldDispatch {
		m.dispatching = true
	}
	m.bufferMu.Unlock()

	if shouldDispatch {
		m.drainPending()
	}
}

func (m *StreamManager) drainPending() {
	defer func() {
		if recovered := recover(); recovered != nil {
			m.bufferMu.Lock()
			m.dispatching = false
			m.bufferMu.Unlock()
			panic(recovered)
		}
	}()

	for {
		m.bufferMu.Lock()
		if len(m.pending) == 0 {
			m.dispatching = false
			m.bufferMu.Unlock()
			return
		}
		event := m.pending[0]
		m.pending[0] = StreamEvent{}
		m.pending = m.pending[1:]
		if len(m.pending) == 0 {
			m.pending = nil
		}
		m.bufferMu.Unlock()

		m.emitToHandlers(event)
	}
}

func (m *StreamManager) Stats() []StreamSourceStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]StreamSourceStats, 0, len(m.stats))
	for _, value := range m.stats {
		out = append(out, *value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (m *StreamManager) emitToHandlers(event StreamEvent) {
	m.incrementEmitted(event.Source, event.SourceType, event.ReceivedAt)
	m.mu.RLock()
	handlers := make([]StreamEventHandler, 0, len(m.handlers))
	for _, registration := range m.handlers {
		handlers = append(handlers, registration.handler)
	}
	m.mu.RUnlock()
	for _, handler := range handlers {
		handler(event)
	}
}

func (m *StreamManager) ensureStats(name string, sourceType StreamSourceType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureStatsLocked(name, sourceType)
}

func (m *StreamManager) ensureStatsLocked(name string, sourceType StreamSourceType) string {
	if name == "" {
		name = "unknown"
	}
	if stats, ok := m.stats[name]; !ok {
		m.stats[name] = &StreamSourceStats{Name: name, Type: sourceType}
	} else if stats.Type == "" {
		stats.Type = sourceType
	}
	return name
}

func (m *StreamManager) setConnected(name string, sourceType StreamSourceType, connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = m.ensureStatsLocked(name, sourceType)
	m.stats[name].Connected = connected
}

func (m *StreamManager) setError(name string, sourceType StreamSourceType, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = m.ensureStatsLocked(name, sourceType)
	m.stats[name].LastError = err.Error()
}

func (m *StreamManager) incrementReceived(name string, sourceType StreamSourceType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = m.ensureStatsLocked(name, sourceType)
	m.stats[name].Received++
}

func (m *StreamManager) incrementEmitted(name string, sourceType StreamSourceType, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = m.ensureStatsLocked(name, sourceType)
	m.stats[name].Emitted++
	m.stats[name].LastEvent = at
}

type JSONValueDecoder struct {
	buffer []byte
}

func (d *JSONValueDecoder) Push(chunk []byte) ([]json.RawMessage, error) {
	if len(chunk) > 0 {
		d.buffer = append(d.buffer, chunk...)
	}
	d.buffer = trimLeftSpace(d.buffer)
	if len(d.buffer) == 0 {
		d.buffer = nil
		return nil, nil
	}

	var values []json.RawMessage
	for len(d.buffer) > 0 {
		reader := bytes.NewReader(d.buffer)
		decoder := json.NewDecoder(reader)
		var raw json.RawMessage
		err := decoder.Decode(&raw)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return values, nil
			}
			return values, err
		}
		values = append(values, cloneRawMessage(raw))
		consumed := int(decoder.InputOffset())
		if consumed <= 0 || consumed > len(d.buffer) {
			d.buffer = nil
			return values, nil
		}
		d.buffer = trimLeftSpace(d.buffer[consumed:])
		if len(d.buffer) == 0 {
			d.buffer = nil
		}
	}
	return values, nil
}

func (d *JSONValueDecoder) Buffered() int {
	return len(d.buffer)
}

func eventSlot(event StreamEvent) uint64 {
	if event.BlockNumber > 0 {
		return event.BlockNumber
	}
	return event.Sequence
}

func sortStreamEvents(events []StreamEvent) {
	sort.SliceStable(events, func(i, j int) bool {
		left := events[i]
		right := events[j]
		if left.BlockNumber != right.BlockNumber {
			return left.BlockNumber < right.BlockNumber
		}
		if left.TransactionIndex != right.TransactionIndex {
			return left.TransactionIndex < right.TransactionIndex
		}
		if left.LogIndex != right.LogIndex {
			return left.LogIndex < right.LogIndex
		}
		if left.Sequence != right.Sequence {
			return left.Sequence < right.Sequence
		}
		return left.ReceivedAt.Before(right.ReceivedAt)
	})
}

func trimLeftSpace(value []byte) []byte {
	return bytes.TrimLeftFunc(value, func(r rune) bool {
		return r == ' ' || r == '\n' || r == '\r' || r == '\t'
	})
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	out := make(json.RawMessage, len(value))
	copy(out, value)
	return out
}

// snapshotStreamEvent copies supported mutable containers; custom Raw and Metadata values retain references.
func snapshotStreamEvent(event StreamEvent) StreamEvent {
	event.Raw = snapshotStreamRawValue(event.Raw)
	if event.Topics != nil {
		event.Topics = append([]common.Hash{}, event.Topics...)
	}
	if event.Data != nil {
		event.Data = append([]byte{}, event.Data...)
	}
	if event.Metadata != nil {
		event.Metadata = newStreamMetadataSnapshotter().cloneMap(event.Metadata)
	}
	return event
}

func snapshotStreamRawValue(value any) any {
	switch value := value.(type) {
	case json.RawMessage:
		if value == nil {
			return json.RawMessage(nil)
		}
		return cloneRawMessage(value)
	case []byte:
		if value == nil {
			return []byte(nil)
		}
		return append([]byte{}, value...)
	default:
		return value
	}
}

type streamMetadataSliceKey struct {
	pointer  uintptr
	length   int
	capacity int
}

type streamMetadataSnapshotter struct {
	maps   map[uintptr]map[string]any
	slices map[streamMetadataSliceKey][]any
}

func newStreamMetadataSnapshotter() *streamMetadataSnapshotter {
	return &streamMetadataSnapshotter{
		maps:   make(map[uintptr]map[string]any),
		slices: make(map[streamMetadataSliceKey][]any),
	}
}

// cloneValue copies the mutable container types produced by encoding/json.
func (s *streamMetadataSnapshotter) cloneValue(value any) any {
	switch value := value.(type) {
	case json.RawMessage:
		if value == nil {
			return json.RawMessage(nil)
		}
		return cloneRawMessage(value)
	case []byte:
		if value == nil {
			return []byte(nil)
		}
		return append([]byte{}, value...)
	case map[string]any:
		return s.cloneMap(value)
	case []any:
		return s.cloneSlice(value)
	default:
		return value
	}
}

func (s *streamMetadataSnapshotter) cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	pointer := reflect.ValueOf(value).Pointer()
	if cloned, ok := s.maps[pointer]; ok {
		return cloned
	}
	cloned := make(map[string]any, len(value))
	s.maps[pointer] = cloned
	for key, item := range value {
		cloned[key] = s.cloneValue(item)
	}
	return cloned
}

func (s *streamMetadataSnapshotter) cloneSlice(value []any) []any {
	if value == nil {
		return nil
	}
	key := streamMetadataSliceKey{
		pointer:  reflect.ValueOf(value).Pointer(),
		length:   len(value),
		capacity: cap(value),
	}
	if cloned, ok := s.slices[key]; ok {
		return cloned
	}
	cloned := make([]any, len(value))
	s.slices[key] = cloned
	for i, item := range value {
		cloned[i] = s.cloneValue(item)
	}
	return cloned
}

func streamingEventIndex(event StreamEvent) uint64 {
	if event.EventType == StreamEventLog {
		return uint64(event.LogIndex)
	}
	return uint64(event.TransactionIndex)
}
