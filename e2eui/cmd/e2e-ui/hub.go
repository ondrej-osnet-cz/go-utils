package main

import (
	"encoding/json"
	"sync"
)

// event is what the browser receives. Type is run-start, test (a go test
// event), exchange (a recorded HTTP call) or run-end.
type event struct {
	Type     string          `json:"type"`
	Run      int             `json:"run,omitempty"`
	Packages []string        `json:"packages,omitempty"`
	Package  string          `json:"package,omitempty"`
	Test     string          `json:"test,omitempty"`
	Tests    []string        `json:"tests,omitempty"` // run-start: the selected tests, none for all
	Action   string          `json:"action,omitempty"`
	Elapsed  *float64        `json:"elapsed,omitempty"`
	Output   string          `json:"output,omitempty"`
	Exchange json.RawMessage `json:"exchange,omitempty"`
	Error    string          `json:"error,omitempty"`
	Stopped  bool            `json:"stopped,omitempty"`
}

// hub keeps the event history and fans new events out to subscribers. A new
// subscriber first gets the history, so a reloaded page shows the last runs.
type hub struct {
	limit int

	mu      sync.Mutex
	history [][]byte
	subs    map[chan []byte]struct{}
}

func newHub(limit int) *hub {
	return &hub{limit: limit, subs: map[chan []byte]struct{}{}}
}

func (h *hub) publish(ev event) {
	b, err := json.Marshal(ev)
	if err != nil {
		panic("marshal event: " + err.Error()) // the event type always marshals
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.history = append(h.history, b)
	if over := len(h.history) - h.limit; over > 0 {
		h.history = append([][]byte(nil), h.history[over:]...)
	}
	for ch := range h.subs {
		select {
		case ch <- b:
		default: // too slow: drop it, the browser reconnects and replays the history
			delete(h.subs, ch)
			close(ch)
		}
	}
}

// subscribe returns the history so far and a channel with the events after it.
// The channel is closed when the subscriber falls behind or cancel is called.
func (h *hub) subscribe() (history [][]byte, events <-chan []byte, cancel func()) {
	ch := make(chan []byte, 1024)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[ch] = struct{}{}
	return append([][]byte(nil), h.history...), ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
}
