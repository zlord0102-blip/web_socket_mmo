# ws-server-go

Go implementation of the local WebSocket bridge used by the extension.

## Purpose

This folder exists so you can benchmark the Go server against the existing Node server in `/Users/Code/MySelf/Kling_Auto_Reg/ws-server`.

The Go server keeps the same:

- WebSocket URL: `ws://localhost:9877`
- Request/response shape
- Supported actions:
  - `generate_cards`
  - `try_stripe_card`
  - `send_telegram`
  - `set_proxy`
  - `gpm_report_done`
  - `gpm_charge_success`
  - `gpm_share_hot_card`
  - `sheet_claim_lock_acquire`
  - `sheet_claim_lock_release`

Run only one server at a time on port `9877`.

## Run

```bash
cd /Users/Code/MySelf/Kling_Auto_Reg/ws-server-go
go run .
```

## Optional env vars

```bash
PORT=9877
HITCHKR_BASE=https://hitchkr.replit.app
HITCHKR_CONNECT_SID=...
TELEGRAM_BOT_TOKEN=...
```

## Switch A/B

Node:

```bash
cd /Users/Code/MySelf/Kling_Auto_Reg/ws-server
npm start
```

Go:

```bash
cd /Users/Code/MySelf/Kling_Auto_Reg/ws-server-go
go run .
```
