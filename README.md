# yahoo-finance-mcp

MCP server for Thai SET/MAI stock data via Yahoo Finance API.

## Tools

| Tool | Description |
|------|-------------|
| `scan_uptrend_pullback` | Scan all SET/MAI stocks for 3-day uptrend pullback pattern (price > EMA200 & EMA90, 3 consecutive lower closes, today price ≥ yesterday & volume > yesterday) |
| `get_stock_quote` | Get current price, EMA200, EMA90, volume, % change for a single stock |
| `get_stock_history` | Get daily close + volume history for N days with EMA200/EMA90 |
| `get_income_statement` | Quarterly or annual income statement (revenue, gross profit, operating income, net income, EBITDA, EPS) |
| `get_balance_sheet` | Quarterly or annual balance sheet (assets, liabilities, equity, D/E ratio) |
| `get_cash_flow` | Quarterly or annual cash flow (operating, investing, financing CF, FCF, capex) |
| `list_stocks` | List all stocks in local DB (SET/mai) |

## Requirements

- Python 3.10+
- `mcp` library
- `yfinance` library
- SQLite DB at `C:/work/AI/SET/set_stocks.db` (symbols from `listedCompanies_th.csv`)

## Install

```bash
pip install -r requirements.txt
```

## Claude Desktop / Claude Code Config

**Claude Desktop** (`claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "yahoo-finance": {
      "command": "python",
      "args": ["C:/work/AI/MCP/yahoo-finance-mcp/src/server.py"]
    }
  }
}
```

**Claude Code**:
```bash
claude mcp add yahoo-finance python "C:/work/AI/MCP/yahoo-finance-mcp/src/server.py"
```

## Data Sources

- **Price / EMA / Volume**: Yahoo Finance chart API (`query2.finance.yahoo.com/v8/finance/chart`)
- **Financials**: Yahoo Finance via `yfinance` library
- **Symbol list**: Local SQLite DB (931 symbols, SET + mai)

## Notes

- Thai stocks use `.BK` suffix on Yahoo Finance (e.g. `PTT.BK`, `AOT.BK`)
- Quarterly financials: up to ~7 quarters history
- Annual financials: up to ~5 years history
- EMA is calculated from raw 2-year daily data (not from TradingView)
