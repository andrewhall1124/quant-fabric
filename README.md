# Quant Fabric Repository

## Getting Started

### Install UV
MacOS
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows
```bash
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Sync Denpendencies
```bash
uv sync
```

### Activate Environment
MacOS
```bash
source .venv/bin/activate
```

Windows
```bash
.venv/Scripts/activate
```

### Add Module to Python Path
I prefer editing rc files in vs code but you can also use vim or nano
Note: if you don'
```bash
code ~/.zshrc
```

Add this line to the bottom:
```bash
# Custom python path for quant-fabric project
export PYTHONPATH=/path/to/project/quant-fabric/:$PYTHONPATH
```

Make sure to kill all running terminals and reopen for the environment to reset.

### Alpaca API Keys
You can get Alpaca API keys [here](https://alpaca.markets/learn/connect-to-alpaca-api)

Then create a `.env` file in the root directory like this:

```
ALPACA_API_KEY=<api-key>
ALPACA_API_SECRET_KEY=<api-secret-key>
```

Make sure to not put quotes around your api keys
