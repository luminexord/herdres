# Security

The tendwired branch keeps direct Herdr access out of Herdres.

Private state may contain Telegram topic/message ids and bot tokens. Public JSON
from Herdres commands is pruned so it does not expose tokens, socket paths, raw
backend targets, command stdout/stderr, or Telegram ids.

Normal verification:

```sh
HERDRES_TENDWIRE_MODE=source ./herdres.py doctor
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

`direct_herdr_calls` must remain `0`.
