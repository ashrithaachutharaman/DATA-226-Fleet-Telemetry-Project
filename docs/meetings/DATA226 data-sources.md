# Data sources (backlog item B3)

## 511 SF Bay Open Data (all Bay Area agencies)

- Request a token: https://511.org/open-data/token (free; key arrives by email)
- Docs: https://511.org/open-data/transit
- **Rate limit: 60 requests per hour per token.** Higher limits can be requested by emailing 511sfbaydeveloperresources@googlegroups.com (never include the key in the email).

| Feed | URL |
| --- | --- |
| Vehicle positions | `http://api.511.org/transit/vehiclepositions?api_key=KEY&agency=CODE` |
| Trip updates (delays) | `http://api.511.org/transit/tripupdates?api_key=KEY&agency=CODE` |
| Service alerts | `http://api.511.org/transit/servicealerts?api_key=KEY&agency=CODE` |
| Operator list (agency codes) | `http://api.511.org/transit/gtfsoperators?api_key=KEY` |

`agency=RG` returns the whole region in one response.

Agency codes (confirm against the operator list): BART `BA`, VTA `SC`, Muni `SF`, Caltrain `CT`.

## BART direct feeds (no key needed)

| Feed | URL |
| --- | --- |
| Trip updates | `http://api.bart.gov/gtfsrt/tripupdate.aspx` |
| Service alerts | `http://api.bart.gov/gtfsrt/alerts.aspx` |

BART publishes **no vehicle positions feed**. BART data is used for delay propagation only; bus bunching and stalled-vehicle detection use Muni and VTA.

## Polling plan within the rate limit

60 requests/hour = one request per minute per token. Each team member requests their own token.

| Token | Feed | Interval | Requests/hour |
| --- | --- | --- | --- |
| Token A | 511 vehicle positions, `agency=RG` | every 90 s | 40 |
| Token B | 511 trip updates, `agency=RG` | every 90 s | 40 |
| none | BART trip updates (direct) | every 30 s | no 511 limit |
| Token C | spare: testing and the operator list | as needed | |

Benchmarks do not depend on polling speed: the 24 h recording (B14) is replayed through Kafka at 10x to 100x.

## Rules

- Keys go in `.env` only, never in code or Git.
- Each producer logs every request time, so the rate limit can be audited.
