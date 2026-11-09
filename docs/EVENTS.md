# Event format

This is the message format between the Java command service and the Scala processor.

Topic: `ledger-events`. Key: the account id. Value: a JSON object.

Every event has these fields:

| Field | Type | Notes |
|---|---|---|
| `eventId` | UUID | Unique for every event |
| `eventType` | string | One of the types below |
| `accountId` | UUID | The account this event belongs to |
| `version` | number, 1 or more | Position in that account's history, no gaps |
| `occurredAt` | ISO-8601 time in UTC | for example `2026-05-01T09:00:00.123Z` |

Amounts are always whole cents (`amountCents`), never decimals.

| `eventType` | Extra fields |
|---|---|
| `AccountOpened` | `owner`, `currency` (like USD) |
| `FundsDeposited` | `amountCents`, `reference` (can be null) |
| `FundsWithdrawn` | `amountCents`, `reference` (can be null) |
| `TransferSent` | `transferId`, `counterpartyAccountId`, `amountCents` |
| `TransferReceived` | `transferId`, `counterpartyAccountId`, `amountCents` |

A transfer always creates a `TransferSent` on the sender's account and a `TransferReceived`
on the receiver's account with the same `transferId`. Both are saved together.

Example:

```json
{
  "eventId": "8b1f5c3e-4c1e-4b55-9d0e-2a9b6f3f2b11",
  "eventType": "TransferSent",
  "accountId": "0f9a3a8e-6a7e-4c2b-8a8f-3d1c2b4e5f60",
  "version": 4,
  "occurredAt": "2026-05-01T09:05:00.000Z",
  "transferId": "5a2c...",
  "counterpartyAccountId": "7d4e...",
  "amountCents": 25000
}
```

## Changing the format later

- Adding a new optional field is safe. Readers ignore fields they don't know.
- Renaming or removing a field, or changing its type, needs a new event type (for example
  `FundsDepositedV2`). The consumer has to understand both before the producer switches.
- I wrote the JSON code by hand on both sides (`EventCodec.java` and `EventCodec.scala`)
  and tested it both ways, so a format change is always visible in review.
