# Token Pricing Convention

Polaris stores token balances and market prices as integers in the smallest token unit.

## Unit Rule

- `1` full token = `100` stored units
- `100` stored units = full payout for one winning binary market ticket

This means prices below one token are still stored exactly:

- `0.25` tokens -> `25`
- `0.40` tokens -> `40`
- `1.00` token -> `100`

## Market Meaning

- `user_token_stock.qty` stores a user's spendable token balance in these base units
- `market_transaction.price` stores the current ticket price in these base units
- `qty` in market ticket logic is the number of tickets, not fractional token value
- `user_market_ticket.qty` stores the number of currently held tickets on a market side

## Market Architecture

Polaris treats `market_transaction` as the historical source of truth for market behavior.

- transaction logs store the executed trade history
- ticket holdings store only current ownership
- payout uses current winning tickets at market resolution

This means we do **not** build historical charts from ticket rows.
We build history from the transaction log.

## Ratio-Based Market Logic

Polaris uses a ratio-pool style pricing model, closer to pooled horse-racing tickets than to a traditional order book exchange.

The basic intuition is:

- each side of a binary market accumulates support over time
- the relative support on each side determines the implied current odds
- as more people buy one side, that side becomes more expensive
- as support is removed from a side, that side becomes cheaper

In practical terms:

- `YES pool` is derived from historical YES-side buys and sells
- `NO pool` is derived from historical NO-side buys and sells
- current side price is computed from the ratio between those pools

When a market has no support yet, it opens at:

- `YES price = 50`
- `NO price = 50`

As support shifts, prices move according to the current ratio of support on each side.

## Why This Resembles Horse-Racing Tickets

This design is conceptually similar to pari-mutuel style ticket systems:

- users buy into an outcome
- accumulating support changes the implied value of each side
- price is not supplied by the client as truth
- price is derived from the market's current state

The backend therefore computes execution from transaction history rather than trusting externally supplied prices.

## Trade Interpretation

- `BUY`: the user spends token units to acquire tickets
- `SELL`: the user gives up tickets and receives token units back

Because the market is ratio-based, larger trades can move price while they are being filled.
Polaris therefore computes the fill by simulating ticket acquisition or release against the current ratio state.

This gives each trade:

- a total token cost or proceeds
- an average execution price per ticket

That average execution price is what gets stored in `market_transaction.price`.

## Current Holdings vs History

Polaris separates current inventory from history:

- `market_transaction` = full trade history
- `user_market_ticket` = current open ticket holdings
- `user_token_stock` = current spendable token balance

This separation makes it possible to:

- rebuild chart history from transactions
- inspect current user positions from tickets
- settle markets cleanly using only current winning tickets

## Binary Market Payout

At resolution, each winning ticket pays the full token amount:

- winning ticket payout = `100`
- losing ticket payout = `0`

So if a user holds `n` winning tickets at settlement:

- total payout = `n * 100`

## Why This Convention

- keeps all money math in integers
- avoids floating point rounding issues
- supports sub-1 ticket prices naturally
- makes final binary payouts simple and deterministic
- keeps market history and current holdings conceptually separate
- allows graphing and analytics to come directly from executed transaction logs
