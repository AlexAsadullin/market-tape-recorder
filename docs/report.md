# Proving completeness: evidence for the Market Tape Recorder quality layer

The present report gives the evidence for the method choices of the Market Tape Recorder, which is project I1
of the Tradeq platform. All numbers come from the command make report, which runs the package experiments with
the seed 20261022 and writes the files tables.md, results.json, detection.csv, rotation.csv and regions.csv under
the directory $TRADEQ_DATA_DIR/market-tape-recorder/reports/. The file config/report.yaml holds every parameter
of the experiments, so a rerun of make report regenerates every number that the present report cites.

## 1. Question

The present project asks which method can prove, for each instrument and day, that a recorded tape of market
data is complete, and which reconnect policy keeps the tape complete when a connection is replaced. The
specification of the platform, section I1, states that no open and self-hosted recorder proves completeness per
day, and it states that Tardis.dev documents a gap of 300 to 3000 ms at each daily re-subscription, so we can
conclude that the comparison must cover both the detection of losses and the avoidance of losses. The study
therefore asks three questions. The first question is whether the quality certificate detects five kinds of
injected faults more reliably than a simple timing monitor and than the cryptofeed feed handler. The second
question is how many messages a planned reconnect loses under the overlapping policy and under the naive
policy. The third question is how much a merge of two capture regions recovers when each region loses messages
on its own.

## 2. Data

The present study uses synthetic data only, because the experiments must know the exact position of every
fault, so the results describe how the software behaves and are not evidence about real market data. The
module experiments/synthetic.py generates an order book that has 20 price levels per side around a mid price of
67000 with a tick of 0.1, and every message changes one to three levels, which means a size update, a new level
next to the touch or a deletion. The generator writes the messages in the real wire formats of Binance spot,
Bybit and OKX, and it numbers them with the update ids that each venue documents. A Binance message carries the
fields U and u, where U is one more than the u of the message before it, while a Bybit message raises u by one
and an OKX message carries prevSeqId, which equals the seqId of the message before it. The generator inserts a
full snapshot every 100 messages for Binance, which arrives as a response of the REST endpoint for depth, and
every 250 messages for Bybit and OKX, which arrive inside the stream.

The three streams differ in their timing, because real venues differ in the same way. The Binance stream has
600 messages at a fixed interval of 100 ms, the Bybit stream has 1500 messages at a fixed interval of 20 ms,
and the OKX stream has 2000 messages whose intervals follow an exponential distribution with a mean of 10 ms.
Each frame arrives 3 ms after its exchange timestamp plus a jitter that is uniform between 0 and 1 ms. Each run
of the detection experiment uses a stream that the run number seeds, so the 200 runs per venue and fault see 200
different books.

## 3. Methods

The quality certificate is the method that the present project proposes. The certifier reads the stored
Parquet tape of one instrument and day, it rebuilds the order book from the snapshots and the deltas, and it
classifies every delta against the last applied update id L. A delta is stale when its final id is at most L,
it is contiguous when the first id that it needs is at most L plus 1, and it is a gap otherwise, which is one
rule that covers the Binance, Bybit and OKX numbering. After a gap the certifier treats the book as
unsynchronised until the next snapshot, and it counts that time against coverage. The certifier also counts
messages whose id is lower than the highest id that the same connection delivered before, which are out of
order, and ids that appear twice, which are duplicates. It counts book states in which the best bid reaches or
crosses the best ask, and it measures the blind interval, which is the longest hole in the union of the time
spans of all connections. Each count is a check with a threshold in config/settings.yaml, and the certificate
status is certified only when every check passes.

The gap monitor is the simple baseline, and it stands for a recorder that has no sequence audit. It sorts the
arrival times of the stream messages, and it raises an alarm when the longest interval between two arrivals
exceeds three times the median interval. The cryptofeed baseline runs the Binance book handler of cryptofeed
3.0.1 offline, and it reads the same frames that the certifier reads. The module experiments/cryptofeed_probe.py fills the symbol table by hand,
and it answers the request for a REST snapshot with the latest snapshot frame of the stream, so no network is
needed. Cryptofeed checks the U and u continuity in its method _check_update_id and resets the book on a gap,
and the option cross_check raises an exception when the bid and the ask overlap, so we count a fault as
detected when either signal occurs. The present study runs cryptofeed only on Binance, because its offline
probe exists only for the Binance handler.

The detection experiment injects one fault per run with the module faults.py at a delta frame that the
experiment draws uniformly from the frames between 10% and 70% of the stream. A drop deletes the frame, so the
update ids jump. A reorder delays the frame past the frame after it, so the two frames arrive in the wrong
order. A cross moves the first bid of the frame above the best ask, which corrupts the book.
A re-subscription replaces the connection, so the frames of the next 300 to 3000 ms never arrive, and the
experiment draws that length uniformly from the range that Tardis.dev documents. A duplicate adds a copy of
the frame, which a second connection delivers 1 to 50 ms later. The experiment then replays the frames through the same
adapters, deduplicator and writer that live recording uses, and it certifies the result. A fault counts as
detected when the check that matches it fails, and a clean run counts as a false alarm when its status is not
certified. Duplicates go through a recorder whose deduplicator is off, because the deduplicator would stop
them before they reach the tape, and the experiment then certifies the same frames a second time while the
deduplicator is on.

The rotation experiment runs the real live session class WsVenueSession and the real Parquet writer against a
fake venue. The fake venue emits a Binance trade message with a consecutive trade id every 2 ms of wall-clock
time, and a connection receives only the messages that the venue emits after its connect and subscribe step,
which takes a random time between 50 and 250 ms. Each run lasts 3 s, and the session makes a planned reconnect every 0.25 s.
The naive policy closes the old connection and then opens the new one. The overlapping policy opens the new
connection and closes the old one only after the new one has delivered data. The third mode keeps two naive
connections, which rotate independently, and the deduplicator merges them. The certifier then measures the
blind interval and counts the trade ids that are missing.

The regions experiment sends one Bybit stream of 2000 deltas at 20 ms to two capture regions, which we call eu
and tokyo. Each region loses every delta with a probability of 0.002 on its own, and each region has one blind
re-subscription of 300 to 3000 ms between 10% and 80% of the stream. The eu region adds 10 ms of latency and
the tokyo region adds 20 ms, and both add a jitter that is uniform between 0 and 30 ms. The experiment
certifies each region on its own, which is the single-region baseline, and then it merges the two trees with
the command merge-regions, which keeps for every update id the copy that arrived first, and certifies the
merged tape.

Every rate in the tables comes with the 95% score interval of Wilson (1927) and the number of runs. Every mean
comes with its standard error, which is the sample standard deviation divided by the square root of n, and with
a 95% percentile bootstrap interval from 10000 resamples. We compare two rates with the two-proportion z-test
with a pooled variance, whose null hypothesis is that the two rates are equal. We compare two samples of runs
with the Mann-Whitney U test in its normal approximation with a correction for ties, whose null hypothesis is
that the two samples come from the same distribution. The critical value of both tests at 95% confidence is
1.96 in absolute value, which means that we reject the null hypothesis when the absolute value of z exceeds 1.96.

## 4. Experiments and results

The first result is that the certificate detected every injected fault and raised no false alarm. As Table 1
of tables.md shows, the certificate detected the fault in all 600 runs of each of the five fault types, which
gives a 95% Wilson interval from 99.4% to 100%, and it certified all 600 clean runs, so its false alarm rate
has an interval from 0.0% to 0.6%. Table 3 shows that the rate was the same on each of the three venues, with
200 runs per cell and an interval from 98.1% to 100%. The certificate also measured the length of each injected
re-subscription, because Table 4 gives a mean blind interval of 1645.7 ms with a standard error of 31.7 ms over
600 runs, which agrees with the mean of 1650 ms of the uniform range from 300 to 3000 ms that the experiment
drew from.

| Fault (n per cell) | Certificate | Gap monitor, Binance and Bybit (400 runs) | Gap monitor, OKX (200 runs) | cryptofeed, Binance (200 runs) |
|---|---|---|---|---|
| clean, false alarms (600) | 0.000 [0.000, 0.006] | 0.000 | 1.000 | 0.000 [0.000, 0.019] |
| drop (600) | 1.000 [0.994, 1.000] | 0.000 | 1.000 | 1.000 [0.981, 1.000] |
| reorder (600) | 1.000 [0.994, 1.000] | 0.000 | 1.000 | 1.000 [0.981, 1.000] |
| cross (600) | 1.000 [0.994, 1.000] | 0.000 | 1.000 | 1.000 [0.981, 1.000] |
| re-subscription (600) | 1.000 [0.994, 1.000] | 1.000 | 1.000 | 1.000 [0.981, 1.000] |
| duplicate (600) | 1.000 [0.994, 1.000] | 0.000 | 1.000 | 0.000 [0.000, 0.019] |

The second result is that the gap monitor carries almost no information about faults. As Table 3 shows, on the
Binance and Bybit streams, which have fixed intervals, the gap monitor caught every re-subscription but none of
the drops, reorders, crossed books or duplicates. On the OKX stream, whose intervals are exponential, the gap
monitor raised an alarm in all 200 clean runs, because the longest of 2000 exponential intervals has an
expected length of 8.2 times the mean, while the median is 0.69 times the mean, so its alarms there say nothing
about faults. The pooled rate of 33.3% in Table 1, with an
interval from 29.7% to 37.2%, is therefore the share of OKX runs and not a detection rate. The two-proportion
z-test of the certificate against the gap monitor on drops gives z = 24.49, while the critical value at 95%
confidence is 1.96, so the difference is not random.

The third result is that cryptofeed noticed the same faults as the certificate on Binance, except duplicates,
and that it keeps no record of what it noticed. As Table 2 shows, cryptofeed reset its book in all 200 runs
with a drop, a reorder or a re-subscription, and its cross_check option raised an exception in all 200 runs
with a crossed book. It ignored all 200 duplicates, because its continuity check treats a message whose ids it
has already applied as old and drops it without a signal, so the two-proportion z-test of the certificate
against cryptofeed on duplicates gives z = 20.00, while the critical value is 1.96. The deduplicator of the
recorder removed the duplicate copy in all 600 runs, as Table 4 shows, with an interval from 99.4% to 100%, so
the recorded day was certified in each of those runs.

The fourth result is that the overlapping policy removes the loss at a planned reconnect, while the naive policy
loses a share of the stream that equals the connect time. As Table 5 shows, the naive policy left a blind
interval of 151.9 ms per reconnect, with a standard error of 6.7 ms and a 95% bootstrap interval from 138.9 to 164.7 ms over 20 runs, and it lost 75.0 trades per reconnect, with an interval from 68.5 to 81.4. That loss
matches the mean connect time of 150 ms divided by the message interval of 2 ms. The overlapping policy lost 0.0
trades per reconnect, and all 20 of its runs had no missing trade, which gives an interval from 83.9% to 100%.
The Mann-Whitney U test that compares the missing trades per reconnect of the overlapping policy with those of
the naive policy gives z = -5.78, while
the critical value at 95% confidence is 1.96 in absolute value, so the difference is not random. The overlapping
policy still shows a blind interval of 2.0 ms per reconnect, with a standard error below 0.05 ms, which equals one message interval of 2 ms, because the certifier measures the hole between the last row of the old
connection and the first row of the new one, so the count of missing trades is the direct measure. Two
redundant naive connections lost 16.0 trades per reconnect, with an interval from 13.3 to 18.6, which is lower than the 75.0 of one naive connection but still above zero, because the down times of the two connections sometimes overlapped.

| Mode (20 runs each) | Reconnects per run | Blind ms per reconnect | Trades lost per reconnect | Runs without loss |
|---|---|---|---|---|
| naive, 1 connection | 6.5 [6.3, 6.8] | 151.9 [138.9, 164.7] | 75.0 [68.5, 81.4] | 0.000 [0.000, 0.161] |
| overlapping, 1 connection | 6.6 [6.4, 6.8] | 2.0 [1.9, 2.0] | 0.0 [0.0, 0.0] | 1.000 [0.839, 1.000] |
| naive, 2 connections | 13.3 [13.1, 13.6] | 32.8 [27.3, 38.1] | 16.0 [13.3, 18.6] | 0.000 [0.000, 0.161] |

The fifth result is that a merge of two regions turns most degraded days into certified days, but it cannot
recover a message that both regions lost. As Table 6 shows, each region on its own had 3.67 and 3.78 sequence
gaps per day on average, with standard errors of 0.12 and 0.13 over 100 runs, and none of its 100 days was
certified, so the interval runs from 0.0% to 3.7%. The merged tape had 0.33 gaps per day, with a standard error
of 0.05, and 70.0% of its days were certified, with an interval from 60.4% to 78.1%. The two-proportion z-test
of the merged tape against the eu region gives z = 10.38, while the critical value is 1.96, so the gain is not
random. The 30 merged days that stayed degraded had a gap because a delta that one region dropped fell inside
the blind interval of the other region, as the per-run file regions.csv shows, and in 9 of these runs the two
blind intervals overlapped in time, which left a merged blind interval of up to 2173 ms.

| Capture (100 runs) | Sequence gaps per day | Longest blind interval, ms | Days certified |
|---|---|---|---|
| eu region | 3.67 [3.43, 3.91] | 1690.6 [1534.4, 1838.5] | 0.000 [0.000, 0.037] |
| tokyo region | 3.78 [3.53, 4.03] | 1804.9 [1657.8, 1953.0] | 0.000 [0.000, 0.037] |
| merged | 0.33 [0.23, 0.44] | 84.2 [25.1, 159.9] | 0.700 [0.604, 0.781] |

## 5. Comparison with analogs

The specification names Tardis.dev, cryptofeed and CCXT as the analogs, and the present study ran the one of
them that it could run fairly, which is cryptofeed. The specification, section I1, states that cryptofeed is a
feed handler without storage, gap auditing or archive, and Table 2 confirms the first two points, because
cryptofeed noticed four of the five faults but wrote no record that a later reader could audit and ignored
every duplicate. Tardis.dev sells its data for 350 to 6000 dollars a month, so the present study did not run
it. Its documentation, in the overview of its historical data details, states that each daily re-subscription
leaves a gap of 300 to 3000 ms, so we can conclude that its archive has the loss that the naive policy shows in
Table 5, at a larger scale. The overlapping policy of the present project lost no trade at a reconnect in
Table 5, and the certificate measured every injected re-subscription gap in Table 4, so a user of the tape can
see such a gap when it happens. CCXT is a client for live exchange APIs and not a tick archive, as the
comparison page of the CCXT documentation states, so it has no completeness check that the present study could
compare.

| Tool | Detects losses | Avoids losses at reconnect | Evidence in the present study |
|---|---|---|---|
| Gap monitor (baseline) | re-subscriptions only, on fixed-interval streams | no | Table 3: 0.0% of drops on Binance and Bybit, 100% false alarms on OKX |
| cryptofeed 3.0.1 | drops, reorders, crossed books, no record | no | Table 2: 0.0% of duplicates |
| Tardis.dev | not documented | no, 300 to 3000 ms gap per day | its documentation, not run |
| Market Tape Recorder | all five fault types, stored per day | yes, overlapping rotation | Tables 1, 5 and 6 |

## 6. Decision

The present project adopts the quality certificate with a sequence audit as the record of completeness, the
overlapping rotation as the default reconnect policy, and a merge of two capture regions for days that must be
complete. Table 1 and Table 3 support the certificate, because it caught every fault on every venue with no
false alarm, while the gap monitor caught 0.0% of the drops on the fixed-interval venues. Table 5 supports the
overlapping rotation, because it cut the loss per reconnect from 75.0 trades to 0.0. Table 6 supports the
merge, because it raised the share of certified days from 0.0% to 70.0%. During the study we changed the
coverage window of the certificate from day to observed for the experiments, because a synthetic stream lasts
less than one minute, so a window of a full day would have marked every synthetic day as uncovered. We also
changed the reporting of the gap monitor from a pooled rate to a rate per venue, because the first run of make
report showed a pooled rate of 33.3% for every fault type and for the clean streams as well, which came from OKX alone.

## 7. Limitations

The present study rests on synthetic streams and a fake venue, so it does not show how often real venues drop,
reorder or duplicate messages, and the platform needs a live soak test across a daily venue disconnect before
it can measure that. The injected faults match the checks by design, so a detection rate of 100% shows that each
check works, and it does not show that the checks cover every fault that real feeds have, such as a stale
snapshot that agrees with its own update id. The certificate audits update ids, so it cannot detect a lost
message on the Hyperliquid book or on MOEX, whose feeds carry no such ids, and on those venues it relies on
coverage and silence checks. The rotation experiment runs on wall-clock time, so its numbers vary slightly
between runs of make report, while the detection and regions experiments are deterministic. Each cell has 20 to
600 runs, so a rate of zero has an upper bound between 0.6% and 16.1%.

## 8. References

Wilson, E. B. (1927). Probable inference, the law of succession, and statistical inference. Journal of the
American Statistical Association 22(158).

Mann, H. B. and Whitney, D. R. (1947). On a test of whether one of two random variables is stochastically larger
than the other. Annals of Mathematical Statistics 18(1).

Tardis.dev documentation, Historical data details, overview, https://docs.tardis.dev/historical-data-details/overview.

CCXT documentation, CCXT vs Tardis comparison, https://docs.ccxt.com/docs/comparisons/ccxt-vs-tardis.

cryptofeed 3.0.1, source code of the Binance exchange class and of Feed.book_callback, https://github.com/bmoscon/cryptofeed.

The Tradeq platform plan, quant_platform_plan_full.md, section I1, gives the list of analogs, and the venue API
documentation of Binance, Bybit and OKX gives the update id rules that the generator follows.
