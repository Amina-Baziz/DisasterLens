# Findings

This file holds the findings we draw from the warehouse, with the numbers and where each one comes from. All numbers cover 2025-12-01 to 2026-11-19 (about one year) and come from `sql/findings.sql`. The letters in brackets are the sections of the first query in that file. The numbers count events that USGS or EONET reported. They do not measure damage or severity.

The numbers were checked against the dashboard: with a custom period of 2025/12/01 to 2026/11/19, "Include small earthquakes" switched on and no other filter, the dashboard shows 39,378 events, which is the sum of the six hazard types in finding 1. The screenshot is `docs/screenshots/p2-07_dashboard_crosscheck.png`.

## 1. Earthquakes dominate the data [A]

| Hazard type | Events | Share |
| --- | --- | --- |
| Earthquake | 29,008 | 74% |
| Wildfire | 9,688 | 25% |
| Flood | 537 | 1.4% |
| Severe storm | 93 | 0.2% |
| Sea and lake ice | 31 | 0.1% |
| Volcano | 21 | 0.1% |

Earthquakes and wildfires make up 98% of all events. A reason is the way the sources work: USGS records every earthquake that instruments detect, even very small ones, while EONET only lists events that were curated from news and satellite reports.

## 2. Most earthquakes are moderate and shallow [B, C, G]

- 13,757 earthquakes (47%) have a magnitude from 4 to 4.9.
- Only 1,970 (7%) are magnitude 5 or higher, and only 15 are magnitude 7 or higher.
- 23,173 (80%) are shallower than 70 km, 4,740 are intermediate (70 to 300 km) and 1,095 are deep (over 300 km).
- The strongest earthquakes were two of magnitude 7.8: 25 km SW of Kablalan, Philippines (2026-06-07) and 66 km NNW of Ende, Indonesia (2026-08-14). Next came 7.7 in Panama (2026-10-09), 7.6 in Aomori, Japan (2025-12-08) and 7.5 near Tonga (2026-03-24).

The backfill only loads earthquakes of magnitude 2.5 or higher, so the count of small earthquakes (8,315 under magnitude 3) is incomplete.

## 3. Wildfire reports peak in summer [E]

Wildfire events per month: 655 (Dec 2025), 730, 720, 897, 542, 315 (May 2026, the lowest), 703, 1,071, 1,756 (Aug 2026, the highest), 933, 942 and 424 (Nov 2026, only until the 19th).

August has 5.6 times more wildfire events than May. The map shows the wildfire clusters over Africa, Australia and the United States.

## 4. Most EONET events are already closed [F]

Of 10,370 EONET events, 8,759 (84%) are closed and 1,611 (16%) are still open. The pipeline updates the status of an event when the source reports a newer version, so open events turn into closed ones in the warehouse over time.

## 5. One swarm explains the busiest day [D]

The busiest earthquake day was 2025-12-07 with 366 earthquakes, against an average of about 82 per day. The second query in `sql/findings.sql` shows that 299 of them (82%) were in one place, Yakutat, Alaska, so the spike comes from a single swarm and not from the whole world. The following day (2025-12-08) had 241 earthquakes, and it is also the day of the magnitude 7.6 earthquake in Aomori, Japan.