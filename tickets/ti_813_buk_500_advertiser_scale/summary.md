# TI-813: Scale BUK Keyword Visit Rate Analysis to 500 Advertisers

**Jira:** https://mntn.atlassian.net/browse/TI-813
**Parent Epic:** https://mntn.atlassian.net/browse/TI-803
**Status:** In Progress
**Date Started:** 2026-04-02
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Scale the TI-804 keyword visit rate analysis from 50 to 500 advertisers. Alex Knorr reviewed TI-804 and endorsed the methodology — his one ask was a larger sample. This produces management-ready results for the BUK value case.

## 2. The Problem

TI-804 proved the 184x signal with 50 advertisers, but only 15 had >10 visitors in the 10-day outcome window. Many verticals had only 1 advertiser. Scaling to 500 will:
- Get 100+ advertisers above the visitor threshold
- Produce robust per-vertical breakdowns (multiple advertisers per vertical)
- Strengthen the statistical case for management

## 3. Plan of Action

1. Copy TI-804 queries, change `LIMIT 50` to `LIMIT 500`
2. Dry-run to verify cost (~65GB expected, ipdsc is the bottleneck)
3. Run rank bucket aggregate query
4. Run per-advertiser breakdown query
5. Run per-vertical breakdown query
6. Regenerate Tufte-style charts (generate_charts.py adapted from TI-804)
7. Rebuild RevealJS standalone deck
8. Update presentation.md with scaled results

## 4. Investigation & Findings

*(In progress)*

## 5. Solution

## 6. Questions Answered

## 7. Data Documentation Updates

## 8. Open Items / Follow-ups

## Outputs

| File | Description |
|------|-------------|
