# TI-837: Implementation Plan for Intent Score Shuffling

**Jira:** https://mntn.atlassian.net/browse/TI-837
**Status:** Backlog
**Date Started:**
**Date Completed:**
**Assignee:** Malachi
**Parent:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250) — Incrementality Overhaul

---

## 1. Introduction

Create the technical implementation plan for the intent score shuffling experiment. This translates the control group design (TI-835) into a concrete engineering plan.

## 2. The Problem

Need to define exactly how scores get shuffled, which systems change, how original scores are preserved, and how to roll back cleanly.

## 3. Plan of Action

1. Define the shuffling mechanism — how IPs get reassigned between intent tiers
2. Identify which systems need modification (intent scoring pipeline, Aerospike, logging)
3. Design the score logging approach — preserve original scores before shuffle
4. Define rollback procedure (must be fully reversible)
5. Coordinate with RX squad on ITT reporting data needs
6. Scope engineering effort, create subtasks if needed

## 4. Investigation & Findings

*Not yet started.*

## 5. Solution

*Pending.*

## 6. Questions Answered

*None yet.*

## 7. Data Documentation Updates

*None yet.*

## 8. Open Items / Follow-ups

- [ ] Depends on TI-835 (control group design)
- [ ] Coordination with bidder/scoring pipeline team
- [ ] RX squad consultation on ITT measurement requirements
