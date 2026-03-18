# VV IP Trace Flowchart

Three separate charts — one per stage — for readable PDF export.

**How to read:** Start at the top of the relevant stage. Each node is `table.ip` — trace the IP through the pipeline from clickpass (VV) back to bid. Cross-stage links are called out in rule boxes.

---

## Stage 1: Within-Stage Trace

```mermaid
flowchart TD
    START([Find S1 VV in clickpass_log]) --> TYPE{Impression type?}

    TYPE -->|CTV| S1_CTV
    TYPE -->|Display| VIEW{Viewable?}
    VIEW -->|Yes| S1_DV
    VIEW -->|No| S1_DNV

    subgraph CTV ["CTV"]
        S1_CTV[clickpass.ip] --> S1_CTV_2["event_log.ip (vast_start)"]
        S1_CTV_2 --> S1_CTV_2b["event_log.ip (vast_impression)"]
        S1_CTV_2b --> S1_CTV_3[win_log.ip]
        S1_CTV_3 --> S1_CTV_4[impression_log.ip]
        S1_CTV_4 --> S1_CTV_5[bid_log.ip]
    end

    subgraph DV ["Viewable Display"]
        S1_DV[clickpass.ip] --> S1_DV_2[viewability_log.ip]
        S1_DV_2 --> S1_DV_3[win_log.ip]
        S1_DV_3 --> S1_DV_4[bid_log.ip]
    end

    subgraph DNV ["Non-Viewable Display"]
        S1_DNV[clickpass.ip] --> S1_DNV_2[impression_log.ip]
        S1_DNV_2 --> S1_DNV_3[win_log.ip]
        S1_DNV_3 --> S1_DNV_4[bid_log.ip]
    end

    S1_CTV_5 --> DONE([S1 fully traced])
    S1_DV_4 --> DONE
    S1_DNV_4 --> DONE

    classDef start fill:#2563eb,stroke:#1d4ed8,color:#fff,font-weight:bold
    classDef decision fill:#1e3a5f,stroke:#0f2744,color:#fff,font-weight:bold
    classDef done fill:#16a34a,stroke:#15803d,color:#fff,font-weight:bold

    class START start
    class TYPE,VIEW decision
    class DONE done
```

---

## Stage 2: Within-Stage + Cross-Stage to S1

```mermaid
flowchart TD
    START([Find S2 VV in clickpass_log]) --> TYPE{Impression type?}

    TYPE -->|CTV| S2_CTV
    TYPE -->|Display| VIEW{Viewable?}
    VIEW -->|Yes| S2_DV
    VIEW -->|No| S2_DNV

    subgraph S2_WITHIN ["S2 within-stage trace"]
        subgraph CTV ["CTV"]
            S2_CTV[clickpass.ip] --> S2_CTV_2["event_log.ip (vast_start)"]
            S2_CTV_2 --> S2_CTV_2b["event_log.ip (vast_impression)"]
            S2_CTV_2b --> S2_CTV_3[win_log.ip]
            S2_CTV_3 --> S2_CTV_4[impression_log.ip]
            S2_CTV_4 --> S2_CTV_5[bid_log.ip]
        end

        subgraph DV ["Viewable Display"]
            S2_DV[clickpass.ip] --> S2_DV_2[viewability_log.ip]
            S2_DV_2 --> S2_DV_3[win_log.ip]
            S2_DV_3 --> S2_DV_4[bid_log.ip]
        end

        subgraph DNV ["Non-Viewable Display"]
            S2_DNV[clickpass.ip] --> S2_DNV_2[impression_log.ip]
            S2_DNV_2 --> S2_DNV_3[win_log.ip]
            S2_DNV_3 --> S2_DNV_4[bid_log.ip]
        end
    end

    S2_CTV_5 --> CROSS
    S2_DV_4 --> CROSS
    S2_DNV_4 --> CROSS

    CROSS[/"CROSS-STAGE: Search event_log for S1 VAST<br/>where ip = S2.bid_ip, same campaign_group_id<br/>S2 targeting requires a prior S1 CTV impression"/]

    subgraph S1_TRACE ["S2 -> S1: CTV impression trace"]
        S1_CTV_2["event_log.ip (vast_start)"] --> S1_CTV_2b["event_log.ip (vast_impression)"]
        S1_CTV_2b --> S1_CTV_3[win_log.ip]
        S1_CTV_3 --> S1_CTV_4[impression_log.ip]
        S1_CTV_4 --> S1_CTV_5[bid_log.ip]
    end

    CROSS --> S1_CTV_2
    S1_CTV_5 --> DONE([S2 -> S1 fully traced])

    classDef start fill:#2563eb,stroke:#1d4ed8,color:#fff,font-weight:bold
    classDef decision fill:#1e3a5f,stroke:#0f2744,color:#fff,font-weight:bold
    classDef done fill:#16a34a,stroke:#15803d,color:#fff,font-weight:bold
    classDef rule fill:#7c3aed,stroke:#6d28d9,color:#fff,font-weight:bold

    class START start
    class TYPE,VIEW decision
    class DONE done
    class CROSS rule
```

---

## Stage 3: Within-Stage + Cross-Stage VV Bridge

S3 targeting is **VV-based**: the IP entered S3 because it had a prior S1 or S2 verified visit in the same campaign_group. The cross-stage link is `S3.bid_ip -> clickpass_log.ip` (prior VV), NOT `S3.bid_ip -> event_log.ip`.

```mermaid
flowchart TD
    START([Find S3 VV in clickpass_log]) --> TYPE{Impression type?}

    TYPE -->|CTV| S3_CTV
    TYPE -->|Display| VIEW{Viewable?}
    VIEW -->|Yes| S3_DV
    VIEW -->|No| S3_DNV

    subgraph S3_WITHIN ["S3 within-stage trace"]
        subgraph CTV ["CTV"]
            S3_CTV[clickpass.ip] --> S3_CTV_2["event_log.ip (vast_start)"]
            S3_CTV_2 --> S3_CTV_2b["event_log.ip (vast_impression)"]
            S3_CTV_2b --> S3_CTV_3[win_log.ip]
            S3_CTV_3 --> S3_CTV_4[impression_log.ip]
            S3_CTV_4 --> S3_CTV_5[bid_log.ip]
        end

        subgraph DV ["Viewable Display"]
            S3_DV[clickpass.ip] --> S3_DV_2[viewability_log.ip]
            S3_DV_2 --> S3_DV_3[win_log.ip]
            S3_DV_3 --> S3_DV_4[bid_log.ip]
        end

        subgraph DNV ["Non-Viewable Display"]
            S3_DNV[clickpass.ip] --> S3_DNV_2[impression_log.ip]
            S3_DNV_2 --> S3_DNV_3[win_log.ip]
            S3_DNV_3 --> S3_DNV_4[bid_log.ip]
        end
    end

    S3_CTV_5 --> VV_SEARCH
    S3_DV_4 --> VV_SEARCH
    S3_DNV_4 --> VV_SEARCH

    VV_SEARCH[/"CROSS-STAGE: Search clickpass_log for<br/>prior S1 or S2 VV where ip = S3.bid_ip<br/>same campaign_group_id, time < S3.impression_time"/]

    VV_SEARCH --> FOUND{Found prior VV?}

    FOUND -->|No| UNRESOLVED([Unresolved — prior VV exists<br/>but IP untraceable within lookback<br/>cross-device or IP rotation])

    FOUND -->|Yes| VV_STAGE{Prior VV stage?}

    VV_STAGE -->|S1 VV| S1_PATH
    VV_STAGE -->|S2 VV| S2_PATH

    classDef start fill:#2563eb,stroke:#1d4ed8,color:#fff,font-weight:bold
    classDef decision fill:#1e3a5f,stroke:#0f2744,color:#fff,font-weight:bold
    classDef done fill:#16a34a,stroke:#15803d,color:#fff,font-weight:bold
    classDef rule fill:#7c3aed,stroke:#6d28d9,color:#fff,font-weight:bold
    classDef warn fill:#b91c1c,stroke:#991b1b,color:#fff,font-weight:bold

    class START start
    class TYPE,VIEW,FOUND,VV_STAGE decision
    class VV_SEARCH rule
    class UNRESOLVED warn
    class S1_PATH,S2_PATH done
```

### S3 -> S1 VV Path (prior VV was S1)

```mermaid
flowchart TD
    BRIDGE["S1 VV in clickpass_log<br/>clickpass.ip = S3.bid_ip<br/>Get ad_served_id + impression_time"]
    --> TRACE["Trace S1 impression via ad_served_id<br/>S1.bid_ip MAY DIFFER from S3.bid_ip (cross-device)"]
    --> IMP_TYPE{S1 impression type?}

    IMP_TYPE -->|CTV| CTV_2
    IMP_TYPE -->|Display| DISP{Viewable?}
    DISP -->|Yes| DV_2
    DISP -->|No| DNV_2

    subgraph CTV ["S1 impression — CTV"]
        CTV_2["event_log.ip (vast_start)"] --> CTV_2b["event_log.ip (vast_impression)"]
        CTV_2b --> CTV_3[win_log.ip]
        CTV_3 --> CTV_4[impression_log.ip]
        CTV_4 --> CTV_5[bid_log.ip]
    end

    subgraph DV_PATH ["S1 impression — Viewable Display"]
        DV_2[viewability_log.ip] --> DV_3[win_log.ip]
        DV_3 --> DV_4[bid_log.ip]
    end

    subgraph DNV_PATH ["S1 impression — Non-Viewable"]
        DNV_2[impression_log.ip] --> DNV_3[win_log.ip]
        DNV_3 --> DNV_4[bid_log.ip]
    end

    CTV_5 --> DONE([S3 VV -> S1 VV -> S1 impression traced])
    DV_4 --> DONE
    DNV_4 --> DONE

    classDef bridge fill:#7c3aed,stroke:#6d28d9,color:#fff,font-weight:bold
    classDef decision fill:#1e3a5f,stroke:#0f2744,color:#fff,font-weight:bold
    classDef done fill:#16a34a,stroke:#15803d,color:#fff,font-weight:bold

    class BRIDGE,TRACE bridge
    class IMP_TYPE,DISP decision
    class DONE done
```

### S3 -> S2 VV -> S1 Path (prior VV was S2)

```mermaid
flowchart TD
    BRIDGE["S2 VV in clickpass_log<br/>clickpass.ip = S3.bid_ip<br/>Get S2 ad_served_id + impression_time"]
    --> TRACE["Trace S2 impression via ad_served_id<br/>Get S2.bid_ip<br/>S2.bid_ip MAY DIFFER from S3.bid_ip (cross-device)"]
    --> S2_TYPE{S2 impression type?}

    S2_TYPE -->|CTV| S2C_2
    S2_TYPE -->|Display| S2_DISP{Viewable?}
    S2_DISP -->|Yes| S2DV_2
    S2_DISP -->|No| S2DNV_2

    subgraph S2_CTV ["S2 impression — CTV"]
        S2C_2["event_log.ip (vast_start)"] --> S2C_2b["event_log.ip (vast_impression)"]
        S2C_2b --> S2C_3[win_log.ip]
        S2C_3 --> S2C_4[impression_log.ip]
        S2C_4 --> S2C_5[bid_log.ip]
    end

    subgraph S2_DV ["S2 impression — Viewable Display"]
        S2DV_2[viewability_log.ip] --> S2DV_3[win_log.ip]
        S2DV_3 --> S2DV_4[bid_log.ip]
    end

    subgraph S2_DNV ["S2 impression — Non-Viewable"]
        S2DNV_2[impression_log.ip] --> S2DNV_3[win_log.ip]
        S2DNV_3 --> S2DNV_4[bid_log.ip]
    end

    S2C_5 --> S1_CROSS
    S2DV_4 --> S1_CROSS
    S2DNV_4 --> S1_CROSS

    S1_CROSS[/"CROSS-STAGE: Search event_log for S1 VAST<br/>where ip = S2.bid_ip, same campaign_group_id<br/>Note: S2.bid_ip may differ from S3.bid_ip"/]

    S1_CROSS --> S1_FOUND{Found S1 impression?}

    S1_FOUND -->|No| UNRESOLVED([Unresolved at S1 — S2 VV found<br/>but no S1 impression for S2.bid_ip])
    S1_FOUND -->|Yes| S1_2

    subgraph S1_TRACE ["S1 impression trace"]
        S1_2["event_log.ip (vast_start)"] --> S1_2b["event_log.ip (vast_impression)"]
        S1_2b --> S1_3[win_log.ip]
        S1_3 --> S1_4[impression_log.ip]
        S1_4 --> S1_5[bid_log.ip]
    end

    S1_5 --> DONE([S3 VV -> S2 VV -> S1 impression traced])

    classDef bridge fill:#7c3aed,stroke:#6d28d9,color:#fff,font-weight:bold
    classDef decision fill:#1e3a5f,stroke:#0f2744,color:#fff,font-weight:bold
    classDef done fill:#16a34a,stroke:#15803d,color:#fff,font-weight:bold
    classDef rule fill:#7c3aed,stroke:#6d28d9,color:#fff,font-weight:bold
    classDef warn fill:#b91c1c,stroke:#991b1b,color:#fff,font-weight:bold

    class BRIDGE,TRACE bridge
    class S2_TYPE,S2_DISP,S1_FOUND decision
    class DONE done
    class S1_CROSS rule
    class UNRESOLVED warn
```
