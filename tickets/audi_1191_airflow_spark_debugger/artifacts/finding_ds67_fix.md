# DS67 fix note (for Alyson / Sean)

**Root cause of the 2026-08-02/03 `data_source_id=67` failures:** a code bug in the DS67 model.

`ipdsc_ds_67.py:73` passes `write_location` (the bound method) where a path string is expected, instead
of calling it `write_location()`. So the GCS path becomes the method's repr
(`<bound method BaseModel.write_location of DS67>`), which fails bucket-name validation:

```
IllegalArgumentException: Invalid GCS bucket name '<bound method BaseModel.write_location of DS67>':
bucket name must contain only 'a-z0-9_.-' characters.
```

**Fix:** add the parentheses at `ipdsc_ds_67.py:73` (and anywhere else the path is built from
`write_location`): `write_location` → `write_location()`.

**Ripple:** ds67 never wrote its `ipdsc/dt=.../data_source_id=67` partition, so `tpa_export` and
`tpa_mntn_id_export` failed with "path not found / missing partition" (downstream symptoms, not their own
bug). Sean's quick fix (skip ds67 + force export) unblocked downstream; this is the real fix.

*(Surfaced automatically by the AUDI-1191 RCA debugger from the failed Dataproc batch driver log.)*
