# RevealJS Presentation Guide

Lessons learned from building TI-804/TI-813 presentation decks. Follow these rules to avoid layout issues.

## RevealJS Configuration

```javascript
Reveal.initialize({
    hash: true,
    slideNumber: true,
    controls: true,
    progress: true,
    center: true,          // ALWAYS true — vertically centers content
    transition: 'fade',
    transitionSpeed: 'slow',
    width: 1100,           // slightly under 1200 to avoid horizontal overflow
    height: 800,           // taller than default 700 to give more room
    margin: 0.01,          // minimal margin — content controls its own spacing
    minScale: 0.2,         // allow RevealJS to shrink slides to fit viewport
    maxScale: 1.5,         // allow slight zoom on large screens
});
```

## Base Font Size

- **Set `font-size: 32px` on `.reveal`** — RevealJS default is 42px which causes overflow on most slides
- This is the single most impactful setting for preventing cutoff
- All other sizes (headings, tables, etc.) are relative to this base

## Rules That Prevent Cutoff

### 1. Never use `position: absolute` for footer elements
Footer notes with `position: absolute; bottom: X` will overflow the viewport. Use inline positioning instead:
```css
/* BAD — causes cutoff */
.footer-note { position: absolute; bottom: 2em; }

/* GOOD — flows with content */
.footer-note { text-align: center; font-size: 0.4em; color: #AAA; margin-top: 0.5em; }
```

### 2. One idea per slide
If a slide has more than ~6 lines of content, it will overflow. Split into multiple slides. Specific limits:
- **Text slides:** Title + 3-4 lines max
- **Table slides:** 5-6 rows max (including header)
- **List slides:** 5 items max
- **Chart slides:** Chart + 1 line annotation max

### 3. Zero out heading margins globally, remove `margin-top` from inline styles
RevealJS themes add default `margin-top` to h1/h2/h3 elements. When `center: true` is on, these margins push content below the vertical midpoint. Fix globally in CSS:
```css
.reveal h1 { margin-top: 0; }
.reveal h2 { margin-top: 0; }
```
Also avoid inline `margin-top` on any elements inside slides — let `center: true` handle vertical positioning. If you need spacing between elements, use small values (0.5em max) or `margin-bottom` on the element above.

### 4. Size guidelines for elements

| Element | Size | Notes |
|---------|------|-------|
| Base font | 32px | Set on `.reveal` |
| H1 | 2em | Title slides only |
| H2 | 1.4em | Section headers |
| H3 | 1em | Sub-headers |
| Body text | 0.8em | Descriptions, annotations |
| Table font | 0.55em | Tables are dense — keep small |
| Table padding | 0.35em 0.6em | Tight padding prevents overflow |
| Lollipop/chart labels | 0.55em | Data viz labels |
| Insight box | 0.75em | Callout boxes |
| Bar chart height | 200-250px | Max height for bar charts |
| Bar width | 70px | Fits 6 bars comfortably |

### 5. CDN caching on githack
githack.com caches aggressively. When iterating on a deck:
- **During development:** Create a new gist each time (`gh gist create --public`)
- **For sharing:** Use the latest gist URL
- **Alternative:** Open the local `.html` file directly during development

## Standalone Build Process

1. Write the deck in `ti_xxx_presentation_deck.html` (uses CDN links)
2. Run the inline script to create `ti_xxx_presentation_deck_standalone.html`
3. Share via `bash .claude/scripts/share_deck.sh path/to/standalone.html`

```python
# Inline script to build standalone
with open('deck.html', 'r') as f:
    html = f.read()
for name, url in [
    ('reveal.css', 'https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css'),
    ('white.css', 'https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css'),
    ('reveal.js', 'https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js'),
]:
    # Download once to /tmp, then inline
    with open(f'/tmp/{name}', 'r') as f:
        content = f.read()
    if name.endswith('.css'):
        html = html.replace(f'<link rel="stylesheet" href="{url}">', f'<style>{content}</style>')
    else:
        html = html.replace(f'<script src="{url}"></script>', f'<script>{content}</script>')
with open('deck_standalone.html', 'w') as f:
    f.write(html)
```

## Color Palette

```css
--navy: #1B2A4A;    /* headings, emphasis */
--blue: #2E5090;    /* secondary bars */
--mid: #5A7DB5;     /* mid-range data */
--light: #A8BDD9;   /* light data */
--muted: #C8CDD4;   /* lowest/baseline data */
--red: #D63B2F;     /* key insight, hero number, accent */
--text: #222222;    /* body text */
--text-light: #666666; /* secondary text */
```

## Slide Templates

### Hero Number Slide
```html
<section>
    <p class="big-number-context">Context text above</p>
    <div class="big-number">72x</div>
    <p class="big-number-context">Context text below</p>
</section>
```

### Simple List Slide
```html
<section>
    <h2>Title</h2>
    <div style="margin-top: 1.5em; font-size: 0.8em; line-height: 2;">
        <span style="color: #BBB;">1.</span> Item one<br>
        <span style="color: #BBB;">2.</span> Item two<br>
    </div>
    <p style="color: #999; font-size: 0.7em; margin-top: 1.5em;">Footer note</p>
</section>
```

### Close/Power Line Slide
```html
<section>
    <div class="power-line" style="font-size: 1.5em;">
        Power line text here.
    </div>
    <p style="text-align: center; color: var(--text-light); font-size: 0.75em; margin-top: 1.5em;">
        Supporting stat line.
    </p>
</section>
```
