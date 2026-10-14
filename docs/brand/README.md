# Torii brand files

Two marks carry the Torii name. All files are PNGs with transparent
backgrounds, exported at about twice their usual display width.

| File | Use |
| --- | --- |
| `torii-mark.png` | The gate inside a speech bubble. The gate is negative space, so it takes the page colour on light and dark themes. |
| `torii-wordmark-light.png`, `torii-wordmark-dark.png` | The wordmark whose final "ii" share one vermilion beam, like a gate, with the tagline "your agents, one gate away". |
| `torii-lockup-light.png`, `torii-lockup-dark.png` | The bubble mark beside the plain "torii" wordmark. |

Use the light files on light backgrounds and the dark files on dark
backgrounds. The dark files swap the navy letters for washi cream and keep the
vermilion. In Markdown, pair them with `<picture>` and
`prefers-color-scheme`, as the [README](../../README.md) does.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="torii-lockup-dark.png">
    <img src="torii-lockup-light.png" alt="Torii lockup: the bubble mark beside the torii wordmark" width="480">
  </picture>
</p>

| Colour | Hex | Role |
| --- | --- | --- |
| Shu (vermilion) | `#D4452B` | The gate and the beam. Large text and graphics only. |
| Shu deep | `#B23A22` | Vermilion for links and small text. |
| Sumi | `#1E2230` | Text on washi. |
| Washi | `#F7F1E8` | Background, and text on dark backgrounds. |
| Indigo | `#2E3A59` | Secondary colour. |

Keep the marks as drawn: do not stretch, recolour the vermilion, or redraw the
gate.
