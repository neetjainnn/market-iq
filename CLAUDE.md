# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Market IQ is a single-file quiz widget designed to be embedded within the Upstox trading app UI. It presents daily F&O (Futures & Options) knowledge questions to traders, tracks streaks/accuracy, shows community stats, and recommends courses based on skill gaps.

## Architecture

Everything lives in a single `index.html` file with inline CSS and JS — no build step, no dependencies, no frameworks.

**Structure within the file:**
- **CSS variables** at `:root` define the Upstox light theme (purple accent `#7B2FC4`, greens, reds, etc.)
- **Homepage shell** mimics the Upstox app (header, indices, quick actions, portfolio) with a compact Market IQ card at the bottom
- **Full-screen overlay** (`miq-fullscreen`) slides in from right when the card is tapped, containing three tabs: Self, Community, Courses
- **Quiz engine** in JS handles answer selection, timer countdown (30s), correct/wrong states, confetti animation, and post-answer reveal (explanation, stats, skill map, community chart)

**Key UI states:**
1. **Unanswered** — live 30s timer, interactive options
2. **Answered** — timer stops, correct/wrong highlighting, explanation + stats appear
3. **Double-tap streak badge** resets quiz for demo purposes

## Development

Open `index.html` directly in a browser — no server needed. For live reload during development, use any static server (e.g., `npx serve .` or `python3 -m http.server`).

## Design Conventions

- Upstox light theme (white backgrounds, purple `#7B2FC4` accent)
- Inter font family via Google Fonts CDN
- Mobile-first, max-width 430px centered layout
- CSS class prefix: `miq-` for all Market IQ components, `upstox-` for shell elements
