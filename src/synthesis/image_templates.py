"""Pillow-based image template renderers for visual posts."""

import os
import textwrap
import tempfile
import uuid
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

# Image dimensions (X/Twitter recommended: 1200x675 for 16:9)
WIDTH = 1200
HEIGHT = 675

# Color palettes
PALETTES = {
    "dark": {
        "bg": (24, 24, 27),       # zinc-900
        "text": (244, 244, 245),   # zinc-100
        "accent": (99, 102, 241),  # indigo-500
        "muted": (161, 161, 170),  # zinc-400
        "divider": (63, 63, 70),   # zinc-700
    },
    "light": {
        "bg": (250, 250, 250),    # zinc-50
        "text": (24, 24, 27),     # zinc-900
        "accent": (79, 70, 229),  # indigo-600
        "muted": (113, 113, 122), # zinc-500
        "divider": (212, 212, 216), # zinc-300
    },
}

# Default output directory
DEFAULT_OUTPUT_DIR = Path(tempfile.gettempdir()) / "presence_images"


def _get_output_dir(output_dir: Optional[str] = None) -> Path:
    """Get or create the output directory."""
    resolved = Path(output_dir).expanduser() if output_dir else DEFAULT_OUTPUT_DIR
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _default_output_path(prefix: str, output_dir: Optional[str] = None) -> str:
    """Build a unique PNG path in the configured output directory."""
    filename = f"{prefix}_{uuid.uuid4().hex[:12]}.png"
    return str(_get_output_dir(output_dir) / filename)


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Load a font, falling back to default if system fonts unavailable."""
    font_paths = [
        "/System/Library/Fonts/Menlo.ttc",
        "/System/Library/Fonts/SFMono-Regular.otf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    ]
    for path in font_paths:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except (OSError, IOError):
                continue
    return ImageFont.load_default(size=size)


def _wrap_text(text: str, font: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    """Wrap text to fit within max_width pixels."""
    # Estimate chars per line from average char width
    avg_char_width = font.getlength("M")
    chars_per_line = max(10, int(max_width / avg_char_width))
    return textwrap.wrap(text, width=chars_per_line)


def render_annotated_text(
    title: str,
    body: str,
    style: str = "dark",
    output_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> str:
    """Render text on a styled background.

    Creates a card-style image with a title and body text,
    suitable for "hot take" or insight posts.

    Returns path to the generated PNG file.
    """
    palette = PALETTES.get(style, PALETTES["dark"])
    img = Image.new("RGB", (WIDTH, HEIGHT), palette["bg"])
    draw = ImageDraw.Draw(img)

    # Fonts
    title_font = _load_font(42, bold=True)
    body_font = _load_font(28)

    # Layout
    margin = 80
    content_width = WIDTH - 2 * margin

    # Accent bar at top
    draw.rectangle([0, 0, WIDTH, 6], fill=palette["accent"])

    # Title
    title_lines = _wrap_text(title, title_font, content_width)
    y = margin + 20
    for line in title_lines:
        draw.text((margin, y), line, fill=palette["accent"], font=title_font)
        y += title_font.getbbox("A")[3] + 12

    # Divider
    y += 16
    draw.line([(margin, y), (margin + 200, y)], fill=palette["divider"], width=2)
    y += 24

    # Body text
    body_lines = _wrap_text(body, body_font, content_width)
    for line in body_lines[:10]:  # Cap at 10 lines
        draw.text((margin, y), line, fill=palette["text"], font=body_font)
        y += body_font.getbbox("A")[3] + 10

    if not output_path:
        output_path = _default_output_path("annotated", output_dir)
    img.save(output_path, "PNG")
    return output_path


def render_comparison(
    before: str,
    after: str,
    title: str = "",
    style: str = "dark",
    output_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> str:
    """Render a before/after comparison image.

    Two-panel layout showing transformation or contrast.

    Returns path to the generated PNG file.
    """
    palette = PALETTES.get(style, PALETTES["dark"])
    img = Image.new("RGB", (WIDTH, HEIGHT), palette["bg"])
    draw = ImageDraw.Draw(img)

    title_font = _load_font(36, bold=True)
    label_font = _load_font(24, bold=True)
    body_font = _load_font(24)

    margin = 60
    mid_x = WIDTH // 2

    # Title if provided
    y_start = margin
    if title:
        draw.text((margin, y_start), title, fill=palette["accent"], font=title_font)
        y_start += title_font.getbbox("A")[3] + 30

    # Divider line down the middle
    draw.line(
        [(mid_x, y_start), (mid_x, HEIGHT - margin)],
        fill=palette["divider"],
        width=2,
    )

    # "Before" label + text (left panel)
    panel_width = mid_x - margin - 20
    draw.text((margin, y_start), "BEFORE", fill=palette["muted"], font=label_font)
    y = y_start + label_font.getbbox("A")[3] + 16
    before_lines = _wrap_text(before, body_font, panel_width)
    for line in before_lines[:8]:
        draw.text((margin, y), line, fill=palette["text"], font=body_font)
        y += body_font.getbbox("A")[3] + 8

    # "After" label + text (right panel)
    right_margin = mid_x + 20
    draw.text((right_margin, y_start), "AFTER", fill=palette["accent"], font=label_font)
    y = y_start + label_font.getbbox("A")[3] + 16
    after_lines = _wrap_text(after, body_font, panel_width)
    for line in after_lines[:8]:
        draw.text((right_margin, y), line, fill=palette["text"], font=body_font)
        y += body_font.getbbox("A")[3] + 8

    if not output_path:
        output_path = _default_output_path("comparison", output_dir)
    img.save(output_path, "PNG")
    return output_path


def render_metric_highlight(
    metric: str,
    value: str,
    context: str = "",
    style: str = "dark",
    output_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> str:
    """Render a large metric/number callout image.

    Emphasizes a single key number or statistic with context.

    Returns path to the generated PNG file.
    """
    palette = PALETTES.get(style, PALETTES["dark"])
    img = Image.new("RGB", (WIDTH, HEIGHT), palette["bg"])
    draw = ImageDraw.Draw(img)

    metric_font = _load_font(24)
    value_font = _load_font(96, bold=True)
    context_font = _load_font(24)

    # Center layout
    # Metric label (above the value)
    metric_bbox = draw.textbbox((0, 0), metric, font=metric_font)
    metric_w = metric_bbox[2] - metric_bbox[0]
    draw.text(
        ((WIDTH - metric_w) // 2, HEIGHT // 2 - 120),
        metric,
        fill=palette["muted"],
        font=metric_font,
    )

    # Large value
    value_bbox = draw.textbbox((0, 0), value, font=value_font)
    value_w = value_bbox[2] - value_bbox[0]
    draw.text(
        ((WIDTH - value_w) // 2, HEIGHT // 2 - 60),
        value,
        fill=palette["accent"],
        font=value_font,
    )

    # Context below
    if context:
        context_lines = _wrap_text(context, context_font, WIDTH - 200)
        y = HEIGHT // 2 + 80
        for line in context_lines[:3]:
            line_bbox = draw.textbbox((0, 0), line, font=context_font)
            line_w = line_bbox[2] - line_bbox[0]
            draw.text(
                ((WIDTH - line_w) // 2, y),
                line,
                fill=palette["text"],
                font=context_font,
            )
            y += context_font.getbbox("A")[3] + 8

    if not output_path:
        output_path = _default_output_path("metric", output_dir)
    img.save(output_path, "PNG")
    return output_path


def render_meme_text(
    top_text: str,
    bottom_text: str = "",
    style: str = "dark",
    output_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> str:
    """Render a meme-style caption card with top and bottom text."""
    palette = PALETTES.get(style, PALETTES["dark"])
    img = Image.new("RGB", (WIDTH, HEIGHT), palette["bg"])
    draw = ImageDraw.Draw(img)

    top_font = _load_font(48, bold=True)
    bottom_font = _load_font(42, bold=True)
    center_font = _load_font(24)

    margin = 64
    content_width = WIDTH - 2 * margin

    top_lines = _wrap_text(top_text.upper(), top_font, content_width)
    y = 56
    for line in top_lines[:3]:
        line_bbox = draw.textbbox((0, 0), line, font=top_font)
        line_w = line_bbox[2] - line_bbox[0]
        x = (WIDTH - line_w) // 2
        draw.text((x, y), line, fill=palette["text"], font=top_font)
        y += top_font.getbbox("A")[3] + 12

    # Put a small center caption to make the frame feel more like an actual meme artifact.
    center_label = "CURRENT STATE OF EVENTS"
    label_bbox = draw.textbbox((0, 0), center_label, font=center_font)
    label_w = label_bbox[2] - label_bbox[0]
    draw.text(((WIDTH - label_w) // 2, HEIGHT // 2 - 18), center_label, fill=palette["muted"], font=center_font)

    if bottom_text:
        bottom_lines = _wrap_text(bottom_text.upper(), bottom_font, content_width)
        line_height = bottom_font.getbbox("A")[3] + 10
        total_height = min(3, len(bottom_lines)) * line_height
        y = HEIGHT - margin - total_height
        for line in bottom_lines[:3]:
            line_bbox = draw.textbbox((0, 0), line, font=bottom_font)
            line_w = line_bbox[2] - line_bbox[0]
            x = (WIDTH - line_w) // 2
            draw.text((x, y), line, fill=palette["accent"], font=bottom_font)
            y += line_height

    if not output_path:
        output_path = _default_output_path("meme", output_dir)
    img.save(output_path, "PNG")
    return output_path
