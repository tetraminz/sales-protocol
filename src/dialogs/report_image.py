from __future__ import annotations

from PIL import Image, ImageDraw, ImageFont

from .sgr_core import QualityThresholds, heatmap_zone


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), text, font=font)
    return int(box[2] - box[0])


def write_accuracy_diff_png(
    path: str,
    *,
    rule_keys: list[str],
    conversation_ids: list[str],
    scores: list[list[float | None]],
    thresholds: QualityThresholds,
) -> None:
    rows = conversation_ids or ["NO_DATA"]
    cols = rule_keys or ["NO_RULES"]
    rendered_scores = scores if scores else [[None for _ in cols]]

    font = ImageFont.load_default()
    scratch = Image.new("RGB", (1, 1), (255, 255, 255))
    draw = ImageDraw.Draw(scratch)

    cell_w, cell_h = 88, 22
    gap = 3
    left_pad, right_pad = 14, 14
    top_pad, bottom_pad = 12, 12
    row_label_w = max(_text_width(draw, "CONVERSATION", font), *(_text_width(draw, row, font) for row in rows))

    grid_w = len(cols) * cell_w + (len(cols) - 1) * gap
    grid_h = len(rows) * cell_h + (len(rows) - 1) * gap
    grid_left = left_pad + row_label_w + 12
    grid_top = top_pad + 56
    width = max(grid_left + grid_w + right_pad, 640)
    height = grid_top + grid_h + bottom_pad

    img = Image.new("RGB", (width, height), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    text = (25, 25, 25)
    border = (220, 220, 220)
    zone_color = {
        "green": (66, 161, 96),
        "yellow": (227, 182, 67),
        "red": (201, 82, 70),
        "na": (186, 186, 186),
    }

    draw.text((left_pad, top_pad), "EVAL VS JUDGE HEATMAP", fill=text, font=font)
    legend = [
        (f"GREEN {thresholds.green_min:.2f}-1.00", zone_color["green"]),
        (f"YELLOW {thresholds.yellow_min:.2f}-{thresholds.green_min - 0.01:.2f}", zone_color["yellow"]),
        (f"RED 0.00-{thresholds.yellow_min - 0.01:.2f}", zone_color["red"]),
        ("NA NO_JUDGED", zone_color["na"]),
    ]
    legend_x = left_pad
    for label, color in legend:
        draw.rectangle((legend_x, top_pad + 18, legend_x + 10, top_pad + 28), fill=color, outline=border, width=1)
        draw.text((legend_x + 14, top_pad + 18), label, fill=text, font=font)
        legend_x += _text_width(draw, label, font) + 32

    draw.text((left_pad, grid_top - 18), "CONVERSATION", fill=text, font=font)
    for idx, rule_key in enumerate(cols):
        draw.text((grid_left + idx * (cell_w + gap) + 2, grid_top - 18), rule_key, fill=text, font=font)

    for row_idx, conversation_id in enumerate(rows):
        y = grid_top + row_idx * (cell_h + gap)
        draw.text((left_pad, y + 5), conversation_id, fill=text, font=font)
        row_scores = rendered_scores[row_idx] if row_idx < len(rendered_scores) else []
        for col_idx, _ in enumerate(cols):
            x = grid_left + col_idx * (cell_w + gap)
            score = row_scores[col_idx] if col_idx < len(row_scores) else None
            zone = heatmap_zone(score, thresholds=thresholds)
            draw.rectangle((x, y, x + cell_w, y + cell_h), fill=zone_color[zone], outline=border, width=1)

    img.save(path, format="PNG")
