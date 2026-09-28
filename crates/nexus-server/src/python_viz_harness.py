"""Fixed harness run by python_viz.rs — not user-editable. Reads the input
RecordBatch(es) from `in.parquet`, calls the user's `visualize(df)` (loaded
from `script.py`), writes the resulting image bytes to `out.bin` plus its
content type to `out.contenttype`.

`visualize(df)` may return:
  - a matplotlib/seaborn `Figure` (duck-typed via `.savefig()`) — saved as
    PNG.
  - a `(bytes, content_type)` tuple — written as-is, for any library that
    doesn't produce a matplotlib Figure (Plotly's `to_image()`/`to_html()`,
    Bokeh, etc.).

Argv: harness.py <in.parquet> <out.bin> <out.contenttype> <script.py>
"""
import importlib.util
import io
import sys
import traceback


def main() -> int:
    in_path, out_path, contenttype_path, script_path = (
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
        sys.argv[4],
    )

    import pyarrow.parquet as pq

    df = pq.read_table(in_path).to_pandas()

    spec = importlib.util.spec_from_file_location("nexusflow_user_script", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "visualize"):
        print("script.py must define a `visualize(df)` function", file=sys.stderr)
        return 1

    result = module.visualize(df)

    if hasattr(result, "savefig"):
        buf = io.BytesIO()
        result.savefig(buf, format="png")
        image_bytes, content_type = buf.getvalue(), "image/png"
    elif isinstance(result, tuple) and len(result) == 2:
        image_bytes, content_type = result
    else:
        print(
            "visualize(df) must return a matplotlib/seaborn Figure "
            "(anything with .savefig()) or a (bytes, content_type) tuple, "
            f"got {type(result)!r}",
            file=sys.stderr,
        )
        return 1

    with open(out_path, "wb") as f:
        f.write(image_bytes)
    with open(contenttype_path, "w", encoding="utf-8") as f:
        f.write(content_type)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
