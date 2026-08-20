"""Fixed harness run by python_transform.rs — not user-editable. Reads the
input RecordBatch(es) from `in.parquet`, calls the user's `transform(df)`
(loaded from `script.py`), writes the result to `out.parquet`.

Argv: harness.py <in.parquet> <out.parquet> <script.py>
"""
import importlib.util
import sys
import traceback


def main() -> int:
    in_path, out_path, script_path = sys.argv[1], sys.argv[2], sys.argv[3]

    import pyarrow.parquet as pq

    df = pq.read_table(in_path).to_pandas()

    spec = importlib.util.spec_from_file_location("nexusflow_user_script", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "transform"):
        print("script.py must define a `transform(df)` function", file=sys.stderr)
        return 1

    result = module.transform(df)

    import pyarrow as pa

    pq.write_table(pa.Table.from_pandas(result, preserve_index=False), out_path)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
