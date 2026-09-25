from pathlib import Path
import base64, gzip, json

ROOT = Path(__file__).resolve().parent
bundle = ROOT / "bundle"

assets = json.loads(gzip.decompress(base64.b64decode((bundle / "assets.b64").read_text())).decode("utf-8"))
for rel, content in assets.items():
    p = ROOT / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")

source = gzip.decompress(base64.b64decode((bundle / "app_source.b64").read_text())).decode("utf-8")
exec(compile(source, str(ROOT / "app_impl.py"), "exec"), globals())
