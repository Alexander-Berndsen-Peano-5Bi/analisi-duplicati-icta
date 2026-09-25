from pathlib import Path
import base64, gzip

ROOT = Path(__file__).resolve().parent
bundle = ROOT / "bundle"
source = gzip.decompress(base64.b64decode((bundle / "app_source.b64").read_text())).decode("utf-8")
exec(compile(source, str(ROOT / "app_impl.py"), "exec"), globals())
