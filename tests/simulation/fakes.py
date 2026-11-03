"""Faux Spark / dbutils / requests pour exécuter un notebook Databricks hors Databricks (simulation)."""
import json
import re
import sys
import types


class Row(dict):
    def __init__(self, d=None, **kw):
        super().__init__(d or {}, **kw)

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k)

    def __getitem__(self, k):
        if isinstance(k, int):
            return list(self.values())[k]
        return dict.__getitem__(self, k)

    def asDict(self):
        return dict(self)


class DF:
    def __init__(self, rows):
        self.rows = [r if isinstance(r, Row) else Row(r) for r in rows]

    def collect(self):
        return self.rows

    def count(self):
        return len(self.rows)


class FakeSpark:
    def __init__(self, handler, files):
        self.handler, self.files, self.queries = handler, files, []
        spark = self

        class _Reader:
            def text(self, path, wholetext=False):
                if path not in spark.files:
                    raise FileNotFoundError(f"404 {path}")
                return DF([{"value": spark.files[path]}])
        self.read = _Reader()

    def sql(self, q):
        self.queries.append(q)
        return DF(self.handler(q))

    def createDataFrame(self, rows, schema=None):
        spark = self

        class _Writer:
            def mode(self, m):
                return self

            def text(self, path):
                spark.files[f"{path}/part-0000.txt"] = chr(10).join(r[0] for r in rows)

            def parquet(self, path):
                spark.files.setdefault(f"{path}/rows", []).extend(rows)

        class _Frame:
            write = _Writer()

            def coalesce(self, n):
                return self
        return _Frame()


class Resp:
    def __init__(self, status, payload):
        self.status_code, self._p = status, payload
        self.ok = 200 <= status < 300
        self.text = json.dumps(payload)

    def json(self):
        return self._p

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


def install(spark, api, files, widgets, mtimes=None):
    """Enregistre les faux modules et renvoie les globals d'exécution du notebook."""
    pyspark = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    types_mod = types.ModuleType("pyspark.sql.types")

    class SparkSession:
        class builder:
            @staticmethod
            def getOrCreate():
                return spark
    sql_mod.SparkSession = SparkSession
    for n in ("BinaryType", "LongType", "StringType", "StructField", "StructType"):
        setattr(types_mod, n, lambda *a, **k: None)
    sys.modules.update({"pyspark": pyspark, "pyspark.sql": sql_mod, "pyspark.sql.types": types_mod})

    req = types.ModuleType("requests")
    req.calls = []

    def get(url, headers=None, params=None, timeout=None, **kw):
        req.calls.append((url, dict(params or {})))
        status, payload = api(re.sub(r"^https://host", "", url), params or {})
        return Resp(status, payload)
    req.get = get
    sys.modules["requests"] = req

    class _W:
        def text(self, name, default, label=""):
            widgets.setdefault(name, default)

        def dropdown(self, name, default, choices, label=""):
            widgets.setdefault(name, default)

        def get(self, name):
            return widgets[name]

    class _FileInfo:
        def __init__(self, path, name, mtime):
            self.path, self.name, self.modificationTime = path, name, mtime

    class _FS:
        def ls(self, path):
            path = path.rstrip("/")
            if path in files:
                return [_FileInfo(path, path.rsplit("/", 1)[1], (mtimes or {}).get(path, 0))]
            kids = sorted({p[len(path) + 1:].split("/")[0] for p in files if p.startswith(path + "/")})
            if not kids:
                raise FileNotFoundError(path)
            return [_FileInfo(f"{path}/{k}", k, (mtimes or {}).get(f"{path}/{k}", 0)) for k in kids]

        def mv(self, src, dst, recurse=False):
            for k in [k for k in files if k == src or k.startswith(src + "/")]:
                files[dst + k[len(src):]] = files.pop(k)

        def rm(self, path, recurse=False):
            gone = [k for k in files if k == path or k.startswith(path + "/")]
            for k in gone:              # comme dbutils.fs.rm : chemin absent → False, sans erreur
                del files[k]

        def head(self, path, n=0):
            if path not in files:
                raise FileNotFoundError(path)
            return files[path]

    class _Opt:
        def __init__(self, v):
            self.v = v

        def get(self):
            return self.v

    class _Ctx:
        def apiUrl(self):
            return _Opt("https://host")

        def apiToken(self):
            return _Opt("tok")

        def notebookPath(self):
            return _Opt("/Workspace/bundle/files/notebooks/x")

    class _NB:
        def getContext(self):
            return _Ctx()

        def exit(self, v):
            raise SystemExit(v)

    class _DBU:
        def notebook(self):
            return _NB()

    dbutils = types.SimpleNamespace(
        widgets=_W(), fs=_FS(),
        notebook=types.SimpleNamespace(entry_point=types.SimpleNamespace(getDbutils=lambda: _DBU()),
                                       exit=lambda v: (_ for _ in ()).throw(SystemExit(v))))
    out = {"html": None}
    return {"dbutils": dbutils, "displayHTML": lambda h: out.__setitem__("html", h), "display": print,
            "__name__": "__main__"}, out, req
