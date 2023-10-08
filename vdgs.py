from visidata import ENTER, Column, Path, Sheet, asyncthread, vd

__version__ = "0.1"


class GSPath(Path):
    blob = None
    storage_client = None
    bucket_name = None
    bucket = None
    path = None

    def __init__(self, uri):
        super().__init__(uri)
        path_without_scheme = uri.split("://", 1)[1]
        splits = path_without_scheme.split("/", 1)
        self.bucket_name = splits[0]
        self.path = splits[1]
        from google.cloud import storage
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.bucket_name)
        self.blob = self.bucket.blob(self.path)

    def open(self, *args, **kwargs):
        print(args)
        print(kwargs)
        return self.blob.open(
            *args,
            **kwargs
        )


def openurl_gs(p: Path, filetype):
    """Open a sheet for an GS path.

    GS directories (prefixes) require special handling, but files (objects)
    can use standard VisiData "open" functions.
    """
    # For now, it supports only file.
    # TODO support directory browsing

    if not filetype:
        filetype = p.ext or "txt"
    p = GSPath(p.given)

    openfunc = getattr(vd, f"open_{filetype.lower()}")
    if not openfunc:
        vd.warning(f"no loader found for {filetype} files, falling back to txt")
        filetype = "txt"
        openfunc = vd.open_txt

    assert callable(openfunc), f"no function/method available to open {p.given}"
    vs = openfunc(p)
    vd.status(
        f'opening {p.given} as {filetype}'
    )
    return vs


def vd_getattr(self, attr):
    """Fall back to global lookups for missing VisiData attributes.

    VisiData is gradually replacing global functions with methods on
    the VisiData (vd) object.

    Let's try to use vd methods everywhere, but fall back to global
    lookups. This could/should let us use current patterns
    but also "just work" on older versions.

    Note: Avoiding "private" attributes (starting with _) keeps us
    from breaking lazy properties.
    """
    if not attr.startswith("_"):
        return self.getGlobals().get(attr)
    raise AttributeError(attr)


vd.__class__.__getattr__ = vd_getattr
vd.addGlobals(globals())
