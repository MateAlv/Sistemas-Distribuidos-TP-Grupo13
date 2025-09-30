
class DirectoryBatchReader:
    """
    Recorre un directorio (recursivo) y emite FileChunk por cada archivo encontrado.

    - Ordena los archivos por ruta relativa (estable).
    - Si 'extensions' no está vacío, filtra por extensión (e.g., [".csv"]).
    - Para archivos de 0 bytes emite UN chunk vacío con first=True, last=True
      (para que el caller pueda enviar header + SIZE=0 y esperar ACK sin cuerpo).
    - No conoce nada del protocolo; sólo entrega datos y metadata.

    Stats:
      - total_files, total_bytes, total_chunks
    """

    def __init__(
        self,
        root_dir: str,
        batch_size: int,
        *,
        mode: str = "bytes",
        encoding: str = "utf-8",
        newline: str = "\n",
        extensions: Optional[List[str]] = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size debe ser > 0")
        self.root_dir = os.path.abspath(root_dir)
        self.batch_size = int(batch_size)
        self.mode = mode
        self.encoding = encoding
        self.newline = newline
        self.extensions = [e.lower() for e in (extensions or [])]

        # stats
        self.total_files = 0
        self.total_bytes = 0
        self.total_chunks = 0

    def __iter__(self) -> Generator[FileChunk, None, None]:
        for abs_path, rel_path in self._iter_files(self.root_dir):
            size = self._safe_size(abs_path)
            self.total_files += 1
            self.total_bytes += size

            if size == 0:
                self.total_chunks += 1
                yield FileChunk(rel_path=rel_path, file_size=0, data=b"", first=True, last=True)
                continue

            reader = BatchReader(
                abs_path,
                self.batch_size,
                mode=self.mode,
                encoding=self.encoding,
                newline=self.newline,
            )

            first = True
            remaining = size
            for chunk in reader:
                remaining -= len(chunk)
                last = remaining == 0
                self.total_chunks += 1
                yield FileChunk(
                    rel_path=rel_path,
                    file_size=size,
                    data=chunk,
                    first=first,
                    last=last,
                )
                first = False

    # -----------------------
    # Internos
    # -----------------------
    def _iter_files(self, root: str) -> Generator[Tuple[str, str], None, None]:
        root_abs = os.path.abspath(root)
        for dirpath, _, filenames in os.walk(root_abs):
            for name in sorted(filenames):
                abs_path = os.path.join(dirpath, name)
                if not os.path.isfile(abs_path):
                    continue
                if self.extensions:
                    _, ext = os.path.splitext(name)
                    if ext.lower() not in self.extensions:
                        continue
                rel_path = os.path.relpath(abs_path, root_abs)
                yield abs_path, rel_path

    @staticmethod
    def _safe_size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except OSError:
            return 0
