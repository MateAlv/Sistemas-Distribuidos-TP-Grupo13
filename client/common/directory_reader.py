import os
from typing import Generator, Iterable, List, Optional, Tuple

class DirectoryReader:
    """
    Recorre un directorio (recursivo) y devuelve el path de todos los archivos de cierta extensión.

    - Ordena los archivos por ruta relativa (estable).
    - Si 'extensions' no está vacío, filtra por extensión (e.g., [".csv"]).

    Stats:
      - total_files
    """

    def __init__(
        self,
        rel_root_dir: str,
    ) -> None:
        self.rel_root_dir = rel_root_dir
        self.root_dir = os.path.abspath(rel_root_dir)
        # stats
        self.total_files = 0

    # -----------------------
    # Internos
    # -----------------------
    def iter(self) -> Generator[Tuple[str, str, int], None, None]:
        for dirpath, _, filenames in sorted(os.walk(self.root_dir)):
            for name in sorted(filenames):  # sort filenames inside the folder
                abs_path = os.path.join(dirpath, name)
                if not os.path.isfile(abs_path):
                    continue
                rel_path = os.path.relpath(abs_path, self.root_dir)
                yield abs_path, rel_path, self._safe_size(abs_path)
                self.total_files += 1

    @staticmethod
    def _safe_size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except OSError:
            return 0
