from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import flet as ft


def _is_within_root(candidate: Path, root: Path) -> bool:
    cand = candidate.resolve(strict=False)
    rt = root.resolve(strict=True)
    try:
        cand.relative_to(rt)
        return True
    except ValueError:
        return False


@dataclass(frozen=True)
class PickResult:
    kind: str  # "file" or "dir"
    path: Path
    root_dir: Path

    @property
    def rel_path(self) -> Path:
        return self.path.relative_to(self.root_dir)


class SandboxedFilePicker:
    def __init__(
        self,
        *,
        root_dir: str | Path,
        on_pick: Callable[[PickResult], None],
        allow_files: bool = True,
        allow_dirs: bool = False,
        file_exts: set[str] | None = None,
        title: str = "Select a file",
        start_subdir: str | Path | None = None,
    ):
        self._root_unresolved = Path(root_dir)
        self._root = self._root_unresolved.resolve(strict=True)
        self._on_pick = on_pick
        self._allow_files = allow_files
        self._allow_dirs = allow_dirs
        self._file_exts = {e.lower() for e in file_exts} if file_exts else None
        self._title = title

        if start_subdir is None:
            self._cwd = self._root
        else:
            start_unresolved = self._root_unresolved / start_subdir
            start = (self._root / start_subdir).resolve(strict=False)

            if not _is_within_root(start, self._root):
                raise ValueError(f"start_subdir escapes root: start_subdir={start_unresolved!r}")

            if not start.exists():
                raise FileNotFoundError(f"start_subdir does not exist: {start_unresolved}")

            if not start.is_dir():
                raise NotADirectoryError(f"start_subdir is not a directory: {start_unresolved}")

            self._cwd = start

        self._crumbs_row = ft.Row(wrap=True, spacing=0)
        self._list = ft.ListView(expand=True, spacing=2, padding=0)
        self._up_btn = ft.IconButton(icon=ft.Icons.ARROW_UPWARD, tooltip="Up")
        # self._path_text = ft.Text(size=12, selectable=True, no_wrap=False)

        self._up_btn.on_click = self._on_up_clicked

        self.view = self._build_view()

        # Populate initial UI state without calling update() (not mounted yet)
        self._refresh(initial=True)

    def _build_view(self) -> ft.Control:
        header = ft.Row(
            [
                ft.Text(self._title, size=18, weight=ft.FontWeight.BOLD),
                ft.Container(expand=True),
                self._up_btn,
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )

        return ft.Card(
            content=ft.Container(
                padding=12,
                content=ft.Column(
                    [
                        header,
                        ft.Container(content=self._crumbs_row, padding=ft.Padding.only(bottom=6)),
                        # ft.Container(content=self._path_text, padding=ft.Padding.only(bottom=6)),
                        ft.Divider(height=1),
                        ft.Container(expand=True, content=self._list),
                    ],
                    expand=True,
                    spacing=8,
                ),
            )
        )

    def _on_up_clicked(self, e: ft.ControlEvent) -> None:
        if self._cwd == self._root:
            return
        parent = self._cwd.parent
        if _is_within_root(parent, self._root):
            self._cwd = parent
            self._refresh()

    def _go_to(self, target: Path) -> None:
        t = target.resolve(strict=False)
        if _is_within_root(t, self._root) and t.is_dir():
            self._cwd = t
            self._refresh()

    def _pick_file(self, path: Path) -> None:
        p = path.resolve(strict=False)
        if not _is_within_root(p, self._root) or not p.is_file():
            return
        if self._file_exts and p.suffix.lower() not in self._file_exts:
            return
        self._on_pick(PickResult(kind="file", path=p, root_dir=self._root))

    def _pick_dir(self, path: Path) -> None:
        p = path.resolve(strict=False)
        if not _is_within_root(p, self._root) or not p.is_dir():
            return
        self._on_pick(PickResult(kind="dir", path=p, root_dir=self._root))

    def _refresh(self, *, initial: bool = False) -> None:
        # Mutate control properties + rebuild children
        self._up_btn.disabled = self._cwd == self._root
        # self._path_text.value = str(self._cwd)

        self._crumbs_row.controls.clear()
        rel_parts = self._cwd.relative_to(self._root).parts if self._cwd != self._root else ()
        labels = ("[root]",) + rel_parts

        current = self._root
        for i, label in enumerate(labels):
            if i == 0:
                current = self._root
            else:
                current = current / label

            self._crumbs_row.controls.append(
                ft.TextButton(
                    content=ft.Text(label),
                    on_click=(lambda e, p=current: self._go_to(p)),
                    style=ft.ButtonStyle(padding=ft.Padding.symmetric(horizontal=6, vertical=0)),
                )
            )
            if i < len(labels) - 1:
                self._crumbs_row.controls.append(ft.Text(" / ", size=12))

        self._list.controls.clear()

        try:
            entries = list(self._cwd.iterdir())
        except PermissionError:
            entries = []

        dirs = sorted([p for p in entries if p.is_dir()], key=lambda p: p.name.lower())
        files = sorted([p for p in entries if p.is_file()], key=lambda p: p.name.lower())

        for d in dirs:
            self._list.controls.append(
                ft.ListTile(
                    leading=ft.Icon(ft.Icons.FOLDER),
                    title=ft.Text(d.name),
                    subtitle=ft.Text("Directory", size=11),
                    on_click=(lambda e, p=d: self._go_to(p)),
                    trailing=(
                        ft.TextButton(content=ft.Text("Select"), on_click=(lambda e, p=d: self._pick_dir(p)))
                        if self._allow_dirs
                        else None
                    ),
                )
            )

        for f in files:
            if self._file_exts and f.suffix.lower() not in self._file_exts:
                continue
            self._list.controls.append(
                ft.ListTile(
                    leading=ft.Icon(ft.Icons.INSERT_DRIVE_FILE),
                    title=ft.Text(f.name),
                    subtitle=ft.Text("File", size=11),
                    on_click=(lambda e, p=f: self._pick_file(p)) if self._allow_files else None,
                )
            )

        # Only call update() after being added to the page
        if not initial:
            self.view.update()


def main(page: ft.Page) -> None:
    picked = ft.Text("No selection yet.", selectable=True)

    def on_pick(result: PickResult) -> None:
        picked.value = f"Picked {result.kind}: {result.path}"
        picked.update()

    picker = SandboxedFilePicker(
        root_dir="./data",
        start_subdir="pipelines",
        allow_files=True,
        allow_dirs=False,
        file_exts={".dill"},
        title="Choose a result file",
        on_pick=on_pick,
    )

    page.add(picker.view, ft.Divider(), picked)


if __name__ == '__main__':
    ft.run(main)
