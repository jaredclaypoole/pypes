import os
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import dill
import numpy as np
import pandas as pd
import flet as ft

from pydantic import BaseModel
from omegaconf import DictConfig

from pypes.core.mytyping import (
    FullStepOutput, StepOutputBase, FullDepsDict,
)

from .flet_utils import SandboxedFilePicker, PickResult


default_results_dir = "./data"
RESULTS_DIR_PATH = Path(os.getenv("PYPES_RESULTS_DIR", default_results_dir))

if not RESULTS_DIR_PATH.exists():
    raise FileNotFoundError(
        f"Results dir not found at {RESULTS_DIR_PATH}. "
        "Set PYPES_RESULTS_DIR to point to a parent directory of saved results."
    )


from ..utils.pydantic_utils import(
    get_fields_dict as get_pydantic_fields_dict,
)

from ..utils.dictconfig_utils import(
    get_fields_dict as get_dict_config_fields_dict,
)

StepOutput = BaseModel



def get_fields_dict(model: Any) -> dict[str, Any]:
    if isinstance(model, BaseModel):
        return get_pydantic_fields_dict(model)
    elif isinstance(model, DictConfig):
        return get_dict_config_fields_dict(model)


class Filler:
    def __init__(self, fill_fcn: Callable[[], Any]):
        self.fill_fcn = fill_fcn

    def fill(self, the_iter: Iterable[Any]) -> Iterable[Any]:
        it = iter(the_iter)
        try:
            yield next(it)
        except StopIteration:
            return
        for x in it:
            yield self.fill_fcn()
            yield x



def full_step_output_list_to_exploded_df(
    the_list: list[FullStepOutput],
    include_deps: bool = False,
) -> pd.DataFrame:
    if include_deps:
        raise NotImplementedError()
    rows = []
    for full_step_output in the_list:
        output = full_step_output.output
        row_dict = {
            **dict(
                full_step_output=full_step_output,
                step_output=output,
            ),
            **get_fields_dict(output),
        }
        rows.append(row_dict)
    return pd.DataFrame(rows)


class DfFilter:
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


@dataclass(frozen=True, eq=False)
class FieldEqFilter(DfFilter):
    field_name: str
    value: Any

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df[self.field_name] == self.value]


class FilterableDf:
    def __init__(
        self,
        df: pd.DataFrame,
    ):
        self._df0 = df.copy()
        self._df = self._df0

        self.mask_by_label: dict[str, np.ndarray] = {}
        self.filters: list[DfFilter] = []
        self.callbacks: list[Callable[[], None]] = []

    @property
    def df0(self) -> pd.DataFrame:
        return self._df0.copy()

    @property
    def df(self) -> pd.DataFrame:
        return self._df.copy()

    def update(self, notify: bool = True) -> None:
        self._df = self._df0
        full_mask = np.ones(len(self._df), dtype=bool)
        for _mask_label, new_mask in self.mask_by_label.items():
            full_mask &= new_mask
        self._df = self._df[full_mask]

        for filter in self.filters:
            self._df = filter.apply(self._df)
        if notify:
            self.notify()

    def add_filter(self, filter: DfFilter) -> None:
        self.filters.append(filter)
        self._df = filter.apply(self._df)
        self.notify()

    def notify(self) -> None:
        for callback in self.callbacks:
            callback()


class StepCard(ft.Container):
    def __init__(
        self,
        full_step_output: FullStepOutput,
        filtered_col: "FilteredStepColumn",
        index: int,
        expand=1,
    ):
        super().__init__(expand=expand)
        self.full_step_output = full_step_output
        self.step_output = self.full_step_output.output
        self.filtered_col = filtered_col
        self.index = index
        self.is_selected = bool(self.filtered_col.selection_mask[self.index])

        col = ft.Column(
            [
                ft.Row(
                    [
                        ft.Markdown(f"**{field_name}**:  ", selectable=True),
                        ft.Text(f"{value}", selectable=True),
                    ],
                    spacing=0,
                )
                for field_name, value in get_fields_dict(self.step_output).items()
            ],
            spacing=0,
        )
        self.container = ft.Container(
            col,
            margin=2,
            padding=3,
            expand=1,
            on_click=self.handle_click,
        )
        self.content = self.container
        self.do_update(update=False)

    def handle_click(self, *args) -> None:
        # self.is_selected = not self.is_selected
        self.filtered_col.set_is_selected(self.index, self.is_selected)
        # self.do_update()  # do_update will be called after selection propagation

    def do_update(self, *args, update: bool = True) -> None:
        self.is_selected = bool(self.filtered_col.selection_mask[self.index])
        self.container.border = ft.Border.all(2) if self.is_selected else None
        if update:
            self.container.update()


class FilteredStepColumn(ft.Container):
    def __init__(
        self,
        page: ft.Page,
        step_name: str,
        step_Df: FilterableDf,
        fso_browser: "FullStepOutputBrowser",
        show_only_selected: bool = False,
        expand=1,
    ):
        super().__init__(expand=expand)
        self.the_page = page
        self.step_name = step_name
        self.step_Df = step_Df
        self.step_Df.callbacks.append(self.handle_step_Df_update)
        self.selection_mask = np.zeros(len(step_Df.df0), dtype=bool)
        self.fso_browser = fso_browser
        self._show_only_selected = show_only_selected
        self.do_build(update=False)

    def do_build(self, *args, update: bool = True) -> None:
        self.filters_container = ft.Container()
        filters_col = ft.Column(
            [
                self.filters_container,
                # ft.Chip(
                #     label=ft.Text(f"filter"),
                #     leading=ft.Icon(ft.Icons.ADD),
                #     on_click=self.handle_add_filter,
                # )
            ],
        )
        self.larger_filters_container = ft.Container(filters_col)
        self.steps_container = ft.Container(expand=1)
        col = ft.Column(
            [
                ft.Markdown(f"## {self.step_name}", selectable=True),
                self.larger_filters_container,
                self.steps_container,
            ],
            expand=1,
        )
        self.content = ft.Container(
            col,
            # margin=5,
            padding=5,
            border=ft.Border.all(1),
            expand=1,
        )
        self.do_update(update=update)

    def do_update(self, *args, update: bool = True) -> None:
        filter_controls: list[ft.Control] = []
        for mask_label, mask in self.step_Df.mask_by_label.items():
            chip = ft.Chip(
                label=ft.Text(mask_label),
                on_click=lambda e: None
            )
            filter_controls.append(chip)
        if self.step_Df.filters:
            raise NotImplementedError(f"Filters container not yet visualized")
        filters_col = ft.Column(
            filter_controls,
        )
        self.filters_container.content = filters_col

        self.step_cards = [
            StepCard(
                full_step_output=full_step_output,
                filtered_col=self,
                index=i,
            )
            for i, full_step_output in self.step_Df.df["full_step_output"].items()
        ]
        controls = [
            ft.Row(card)
            for card in self.step_cards
        ]
        controls = list(Filler(ft.Divider).fill(controls))
        col = ft.Column(
            controls,
            spacing=1,
            expand=1,
            scroll=ft.ScrollMode.ALWAYS,
        )
        self.steps_container.content = col

        if update:
            self.filters_container.update()
            self.steps_container.update()

    def handle_step_Df_update(self, *args) -> None:
        self.do_update()

    def handle_add_filter(self, *args) -> None:
        modal = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Add a filter to {self.step_name}"),
            content=ft.Text("TODO"),
            actions=[
                ft.TextButton("Add", on_click=lambda e: self.the_page.pop_dialog()),
                ft.TextButton("Cancel", on_click=lambda e: self.the_page.pop_dialog()),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            # on_dismiss=lambda e: print("Modal dialog dismissed!"),
        )
        self.the_page.show_dialog(modal)

    def set_is_selected(self, index: int, is_selected: bool) -> None:
        self.selection_mask[index] = is_selected
        full_step_output = self.index_to_fso(index)
        self.fso_browser.propagate_selected(full_step_output)

    def set_selected_fsos(self, full_step_outputs: Iterable[FullStepOutput], *, update: bool = True) -> None:
        # prev_mask = self.selection_mask.copy()
        indices = list(self.fsos_to_indices(full_step_outputs))
        self.selection_mask[:] = False
        self.selection_mask[indices] = True
        # self.do_update(update=update)
        for step_card in self.step_cards:
            step_card.do_update(update=update)

    def index_to_fso(self, index: int) -> FullStepOutput:
        return self.step_Df.df0.iloc[index]["full_step_output"]

    def fsos_to_indices(self, full_step_outputs: Iterable[FullStepOutput]) -> list[int]:
        df = self.step_Df.df0
        s = df["full_step_output"]
        fsos_set = set(full_step_outputs)
        indices = s[s.isin(fsos_set)].index
        return list(indices)

    def set_show_only_selected(self, show_only_selected: bool, *, update: bool = True) -> None:
        if self._show_only_selected == show_only_selected:
            return
        self._show_only_selected = show_only_selected

        label = "show_only_selected"
        if self._show_only_selected:
            self.step_Df.mask_by_label[label] = self.selection_mask
        else:
            self.step_Df.mask_by_label.pop(label, None)
        self.step_Df.update(notify=False)

        self.do_update(update=update)

    def __del__(self):
        self.step_Df.callbacks.remove(self.handle_step_Df_update)


class FullStepOutputBrowser(ft.Container):
    def __init__(
        self,
        page: ft.Page,
        full_step_outputs: list[FullStepOutput],
        step_name: str,
        show_only_selected: bool = False,
        expand=1,
    ):
        super().__init__(expand=expand)
        self.the_page = page
        self.full_step_outputs = full_step_outputs
        self.step_name = step_name
        self.df = FullStepOutput.list_to_df(full_step_outputs)

        self.step_Df_by_step_name: dict[str, FilterableDf] = {}
        self.step_col_by_step_name: dict[str, FilteredStepColumn] = {}
        for col in self.df.columns:
            full_step_outputs: list[FullStepOutput] = list(self.df[col].unique())
            if not isinstance(fso:=full_step_outputs[0], FullStepOutput):
                raise AssertionError(f"{repr(fso)}")

            step_df = full_step_output_list_to_exploded_df(full_step_outputs)
            step_Df = FilterableDf(step_df)
            self.step_Df_by_step_name[col] = step_Df
            step_col = FilteredStepColumn(
                page=self.the_page,
                step_name=col,
                step_Df=step_Df,
                fso_browser=self,
                show_only_selected=show_only_selected,
            )
            self.step_col_by_step_name[col] = step_col

        step_cols = list(self.step_col_by_step_name.values())
        row = ft.Row(
            step_cols,
            vertical_alignment=ft.CrossAxisAlignment.START,
            expand=1,
        )
        self.content = row

    def propagate_selected(self, full_step_output: FullStepOutput) -> None:
        df = self.df
        df = df[df[full_step_output.step_name] == full_step_output]
        for step_name in df.columns:
            fsos_to_highlight = list(df[step_name].unique())
            self.step_col_by_step_name[step_name].set_selected_fsos(fsos_to_highlight)

    def set_show_only_selected(self, show_only_selected: bool) -> None:
        for step_col in self.step_col_by_step_name.values():
            step_col.set_show_only_selected(show_only_selected)


class ResultsViewer(ft.Container):
    def __init__(
        self,
        page: ft.Page,
        results_dict: dict[str, list[FullStepOutput]],
        expand=1,
    ):
        super().__init__(expand=expand)
        self.the_page = page
        self.results_dict = results_dict

        options = [
            ft.DropdownOption(key=name)
            for name in results_dict
        ]
        self.dropdown = ft.Dropdown(
            value=options[0].key,
            options=options,
            on_select=self.handle_dropdown_change,
        )

        self.toggle_switch = ft.Switch(
                label=f"Show only selected outputs",
                on_change=self.handle_toggle_switch,
            )
        dropdown_row = ft.Row(
            [
                ft.Text("Browsing: "),
                self.dropdown,
                ft.Container(width=100),
                self.toggle_switch,
            ],
        )
        self.view_container = ft.Container(
            expand=1,
        )

        col = ft.Column(
            [
                dropdown_row,
                self.view_container,
            ],
            expand=1,
        )
        self.content = col
        self.handle_dropdown_change(update=False)

    def handle_dropdown_change(self, *args, update: bool = True) -> None:
        step_name = self.dropdown.value.strip()
        outputs = self.results_dict[step_name]
        self.fso_browser = FullStepOutputBrowser(
            page=self.the_page,
            full_step_outputs=outputs,
            step_name=step_name,
            show_only_selected=self.toggle_switch.value,
        )
        self.view_container.content = self.fso_browser

        if update:
            self.update()

    def handle_toggle_switch(self, *args) -> None:
        value = self.toggle_switch.value
        self.fso_browser.set_show_only_selected(value)


class MyTabs(ft.Tabs):
    def __init__(
        self,
        tabs: list[ft.Tab],
        tab_controls: list[ft.Tab],
        selected_index: int = 0,
        expand=1,
        **kwargs,
    ):
        if len(tabs) != len(tab_controls):
            raise ValueError(f"There must be exactly as many tabs as tab_controls")

        self.tab_bar = ft.TabBar(
            tabs=list(tabs),
        )
        self.tab_bar_view = ft.TabBarView(
            controls=list(tab_controls),
            expand=1,
        )
        col = ft.Column(
            [
                self.tab_bar,
                self.tab_bar_view,
            ],
            expand=1,
        )

        super().__init__(
            content=col,
            length=len(tabs),
            selected_index=selected_index,
            expand=expand,
            **kwargs,
        )

    def add_tab(self, tab: ft.Tab, content: ft.Control, index: int|None = None) -> None:
        if index is None:
            index = self.length
        self.tab_bar.tabs.insert(index, tab)
        self.tab_bar_view.controls.insert(index, content)
        self.length += 1

    def remove_tab(self, index: int|None = None) -> None:
        if index is None:
            index = self.length - 1
        self.tab_bar.tabs.pop(index)
        self.tab_bar_view.controls.pop(index)
        self.length -= 1


class ResultsBrowser(ft.Container):
    def __init__(self, page: ft.Page, root_dir: str|Path, expand=1):
        super().__init__(expand=expand)
        self.the_page = page

        self._has_results_view_tab = False

        self.root_dir = Path(root_dir)
        subdir_path = self.root_dir / "pipelines"
        subdir = subdir_path.name if subdir_path.exists() else None

        self.file_picker = SandboxedFilePicker(
            root_dir=self.root_dir,
            start_subdir=subdir,
            allow_files=True,
            allow_dirs=False,
            file_exts={".dill"},
            on_pick=self.handle_file_pick,
        )
        self.picker_results_Text = ft.Text("Results file not yet chosen")

        choose_results_tab_control = ft.Column(
            [
                self.file_picker.view,
                ft.Divider(),
                self.picker_results_Text,
            ],
            expand=1,
        )

        self.mytabs = MyTabs(
            tabs=[
                ft.Tab(label="Choose results"),
            ],
            tab_controls=[
                choose_results_tab_control,
            ],
        )
        self.content = self.mytabs

    def handle_file_pick(self, pick_result: PickResult) -> None:
        self.results_fpath = pick_result.path
        rel_path = pick_result.rel_path
        self.picker_results_Text.value = f"Chosen results file: {rel_path}"

        with self.results_fpath.open('rb') as fdill:
            results_dict: dict[str, list[FullStepOutput]] = dill.load(fdill)
        self.results_viewer = ResultsViewer(page=self.the_page, results_dict=results_dict)

        if self._has_results_view_tab:
            self.mytabs.remove_tab()

        self.mytabs.add_tab(
            tab=ft.Tab(label="View results"),
            content=self.results_viewer,
        )
        self._has_results_view_tab = True
        self.the_page.update()


def main(page: ft.Page):
    print("Building")

    browser = ResultsBrowser(page=page, root_dir=RESULTS_DIR_PATH)
    page.add(browser)

    print("Done")
