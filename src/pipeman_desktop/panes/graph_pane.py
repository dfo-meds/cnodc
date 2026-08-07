from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.util import BatchOpenState
from pipeman_desktop.state import DisplayChange, ApplicationState
import typing as t
from autoinject import injector
import gcapp.i18n.base as i18n
from pipeman_desktop.components.ocproc_graph import OCProc2Graph


class GraphPane(BasePane):

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._oc2graph: t.Optional[OCProc2Graph] = None
        self._pane_id: str | None = None

    def on_init(self):
        self._oc2graph = OCProc2Graph(self.app.middle, self.app)
        self.app.middle.add(self._oc2graph, text=i18n.tr("pane.graph"), sticky='NSEW')
        self._pane_id = self.app.middle.tabs()[-1]

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.ACTION:
            if app_state.batch_state == BatchOpenState.OPEN:
                self._oc2graph.update_graph(True)
            else:
                self._oc2graph.clear_graph_data()
        elif change_type & (DisplayChange.RECORD | DisplayChange.BATCH_STATE):
            if app_state.batch_state == BatchOpenState.OPEN:
                self._oc2graph.update_graph()
            else:
                self._oc2graph.clear_graph_data()
        if change_type & DisplayChange.LANGUAGE:
            if self._pane_id is not None:
                self.app.middle.tab(self._pane_id, text=i18n.tr("pane.graph"))
