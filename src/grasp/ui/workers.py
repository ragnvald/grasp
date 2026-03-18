from __future__ import annotations

import inspect
import traceback

from grasp.qt_compat import QObject, QRunnable, Signal, Slot


class WorkerSignals(QObject):
    result = Signal(object)
    error = Signal(str)
    progress = Signal(int)
    status = Signal(str)
    finished = Signal()


class FunctionWorker(QRunnable):
    def __init__(self, fn, *args, **kwargs) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @Slot()
    def run(self) -> None:
        try:
            bound_kwargs = dict(self.kwargs)
            signature = inspect.signature(self.fn)
            if "status_callback" in signature.parameters and "status_callback" not in bound_kwargs:
                bound_kwargs["status_callback"] = self.signals.status.emit
            if "progress_callback" in signature.parameters and "progress_callback" not in bound_kwargs:
                bound_kwargs["progress_callback"] = self.signals.progress.emit
            result = self.fn(*self.args, **bound_kwargs)
            self.signals.result.emit(result)
        except Exception:
            self.signals.error.emit(traceback.format_exc())
        finally:
            self.signals.finished.emit()

