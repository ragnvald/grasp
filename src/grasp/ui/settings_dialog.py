from __future__ import annotations

from grasp.qt_compat import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QLineEdit,
    QVBoxLayout,
)
from grasp.settings import AppSettings


MODEL_OPTIONS = [
    "gpt-4o-mini",
    "gpt-4.1-mini",
    "gpt-4.1",
    "gpt-5-mini",
    "gpt-5",
]


class SettingsDialog(QDialog):
    def __init__(self, settings: AppSettings, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("AI Settings")
        self.resize(480, 220)

        layout = QVBoxLayout(self)

        intro = QLabel(
            "Choose the default OpenAI model and API settings used when the app attempts AI understanding and candidate ranking."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        form = QFormLayout()
        self.model_combo = QComboBox()
        self.model_combo.setEditable(True)
        self.model_combo.addItems(MODEL_OPTIONS)
        self.model_combo.setCurrentText(settings.openai_model)
        form.addRow("OpenAI model", self.model_combo)

        self.api_key_edit = QLineEdit(settings.openai_api_key)
        self.api_key_edit.setEchoMode(QLineEdit.Password)
        self.api_key_edit.setPlaceholderText("Optional if OPENAI_API_KEY is already set in the environment")
        form.addRow("API key", self.api_key_edit)

        self.endpoint_edit = QLineEdit(settings.openai_endpoint)
        form.addRow("Endpoint", self.endpoint_edit)

        self.timeout_edit = QLineEdit(str(settings.openai_timeout_s))
        form.addRow("Timeout (s)", self.timeout_edit)

        self.failures_edit = QLineEdit(str(settings.openai_max_consecutive_failures))
        form.addRow("Failover threshold", self.failures_edit)

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def to_settings(self) -> AppSettings:
        timeout_s = float(self.timeout_edit.text().strip() or "20")
        max_failures = int(self.failures_edit.text().strip() or "2")
        return AppSettings(
            openai_model=self.model_combo.currentText().strip() or MODEL_OPTIONS[0],
            openai_api_key=self.api_key_edit.text().strip(),
            openai_endpoint=self.endpoint_edit.text().strip(),
            openai_timeout_s=timeout_s,
            openai_max_consecutive_failures=max(1, max_failures),
        )

