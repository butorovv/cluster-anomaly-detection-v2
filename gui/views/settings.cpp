#include "settings.h"

#include <QDialogButtonBox>
#include <QFormLayout>
#include <QJsonObject>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QSpinBox>
#include <QVBoxLayout>

SettingsDialog::SettingsDialog(const ConnectionSettings &settings, QWidget *parent) : QDialog(parent)
{
    setWindowTitle(tr("Настройки подключения"));
    resize(540, 230);
    auto *layout = new QVBoxLayout(this);
    auto *form = new QFormLayout;
    url_ = new QLineEdit(settings.masterUrl.toString());
    execution_ = new QLineEdit(settings.execution);
    interval_ = new QSpinBox;
    interval_->setRange(250, 60000);
    interval_->setSingleStep(250);
    interval_->setSuffix(tr(" мс"));
    interval_->setValue(settings.intervalMs);
    form->addRow(tr("Адрес Master API"), url_);
    form->addRow(tr("Execution ID"), execution_);
    form->addRow(tr("Интервал обновления"), interval_);
    layout->addLayout(form);
    status_ = new QLabel;
    status_->setWordWrap(true);
    layout->addWidget(status_);
    auto *buttons = new QDialogButtonBox;
    auto *save = buttons->addButton(tr("Сохранить"), QDialogButtonBox::AcceptRole);
    buttons->addButton(tr("Отмена"), QDialogButtonBox::RejectRole);
    check_ = buttons->addButton(tr("Проверить подключение"), QDialogButtonBox::ActionRole);
    layout->addWidget(buttons);
    connect(save, &QPushButton::clicked, this, [this] { if (validate()) accept(); });
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);
    connect(check_, &QPushButton::clicked, this, &SettingsDialog::check);
}

ConnectionSettings SettingsDialog::settings() const
{
    return {QUrl(url_->text().trimmed()), execution_->text().trimmed().toLower(), interval_->value()};
}

bool SettingsDialog::validate()
{
    const auto values = settings();
    if (!MasterClient::validUrl(values.masterUrl)) {
        status_->setText(tr("Укажите действительный HTTP(S)-адрес Master."));
        return false;
    }
    if (!values.execution.isEmpty() && !MasterClient::validExecution(values.execution)) {
        status_->setText(tr("Execution ID должен быть UUID."));
        return false;
    }
    return true;
}

void SettingsDialog::check()
{
    if (!validate()) return;
    const auto values = settings();
    check_->setDisabled(true);
    url_->setDisabled(true);
    execution_->setDisabled(true);
    status_->setText(tr("Проверка…"));
    client_.get(values.masterUrl, "/healthz", {}, false, [this, values](HttpResult live) {
        if (!live.ok()) {
            finishCheck(tr("Master недоступен (%1).").arg(live.error));
            return;
        }
        client_.get(values.masterUrl, "/readyz", {}, false, [this, values](HttpResult ready) {
            if (!ready.ok() || values.execution.isEmpty()) {
                finishCheck(ready.ok() ? tr("Master доступен и готов.") : tr("Master ответил, но система не готова (%1).").arg(ready.error));
                return;
            }
            QUrlQuery query;
            query.addQueryItem("execution_id", values.execution);
            client_.get(values.masterUrl, "/v1/summary", query, true, [this, values](HttpResult summary) {
                const bool ok = summary.ok() && summary.json.object().value("execution_id").toString() == values.execution;
                finishCheck(ok ? tr("Master готов, сводка запуска доступна.")
                               : tr("Master готов, сводка запуска недоступна (%1).")
                                     .arg(summary.error.isEmpty() ? tr("неверный ответ") : summary.error));
            });
        });
    });
}

void SettingsDialog::finishCheck(const QString &message)
{
    check_->setEnabled(true);
    url_->setEnabled(true);
    execution_->setEnabled(true);
    status_->setText(message);
}
