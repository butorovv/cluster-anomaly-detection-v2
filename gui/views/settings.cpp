#include "settings.h"

#include <QHBoxLayout>
#include <QJsonObject>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QSpinBox>
#include <QVBoxLayout>

SettingsForm::SettingsForm(const ConnectionSettings &initial, QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    url_ = new QLineEdit(initial.masterUrl.toString());
    execution_ = new QLineEdit(initial.execution);
    interval_ = new QSpinBox;
    interval_->setRange(250, 60000);
    interval_->setSingleStep(250);
    interval_->setSuffix(tr(" мс"));
    interval_->setValue(initial.intervalMs);
    for (const auto &field : {qMakePair(tr("Адрес REST API"), static_cast<QWidget *>(url_)),
                             qMakePair(tr("Execution ID"), static_cast<QWidget *>(execution_)),
                             qMakePair(tr("Интервал обновления"), static_cast<QWidget *>(interval_))}) {
        auto *caption = new QLabel(field.first);
        caption->setBuddy(field.second);
        layout->addWidget(caption);
        layout->addWidget(field.second);
        layout->addSpacing(7);
    }
    status_ = new QLabel;
    status_->setWordWrap(true);
    layout->addWidget(status_);
    auto *buttons = new QHBoxLayout;
    auto *attach = new QPushButton(tr("Подключиться"));
    attach->setObjectName("primary");
    auto *save = new QPushButton(tr("Сохранить настройки"));
    save->setObjectName("soft");
    check_ = new QPushButton(tr("Проверить подключение"));
    for (auto *action : {attach, save, check_}) buttons->addWidget(action);
    buttons->addStretch();
    layout->addLayout(buttons);
    connect(attach, &QPushButton::clicked, this, [this] { if (validate()) emit submitted(settings(), true); });
    connect(save, &QPushButton::clicked, this, [this] {
        if (!validate()) return;
        emit submitted(settings(), false);
        status_->setText(tr("Настройки сохранены."));
    });
    connect(check_, &QPushButton::clicked, this, &SettingsForm::check);
}

ConnectionSettings SettingsForm::settings() const
{
    return {QUrl(url_->text().trimmed()), execution_->text().trimmed().toLower(), interval_->value()};
}

bool SettingsForm::validate()
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

void SettingsForm::check()
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

void SettingsForm::finishCheck(const QString &message)
{
    check_->setEnabled(true);
    url_->setEnabled(true);
    execution_->setEnabled(true);
    status_->setText(message);
}
