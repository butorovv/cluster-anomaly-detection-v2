#include "system.h"
#include "viewhelpers.h"

#include <QFrame>
#include <QHeaderView>
#include <QStandardItemModel>
#include <QTableView>
#include <QVBoxLayout>

namespace {
QString withUnit(const QJsonValue &value, const QString &unit)
{
    return value.isDouble() ? numberText(value) + ' ' + unit : QStringLiteral("—");
}

QString probeText(int state)
{
    return state < 0 ? QStringLiteral("проверка…") : state == 1 ? QStringLiteral("доступен")
                                                                : QStringLiteral("недоступен");
}
}

SystemView::SystemView(QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->addWidget(sectionLabel(QStringLiteral("Состояние системы")));
    auto *panel = new QFrame;
    panel->setObjectName("panel");
    auto *summary = new QVBoxLayout(panel);
    summary->setContentsMargins(16, 11, 16, 11);
    health_ = plainLabel();
    progressText_ = plainLabel();
    clock_ = plainLabel();
    errors_ = plainLabel();
    errors_->setStyleSheet("color:#95501e");
    progress_ = new QProgressBar;
    for (auto *label : {health_, progressText_, clock_, errors_}) summary->addWidget(label);
    summary->addWidget(progress_);
    layout->addWidget(panel);
    layout->addWidget(sectionLabel(QStringLiteral("Метрики Master")));
    metricsTime_ = plainLabel();
    layout->addWidget(metricsTime_);
    auto *table = new QTableView;
    metricsModel_ = new QStandardItemModel(this);
    metricsModel_->setHorizontalHeaderLabels({QStringLiteral("Показатель"), QStringLiteral("Значение")});
    table->setModel(metricsModel_);
    table->setAlternatingRowColors(true);
    table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    table->verticalHeader()->hide();
    table->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    layout->addWidget(table, 1);
}

void SystemView::refresh(const Poller &poller)
{
    const auto &store = poller.store();
    const auto &summary = store.summary;
    const auto &metrics = store.metrics;
    const auto &errors = poller.errors();
    health_->setText(QStringLiteral("Master %1     ·     Готовность %2     ·     Адрес %3")
        .arg(probeText(poller.health()), poller.readiness() < 0 ? QStringLiteral("проверка…")
                                   : poller.readiness() == 1 ? QStringLiteral("готов") : QStringLiteral("не готов"),
             poller.settings().masterUrl.toDisplayString()));
    setProgress(progress_, summary);
    const auto sync = summary.isEmpty() ? QStringLiteral("сводка недоступна")
        : poller.stale() ? QStringLiteral("данные неактуальны")
        : poller.synchronized() ? QStringLiteral("число записей совпадает со сводкой Master")
                                : QStringLiteral("синхронизация…");
    progressText_->setText(QStringLiteral("Групповых тиков Master %1   ·   В клиенте %2   ·   "
                                         "Завершённых событий %3 / %4\nСверка — %5%6")
        .arg(numberText(summary.value("group_ticks"))).arg(store.resultCount()).arg(store.events().size())
        .arg(numberText(summary.value("parent_events")), sync,
             summary.value("complete").toBool() ? QStringLiteral("   ·   Сетка обработана") : QString{}));
    clock_->setText(QStringLiteral("Системное время %1   ·   Последняя телеметрия %2\n"
                                  "Фактический интервал приёма Master %3   ·   Первый результат %4")
        .arg(timeText(QDateTime::currentDateTimeUtc()), timeText(store.latestTime()), receiptDuration(summary),
             timeText(summary.value("first_result_at"))));
    QStringList messages;
    for (auto it = errors.cbegin(); it != errors.cend(); ++it) messages << it.key() + "  " + it.value();
    errors_->setVisible(!messages.isEmpty());
    errors_->setText(messages.join(QStringLiteral("   ·   ")));
    metricsTime_->setText(metrics.isEmpty() ? QStringLiteral("Метрики недоступны")
                        : QStringLiteral("Получен снимок %1").arg(timeText(metrics.value("at"))));
    if (metrics == lastMetrics_) return;
    lastMetrics_ = metrics;
    metricsModel_->removeRows(0, metricsModel_->rowCount());
    const auto add = [this](const QString &label, const QString &value) {
        metricsModel_->appendRow({new QStandardItem(label), new QStandardItem(value)});
    };
    const auto process = metrics.value("process").toObject();
    const auto resource = [&](const QString &key, const QString &name, const QString &unit) {
        if (process.value(key).isDouble()) add(name, withUnit(process.value(key), unit).trimmed());
    };
    resource("cpu_seconds", QStringLiteral("CPU процесса, накопленное время"), QStringLiteral("с"));
    resource("rss_bytes", QStringLiteral("RSS процесса"), QStringLiteral("байт"));
    resource("heap_bytes", QStringLiteral("Куча Go"), QStringLiteral("байт"));
    resource("goroutines", QStringLiteral("Горутины Go"), {});
    resource("pid", QStringLiteral("PID Master"), {});
    for (const auto &category : {QStringLiteral("counters"), QStringLiteral("gauges"), QStringLiteral("histograms")}) {
        const auto section = metrics.value(category).toObject();
        for (auto it = section.constBegin(); it != section.constEnd(); ++it) {
            if (category == "gauges"
                && ((it.key().startsWith("results_kafka_backlog_") && it.key() != "results_kafka_backlog_sample_valid"
                     && section.value("results_kafka_backlog_sample_valid").toInt() != 1)
                    || (it.key() == "results_kafka_durable_confirmations_per_second"
                        && section.value("results_kafka_durable_rate_valid").toInt() != 1)))
                continue;
            if (it.value().isDouble()) {
                add(it.key(), numberText(it.value()));
            } else if (it.value().isObject()) {
                const auto detail = it.value().toObject();
                add(it.key(), QStringLiteral("Замеров %1   ·   Сумма %2   ·   Макс. %3")
                    .arg(numberText(detail.value("count")), withUnit(detail.value("sum_seconds"), QStringLiteral("с")),
                         withUnit(detail.value("max_seconds"), QStringLiteral("с"))));
            }
        }
    }
}
