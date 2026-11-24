#include "diagnostics.h"
#include "viewhelpers.h"
#include "models/nodetable.h"
#include "widgets/scorechart.h"

#include <QComboBox>
#include <QFormLayout>
#include <QFrame>
#include <QHeaderView>
#include <QScrollArea>
#include <QSignalBlocker>
#include <QTableView>
#include <QVBoxLayout>

namespace {
QString criterionText(const QJsonObject &object)
{
    const auto status = stateText(readState(object.value("state")));
    const auto reason = object.value("reason").toString();
    return reason.isEmpty() ? status : QString("%1 (%2)").arg(status, reason);
}
}

Diagnostics::Diagnostics(QWidget *parent) : QWidget(parent)
{
    auto *outer = new QVBoxLayout(this);
    outer->setContentsMargins(0, 0, 0, 0);
    auto *line = new QHBoxLayout;
    line->addWidget(sectionLabel(tr("Диагностика группы")));
    line->addStretch();
    groups_ = new QComboBox;
    groups_->setMinimumWidth(110);
    groups_->setAccessibleName(tr("Топологическая группа"));
    line->addWidget(groups_);
    outer->addLayout(line);
    auto *scroll = new QScrollArea;
    scroll->setFrameShape(QFrame::NoFrame);
    scroll->setWidgetResizable(true);
    outer->addWidget(scroll);
    auto *body = new QWidget;
    body->setObjectName("workspace");
    scroll->setWidget(body);
    auto *layout = new QVBoxLayout(body);
    layout->setContentsMargins(0, 0, 6, 0);
    layout->setSpacing(8);
    current_ = sectionLabel({});
    layout->addWidget(current_);
    auto *panel = new QFrame;
    panel->setObjectName("panel");
    auto *form = new QFormLayout(panel);
    form->setContentsMargins(13, 9, 13, 9);
    raw_ = plainLabel();
    temporal_ = plainLabel();
    coverage_ = plainLabel();
    outside_ = plainLabel();
    evidence_ = plainLabel();
    form->addRow(tr("Исходные критерии"), raw_);
    form->addRow(tr("После временной обработки"), temporal_);
    form->addRow(tr("Наблюдения"), coverage_);
    form->addRow(tr("AnnotateOutside"), outside_);
    form->addRow(tr("Свидетельства"), evidence_);
    layout->addWidget(panel);
    auto *graphTools = new QHBoxLayout;
    graphTools->addWidget(sectionLabel(tr("Диагностические показатели")));
    graphTools->addStretch();
    metric_ = new QComboBox;
    metric_->setAccessibleName(tr("Показатель на графике"));
    metric_->addItems({tr("R · оценка и порог"), tr("B · оценка и порог"),
                       tr("Локальный I2 · число узлов IF"), tr("Полнота наблюдений"),
                       tr("IF · доли внутри и вне группы"), tr("IF · разность долей (e)")});
    graphTools->addWidget(metric_);
    layout->addLayout(graphTools);
    chart_ = new ScoreChart;
    layout->addWidget(chart_);
    layout->addWidget(sectionLabel(tr("Локальный Isolation Forest")));
    nodes_ = new NodeTable(this);
    auto *table = new QTableView;
    table->setModel(nodes_);
    table->setMinimumHeight(234);
    table->setAlternatingRowColors(true);
    table->setSelectionBehavior(QAbstractItemView::SelectRows);
    table->verticalHeader()->hide();
    table->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    layout->addWidget(table);
    layout->addStretch();
    connect(groups_, &QComboBox::currentIndexChanged, this, &Diagnostics::selectionChanged);
    connect(metric_, &QComboBox::currentIndexChanged, this, &Diagnostics::selectionChanged);
    connect(chart_, &ScoreChart::timeSelected, this, &Diagnostics::timeSelected);
}

void Diagnostics::selectGroup(const QString &group)
{
    const int index = groups_->findText(group);
    if (index < 0) return;
    const QSignalBlocker blocker(groups_);
    groups_->setCurrentIndex(index);
}

void Diagnostics::refresh(const MonitorStore &store, const QDateTime &at, const QDateTime &from, const QDateTime &to)
{
    const auto desired = store.groups();
    QStringList shown;
    for (int i = 0; i < groups_->count(); ++i) shown << groups_->itemText(i);
    if (desired != shown) {
        const auto selected = groups_->currentText();
        const QSignalBlocker blocker(groups_);
        groups_->clear();
        groups_->addItems(desired);
        selectGroup(selected);
    }
    displayTick(store.tickAt(groups_->currentText(), at), at);
    refreshChart(store, at, from, to);
}

void Diagnostics::displayTick(const GroupTick *tick, const QDateTime &at)
{
    const auto group = groups_->currentText();
    const auto state = tick ? tick->state() : GroupState::Missing;
    current_->setText(tr("%1   ·   %2   ·   %3").arg(group.isEmpty() ? tr("Группа") : group, stateText(state), timeText(at)));
    current_->setStyleSheet("color:" + stateColor(state));
    nodes_->setTick(tick);
    if (!tick) {
        for (auto *label : {raw_, temporal_, coverage_, outside_, evidence_}) label->setText("—");
        return;
    }
    const auto &raw = tick->raw;
    const auto &t = tick->temporal;
    const auto local = t.value("local_i2").toObject();
    raw_->setText(tr("R %1   ·   B %2   ·   I2 %3")
        .arg(criterionText(raw.value("r").toObject()), criterionText(raw.value("b").toObject()), criterionText(local)));
    temporal_->setText(tr("R %1   ·   B %2   ·   I2 %3   ·   δ %4 с\nГолоса %5   ·   Итог %6")
        .arg(criterionText(t.value("r").toObject()), criterionText(t.value("b").toObject()),
             criterionText(t.value("i2").toObject()), numberText(t.value("delta_seconds")),
             listText(t.value("active_votes")), stateText(state)));
    const auto missing = raw.value("missing_hosts");
    coverage_->setText(tr("Полные наблюдения %1   ·   Пропуски %2   ·   Узлы без наблюдений %3\n"
                          "Доступные локальные результаты IF %4   ·   Аномальные %5")
        .arg(numberText(raw.value("complete_hosts")),
             missing.isArray() ? QString::number(missing.toArray().size()) : QString("—"),
             listText(missing), numberText(local.value("available")), numberText(local.value("anomalous"))));
    const auto support = raw.value("if").toObject();
    const auto groups = support.value("outside_groups");
    outside_->setText(tr("Другие группы %1\nВне группы доступно %2, аномально %3"
                         "   ·   Доля внутри s %4   ·   Доля вне outside_s %5   ·   Разность e %6")
        .arg(listText(groups), numberText(support.value("outside_available")),
             numberText(support.value("outside_anomalous")), numberText(support.value("s")),
             numberText(support.value("outside_s")), numberText(support.value("e"))));
    QStringList witnesses;
    for (const auto &w : tick->evidence.value("witness_refs").toArray()) {
        const auto ref = w.toObject();
        witnesses << QString("%1 · %2").arg(ref.value("criterion").toString(), timeText(ref.value("timestamp")));
    }
    const auto id = tick->evidence.value("event_id").toString();
    evidence_->setText(id.isEmpty() ? QStringLiteral("—")
                                   : tr("%1\nID события %2").arg(witnesses.join("   "), id));
}

void Diagnostics::refreshChart(const MonitorStore &store, const QDateTime &at,
                               const QDateTime &from, const QDateTime &to)
{
    const int index = metric_->currentIndex();
    const QStringList names{tr("R"), tr("B"), tr("Доступно IF"), tr("Полные наблюдения"),
                            tr("Доля внутри"), tr("Разность e")};
    QList<ChartSeries> series{{names.value(index), QColor("#2366b8"), {}}};
    if (index <= 2 || index == 4) {
        const auto second = index <= 1 ? tr("Порог") : index == 2 ? tr("Аномально IF") : tr("Доля вне");
        series.append(ChartSeries{second, QColor("#b35123"), {}});
    }
    if (!from.isValid() || !to.isValid() || from > to) {
        chart_->setSeries({}, from, to, at);
        return;
    }
    const auto &timeline = store.timeline(groups_->currentText());
    const auto end = timeline.upperBound(to.toMSecsSinceEpoch());
    for (auto it = timeline.lowerBound(from.toMSecsSinceEpoch()); it != end; ++it) {
        const auto &tick = it.value();
        QJsonValue first, second;
        switch (index) {
        case 0:
        case 1: {
            const auto c = tick.raw.value(index == 0 ? "r" : "b").toObject();
            first = c.value("score");
            second = c.value("threshold");
            break;
        }
        case 2: {
            const auto i2 = tick.temporal.value("local_i2").toObject();
            first = i2.value("available");
            second = i2.value("anomalous");
            break;
        }
        case 3:
            first = tick.raw.value("complete_hosts");
            break;
        case 4: {
            const auto support = tick.raw.value("if").toObject();
            first = support.value("s");
            second = support.value("outside_s");
            break;
        }
        case 5:
            first = tick.raw.value("if").toObject().value("e");
            break;
        }
        if (first.isDouble()) series[0].points.append(QPointF(it.key(), first.toDouble()));
        if (second.isDouble()) series[1].points.append(QPointF(it.key(), second.toDouble()));
    }
    chart_->setSeries(std::move(series), from, to, at);
}
