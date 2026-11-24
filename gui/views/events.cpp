#include "events.h"
#include "viewhelpers.h"

#include <QComboBox>
#include <QHeaderView>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QSignalBlocker>
#include <QTableView>
#include <QVBoxLayout>

namespace {
QString closureText(const QString &reason)
{
    if (reason == "negative") return QStringLiteral("Отрицательное решение");
    if (reason == "unknown") return QStringLiteral("Неопределённое решение");
    if (reason == "gap") return QStringLiteral("Разрыв телеметрии");
    if (reason == "missing_tail") return QStringLiteral("Нет наблюдений в конце окна");
    if (reason == "window") return QStringLiteral("Конец окна обработки");
    return reason;
}

QString yesNo(const QJsonValue &value)
{
    return value.isBool() ? (value.toBool() ? QStringLiteral("да") : QStringLiteral("нет")) : QStringLiteral("—");
}
}

Events::Events(QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    auto *bar = new QHBoxLayout;
    bar->addWidget(sectionLabel(tr("Завершённые события")));
    bar->addStretch();
    bar->addWidget(new QLabel(tr("Группа")));
    group_ = new QComboBox;
    group_->setAccessibleName(tr("Фильтр событий по группе"));
    group_->addItem(tr("Все группы"), QString{});
    bar->addWidget(group_);
    layout->addLayout(bar);
    count_ = plainLabel();
    layout->addWidget(count_);
    table_ = new QTableView;
    model_ = new EventTable(this);
    table_->setModel(model_);
    table_->setSelectionBehavior(QAbstractItemView::SelectRows);
    table_->setSelectionMode(QAbstractItemView::SingleSelection);
    table_->setAlternatingRowColors(true);
    table_->verticalHeader()->hide();
    table_->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    layout->addWidget(table_, 3);
    auto *detailBar = new QHBoxLayout;
    detailBar->addWidget(sectionLabel(tr("Подробности события")));
    detailBar->addStretch();
    open_ = new QPushButton(tr("Открыть интервал диагностики"));
    detailBar->addWidget(open_);
    layout->addLayout(detailBar);
    details_ = new QPlainTextEdit;
    details_->setReadOnly(true);
    details_->setMinimumHeight(146);
    layout->addWidget(details_, 2);
    connect(group_, &QComboBox::currentIndexChanged, this, &Events::filterChanged);
    connect(table_->selectionModel(), &QItemSelectionModel::currentChanged, this, &Events::showDetails);
    connect(open_, &QPushButton::clicked, this, [this] {
        if (const auto *event = model_->at(table_->currentIndex().row())) {
            const auto selected = *event;
            emit eventSelected(selected.group, selected.detected, selected.closed);
        }
    });
    showDetails();
}

void Events::clear()
{
    previousCount_ = -1;
    group_->setCurrentIndex(0);
}

void Events::refresh(const MonitorStore &store, const QDateTime &from, const QDateTime &to)
{
    const auto selected = group_->currentData().toString();
    QStringList groups = store.groups();
    for (const auto &event : store.events())
        if (!groups.contains(event.group)) groups << event.group;
    QStringList shown;
    for (int i = 1; i < group_->count(); ++i) shown << group_->itemData(i).toString();
    if (shown != groups) {
        const QSignalBlocker blocker(group_);
        group_->clear();
        group_->addItem(tr("Все группы"), QString{});
        for (const auto &group : groups) group_->addItem(group, group);
        group_->setCurrentIndex(qMax(0, group_->findData(selected)));
    }
    const auto filter = group_->currentData().toString();
    if (store.events().size() == previousCount_ && filter == previousGroup_
        && from == previousFrom_ && to == previousTo_) return;
    QString selectedId;
    if (const auto *event = model_->at(table_->currentIndex().row())) selectedId = event->id;
    model_->replace(store.events(), filter, from, to);
    previousCount_ = store.events().size();
    previousGroup_ = filter;
    previousFrom_ = from;
    previousTo_ = to;
    const auto row = model_->rowOf(selectedId);
    if (row >= 0) table_->selectRow(row);
    showDetails();
    count_->setText(from.isValid() && to.isValid()
        ? tr("Пересекают интервал %1 — %2    ·    Показано %3 из %4")
              .arg(timeText(from), timeText(to)).arg(model_->rowCount()).arg(previousCount_)
        : tr("Показано %1 из %2    ·    Временной отбор доступен в HISTORY")
              .arg(model_->rowCount()).arg(previousCount_));
}

void Events::showDetails()
{
    const auto *event = model_->at(table_->currentIndex().row());
    open_->setEnabled(event != nullptr);
    if (!event) {
        details_->setPlainText(tr("Выберите событие в таблице."));
        return;
    }
    const auto &p = event->payload;
    QStringList lines{
        tr("%1   ·   %2 с   ·   %3").arg(event->group, numberText(p.value("duration_seconds")), event->votes()),
        tr("Обнаружено %1   ·   Завершено %2").arg(timeText(event->detected), timeText(event->closed)),
        tr("Первое свидетельство %1   ·   Последний положительный тик %2")
            .arg(timeText(p.value("first_evidence")), timeText(p.value("last_positive"))),
        tr("Причина завершения — %1   ·   Положительных тиков %2")
            .arg(closureText(p.value("closure_reason").toString()), numberText(p.value("positive_ticks"))),
        tr("Обрезано слева окном %1   ·   Справа окном %2   ·   Справа пропуском %3")
            .arg(yesNo(p.value("left_censored_by_window")), yesNo(p.value("right_censored_by_window")),
                 yesNo(p.value("right_censored_by_missing"))),
        tr("Узлы IF %1").arg(listText(p.value("if_hosts"))),
        tr("Узлы-свидетели %1").arg(listText(p.value("witness_hosts"))),
        tr("ID %1").arg(event->id),
        tr("Интервалы активных голосов (UTC)")
    };
    for (const auto &value : p.value("vote_segments").toArray()) {
        const auto span = value.toObject();
        lines << QString("%1 — %2   %3").arg(timeText(span.value("start")), timeText(span.value("end")),
                                               span.value("votes").toString());
    }
    details_->setPlainText(lines.join('\n'));
}
