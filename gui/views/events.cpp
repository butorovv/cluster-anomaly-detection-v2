#include "events.h"
#include "viewhelpers.h"
#include "widgets/eventhistogram.h"
#include "widgets/utcrange.h"

#include <QComboBox>
#include <QFileDialog>
#include <QFormLayout>
#include <QFrame>
#include <QHeaderView>
#include <QMessageBox>
#include <QPushButton>
#include <QSaveFile>
#include <QScrollBar>
#include <QSignalBlocker>
#include <QSortFilterProxyModel>
#include <QTableView>
#include <QTimeZone>
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
}

Events::Events(QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(15);
    auto *bar = new QHBoxLayout;
    auto *heading = new QVBoxLayout;
    heading->setSpacing(4);
    heading->addWidget(sectionLabel(tr("Журнал аномалий")));
    heading->addWidget(new QLabel(tr("Завершённые события")));
    bar->addLayout(heading);
    bar->addStretch();
    auto *exportButton = new QPushButton(tr("Экспорт CSV"));
    exportButton->setObjectName("soft");
    bar->addWidget(exportButton);
    layout->addLayout(bar);
    auto *filterCard = new QFrame;
    filterCard->setObjectName("panel");
    auto *filterLayout = new QVBoxLayout(filterCard);
    filterLayout->setContentsMargins(15, 13, 15, 13);
    auto *filterHeading = new QHBoxLayout;
    filterHeading->addWidget(sectionLabel(tr("Фильтры событий")));
    filterHeading->addStretch();
    filterHeading->addWidget(new QLabel(tr("По началу события (UTC)")));
    filterLayout->addLayout(filterHeading);
    auto *filters = new QHBoxLayout;
    group_ = new QComboBox;
    group_->setAccessibleName(tr("Фильтр событий по группе"));
    criterion_ = new QComboBox;
    criterion_->setAccessibleName(tr("Фильтр событий по критерию"));
    criterion_->addItem(tr("Все критерии"), QString{});
    for (const QString &criterion : {QStringLiteral("R"), QStringLiteral("B"), QStringLiteral("I2")})
        criterion_->addItem(criterion, criterion);
    for (const auto &field : {qMakePair(tr("Группа"), group_), qMakePair(tr("Критерий"), criterion_)}) {
        auto *column = new QVBoxLayout;
        column->setSpacing(5);
        column->addWidget(new QLabel(field.first));
        column->addWidget(field.second);
        filters->addLayout(column);
    }
    range_ = new UtcRange(tr("Начало периода"), tr("Конец периода (не включительно)"));
    filters->addWidget(range_, 1, Qt::AlignTop);
    filterLayout->addLayout(filters);
    auto *quick = new QHBoxLayout;
    for (const auto &period : {qMakePair(tr("Весь запуск"), 0), qMakePair(tr("Последний час"), 3600),
                                qMakePair(tr("Последние 15 мин"), 900), qMakePair(tr("Сбросить фильтры"), -1)}) {
        auto *button = new QPushButton(period.first);
        quick->addWidget(button);
        connect(button, &QPushButton::clicked, this, [this, seconds = period.second] {
            if (seconds == -1) {
                const QSignalBlocker first(group_), second(criterion_);
                group_->setCurrentIndex(0);
                criterion_->setCurrentIndex(0);
                state_.group.clear();
                state_.criterion.clear();
                table_->sortByColumn(1, Qt::DescendingOrder);
            }
            if (seconds > 0 && maximum_.isValid())
                setPeriod(qMax(minimum_, maximum_.addSecs(-seconds)), maximum_);
            else
                setPeriod();
        });
    }
    quick->addStretch();
    filterLayout->addLayout(quick);
    layout->addWidget(filterCard);
    auto *histogramCard = new QFrame;
    histogramCard->setObjectName("panel");
    auto *histogramLayout = new QVBoxLayout(histogramCard);
    histogramLayout->setContentsMargins(15, 13, 15, 5);
    histogramLayout->addWidget(sectionLabel(tr("Распределение событий по времени")));
    auto *histogramCaption = new QLabel(tr("По времени начала · выберите столбец для фильтрации"));
    histogramCaption->setObjectName("small");
    histogramLayout->addWidget(histogramCaption);
    histogram_ = new EventHistogram;
    histogramLayout->addWidget(histogram_);
    layout->addWidget(histogramCard);
    count_ = plainLabel();
    count_->setObjectName("small");
    auto *sortBar = new QHBoxLayout;
    sortBar->addWidget(count_);
    sortBar->addStretch();
    auto *sortOrder = new QComboBox;
    sortOrder->setAccessibleName(tr("Сортировка"));
    sortOrder->addItems({tr("Сначала новые"), tr("Сначала старые"), tr("Сначала длительные")});
    sortBar->addWidget(sortOrder);
    layout->addLayout(sortBar);

    auto *body = new QHBoxLayout;
    body->setSpacing(15);
    table_ = new QTableView;
    table_->setObjectName("journalTable");
    model_ = new EventTable(this);
    sort_ = new QSortFilterProxyModel(this);
    sort_->setSourceModel(model_);
    sort_->setSortRole(Qt::UserRole);
    table_->setModel(sort_);
    table_->setSortingEnabled(true);
    table_->sortByColumn(1, Qt::DescendingOrder);
    table_->setSelectionBehavior(QAbstractItemView::SelectRows);
    table_->setSelectionMode(QAbstractItemView::SingleSelection);
    table_->setWordWrap(false);
    table_->setShowGrid(false);
    table_->verticalHeader()->hide();
    table_->verticalHeader()->setDefaultSectionSize(44);
    table_->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    table_->horizontalHeader()->setSectionResizeMode(1, QHeaderView::ResizeToContents);
    table_->horizontalHeader()->setSectionResizeMode(2, QHeaderView::ResizeToContents);
    table_->setFixedHeight(385);
    body->addWidget(table_, 7, Qt::AlignTop);

    auto *card = new QFrame;
    card->setObjectName("panel");
    auto *details = new QVBoxLayout(card);
    details->setContentsMargins(20, 15, 20, 18);
    details->setSpacing(10);
    title_ = sectionLabel({});
    details->addWidget(title_);
    auto *values = new QFormLayout;
    values->setSpacing(9);
    detected_ = plainLabel(); closed_ = plainLabel(); duration_ = plainLabel(); votes_ = plainLabel();
    values->addRow(tr("Обнаружено"), detected_);
    values->addRow(tr("Завершено"), closed_);
    values->addRow(tr("Длительность"), duration_);
    values->addRow(tr("Активные критерии"), votes_);
    details->addLayout(values);
    open_ = new QPushButton(tr("Открыть в диагностике"));
    open_->setObjectName("primary");
    details->addWidget(open_);
    technicalToggle_ = new QPushButton(tr("Технические подробности   ›"));
    technicalToggle_->setObjectName("technicalDisclosure");
    technicalToggle_->setCheckable(true);
    details->addWidget(technicalToggle_);
    technical_ = plainLabel();
    technical_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    technical_->hide();
    details->addWidget(technical_);
    body->addWidget(card, 5, Qt::AlignTop);
    layout->addLayout(body);
    layout->addStretch();

    connect(group_, &QComboBox::currentIndexChanged, this, [this] {
        state_.group = group_->currentData().toString();
        state_.event.clear();
        emit filterChanged();
    });
    connect(criterion_, &QComboBox::currentIndexChanged, this, [this] {
        state_.criterion = criterion_->currentData().toString();
        emit filterChanged();
    });
    connect(range_, &UtcRange::edited, this, [this] {
        if (range_->validate()) setPeriod(range_->from(), range_->to());
    });
    connect(histogram_, &EventHistogram::intervalSelected, this, &Events::setPeriod);
    connect(exportButton, &QPushButton::clicked, this, &Events::exportEvents);
    connect(sortOrder, &QComboBox::activated, this, [this](int index) {
        table_->sortByColumn(index == 2 ? 3 : 1, index == 1 ? Qt::AscendingOrder : Qt::DescendingOrder);
    });
    connect(table_->horizontalHeader(), &QHeaderView::sortIndicatorChanged, this,
            [sortOrder](int column, Qt::SortOrder order) {
        sortOrder->setCurrentIndex(column == 3 && order == Qt::DescendingOrder ? 2
                                   : column == 1 ? int(order == Qt::AscendingOrder) : -1);
    });
    connect(technicalToggle_, &QPushButton::toggled, technical_, &QWidget::setVisible);
    connect(table_->selectionModel(), &QItemSelectionModel::currentChanged, this, &Events::showDetails);
    connect(open_, &QPushButton::clicked, this, [this] {
        if (const auto *event = selectedEvent()) {
            const auto copy = *event;
            emit eventSelected(copy.group, copy.detected, copy.closed);
        }
    });
    showDetails();
}

EventViewContext Events::context() const
{
    auto state = state_;
    state.sortColumn = table_->horizontalHeader()->sortIndicatorSection();
    state.sortOrder = table_->horizontalHeader()->sortIndicatorOrder();
    return state;
}

void Events::restore(const EventViewContext &state)
{
    state_ = state;
    previousCount_ = -1;
    const QSignalBlocker blocker(group_);
    const QSignalBlocker criteriaBlocker(criterion_);
    const QSignalBlocker selectionBlocker(table_->selectionModel());
    group_->setCurrentIndex(qMax(0, group_->findData(state.group)));
    criterion_->setCurrentIndex(qMax(0, criterion_->findData(state.criterion)));
    technicalToggle_->setChecked(false);
    table_->sortByColumn(state.sortColumn, state.sortOrder);
}

void Events::refresh(const MonitorStore &store, const QDateTime &start, const QDateTime &end, bool synchronized)
{
    store_ = &store;
    synchronized_ = synchronized;
    const bool boundsChanged = minimum_ != start || maximum_ != end;
    minimum_ = start;
    maximum_ = end;
    range_->setEnabled(start.isValid() && end.isValid());
    if (boundsChanged || previousCount_ < 0) range_->setBounds(start, end);
    range_->setPeriod(state_.from.isValid() ? state_.from : start, state_.to.isValid() ? state_.to : end, true);
    QStringList groups = store.groups();
    for (const auto &event : store.events())
        if (!groups.contains(event.group)) groups << event.group;
    QStringList shown;
    for (int i = 1; i < group_->count(); ++i) shown << group_->itemData(i).toString();
    if (shown != groups || group_->count() == 0) {
        const QSignalBlocker blocker(group_);
        group_->clear();
        group_->addItem(tr("Все группы"), QString{});
        for (const auto &group : groups) group_->addItem(group, group);
        group_->setCurrentIndex(qMax(0, group_->findData(state_.group)));
    }
    if (!boundsChanged && store.events().size() == previousCount_ && state_.group == previousGroup_
        && state_.criterion == previousCriterion_
        && state_.from == previousFrom_ && state_.to == previousTo_) return;
    const auto selected = state_.event;
    const int scroll = table_->verticalScrollBar()->value();
    {
        const QSignalBlocker block(table_->selectionModel());
        model_->replace(store.events(), state_.group, state_.from, state_.to, state_.criterion);
    }
    QList<QDateTime> starts;
    for (const auto &event : store.events())
        if (EventTable::matches(event, state_.group, state_.criterion)) starts << event.detected;
    histogram_->setData(starts, minimum_, maximum_, state_.from, state_.to);
    previousCount_ = store.events().size();
    previousGroup_ = state_.group;
    previousCriterion_ = state_.criterion;
    previousFrom_ = state_.from;
    previousTo_ = state_.to;
    const auto row = model_->rowOf(selected);
    const bool waiting = !selected.isEmpty() && (store.summary.isEmpty()
        || store.events().size() < store.summary.value("parent_events").toInteger());
    const auto index = row >= 0 ? sort_->mapFromSource(model_->index(row, 0))
                               : waiting ? QModelIndex{} : sort_->index(0, 0);
    if (index.isValid()) table_->setCurrentIndex(index);
    table_->verticalScrollBar()->setValue(scroll);
    showDetails();
    count_->setText(tr("Найдено событий: %1 из %2").arg(sort_->rowCount()).arg(previousCount_));
}

const AnomalyEvent *Events::selectedEvent() const
{
    return model_->at(sort_->mapToSource(table_->currentIndex()).row());
}

void Events::showDetails()
{
    const auto *event = selectedEvent();
    open_->setEnabled(event != nullptr);
    technicalToggle_->setEnabled(event != nullptr);
    title_->setText(event ? tr("Группа %1").arg(event->group) : tr("Выберите событие"));
    detected_->setText(event ? timeText(event->detected) : QStringLiteral("—"));
    closed_->setText(event ? timeText(event->closed) : QStringLiteral("—"));
    duration_->setText(event ? tr("%1 с").arg(numberText(event->payload.value("duration_seconds"))) : QStringLiteral("—"));
    votes_->setText(event ? EventTable::criteria(*event) : QStringLiteral("—"));
    if (!event) { technical_->clear(); return; }
    state_.event = event->id;
    const auto &p = event->payload;
    QStringList lines{
        tr("Причина завершения · %1").arg(closureText(p.value("closure_reason").toString())),
        tr("Первое свидетельство · %1").arg(timeText(p.value("first_evidence"))),
        tr("Узлы-свидетели · %1").arg(listText(p.value("witness_hosts"))),
        tr("Узлы IF · %1").arg(listText(p.value("if_hosts"))),
        tr("ID · %1").arg(event->id)};
    for (const auto &value : p.value("vote_segments").toArray()) {
        const auto span = value.toObject();
        lines << QString("%1 — %2  %3").arg(timeText(span.value("start")), timeText(span.value("end")),
                                               span.value("votes").toString());
    }
    technical_->setText(lines.join('\n'));
}

void Events::setPeriod(const QDateTime &from, const QDateTime &to)
{
    state_.from = from;
    state_.to = to;
    range_->setPeriod(from.isValid() ? from : minimum_, to.isValid() ? to : maximum_);
    emit filterChanged();
}

void Events::exportEvents() { exportCsv(store_); }

void Events::exportCsv(const MonitorStore *store)
{
    if (!store) return;
    QMessageBox dialog(this);
    dialog.setWindowTitle(tr("Экспорт CSV"));
    dialog.setText(synchronized_ ? tr("Выберите события для экспорта.")
                                 : tr("Синхронизация не завершена. Экспортируются доступные события."));
    auto *filtered = dialog.addButton(tr("По фильтрам (%1)").arg(sort_->rowCount()), QMessageBox::AcceptRole);
    auto *all = dialog.addButton(tr("Все события (%1)").arg(store->events().size()), QMessageBox::AcceptRole);
    dialog.addButton(QMessageBox::Cancel);
    dialog.exec();
    QList<AnomalyEvent> events;
    if (dialog.clickedButton() == filtered) {
        for (int row = 0; row < sort_->rowCount(); ++row)
            events << *model_->at(sort_->mapToSource(sort_->index(row, 0)).row());
    } else if (dialog.clickedButton() == all) {
        events = store->events().values();
    } else {
        return;
    }
    auto path = QFileDialog::getSaveFileName(this, tr("Сохранить события"), QStringLiteral("events.csv"),
                                              tr("Таблица CSV (*.csv)"));
    if (path.isEmpty()) return;
    if (!path.endsWith(".csv", Qt::CaseInsensitive)) path += ".csv";
    QSaveFile file(path);
    const auto data = EventTable::csv(events);
    if (!file.open(QIODevice::WriteOnly) || file.write(data) != data.size() || !file.commit()) {
        QMessageBox::warning(this, tr("Ошибка экспорта"), file.errorString());
        return;
    }
    QMessageBox::information(this, tr("Экспорт завершён"), tr("Записано событий: %1\n%2").arg(events.size()).arg(path));
}
