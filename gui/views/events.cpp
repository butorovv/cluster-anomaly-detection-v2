#include "events.h"
#include "viewhelpers.h"

#include <QComboBox>
#include <QDateTimeEdit>
#include <QDialog>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QFrame>
#include <QHeaderView>
#include <QPushButton>
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
    bar->addWidget(sectionLabel(tr("Завершённые события")));
    bar->addStretch();
    auto *exportButton = new QPushButton(tr("Экспорт CSV"));
    exportButton->setObjectName("soft");
    exportButton->setEnabled(false);
    exportButton->setToolTip(tr("Экспорт подключается после визуальной приёмки"));
    bar->addWidget(exportButton);
    layout->addLayout(bar);
    auto *filters = new QHBoxLayout;
    group_ = new QComboBox;
    group_->setAccessibleName(tr("Фильтр событий по группе"));
    filters->addWidget(group_);
    period_ = new QPushButton(tr("Интервал событий"));
    filters->addWidget(period_);
    periodLabel_ = plainLabel();
    periodLabel_->setObjectName("small");
    filters->addWidget(periodLabel_, 1);
    layout->addLayout(filters);
    count_ = plainLabel();
    count_->setObjectName("small");
    layout->addWidget(count_);

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
    table_->setFixedHeight(302);
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
    connect(period_, &QPushButton::clicked, this, &Events::editPeriod);
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
    const QSignalBlocker selectionBlocker(table_->selectionModel());
    group_->setCurrentIndex(qMax(0, group_->findData(state.group)));
    technicalToggle_->setChecked(false);
    table_->sortByColumn(state.sortColumn, state.sortOrder);
}

void Events::refresh(const MonitorStore &store, const QDateTime &start, const QDateTime &end)
{
    minimum_ = start;
    maximum_ = end;
    period_->setEnabled(start.isValid() && end.isValid());
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
    if (store.events().size() == previousCount_ && state_.group == previousGroup_
        && state_.from == previousFrom_ && state_.to == previousTo_) return;
    const auto selected = state_.event;
    model_->replace(store.events(), state_.group, state_.from, state_.to);
    previousCount_ = store.events().size();
    previousGroup_ = state_.group;
    previousFrom_ = state_.from;
    previousTo_ = state_.to;
    const auto row = model_->rowOf(selected);
    const bool waiting = !selected.isEmpty() && (store.summary.isEmpty()
        || store.events().size() < store.summary.value("parent_events").toInteger());
    const auto index = row >= 0 ? sort_->mapFromSource(model_->index(row, 0))
                               : waiting ? QModelIndex{} : sort_->index(0, 0);
    if (index.isValid()) table_->setCurrentIndex(index);
    showDetails();
    periodLabel_->setText(state_.from.isValid()
        ? tr("%1 — %2").arg(timeText(state_.from), timeText(state_.to))
        : tr("За весь запуск · отдельно от момента диагностики"));
    count_->setText(tr("Показано %1 из %2 событий").arg(sort_->rowCount()).arg(previousCount_));
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
    votes_->setText(event ? event->votes() : QStringLiteral("—"));
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

void Events::editPeriod()
{
    QDialog dialog(this);
    dialog.setWindowTitle(tr("Интервал событий"));
    auto *form = new QFormLayout(&dialog);
    auto *first = new QDateTimeEdit;
    auto *last = new QDateTimeEdit;
    for (auto *edit : {first, last}) {
#if QT_VERSION >= QT_VERSION_CHECK(6, 7, 0)
        edit->setTimeZone(QTimeZone::utc());
#else
        edit->setTimeSpec(Qt::UTC);
#endif
        edit->setDisplayFormat("dd.MM.yyyy HH:mm:ss");
        edit->setCalendarPopup(true);
        edit->setDateTimeRange(minimum_, maximum_);
    }
    first->setDateTime(state_.from.isValid() ? state_.from : minimum_);
    last->setDateTime(state_.to.isValid() ? state_.to : maximum_);
    form->addRow(tr("С (UTC)"), first);
    form->addRow(tr("По (UTC)"), last);
    auto *buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
    auto *reset = buttons->addButton(tr("Весь запуск"), QDialogButtonBox::ResetRole);
    buttons->button(QDialogButtonBox::Ok)->setText(tr("Применить"));
    buttons->button(QDialogButtonBox::Cancel)->setText(tr("Отмена"));
    form->addRow(buttons);
    const auto validate = [first, last, buttons] {
        buttons->button(QDialogButtonBox::Ok)->setEnabled(first->dateTime() <= last->dateTime());
    };
    connect(first, &QDateTimeEdit::dateTimeChanged, &dialog, validate);
    connect(last, &QDateTimeEdit::dateTimeChanged, &dialog, validate);
    connect(reset, &QPushButton::clicked, &dialog, [&dialog] { dialog.done(2); });
    connect(buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);
    const int result = dialog.exec();
    if (result == QDialog::Rejected) return;
    state_.from = result == 2 ? QDateTime{} : first->dateTime();
    state_.to = result == 2 ? QDateTime{} : last->dateTime();
    state_.event.clear();
    emit filterChanged();
}
