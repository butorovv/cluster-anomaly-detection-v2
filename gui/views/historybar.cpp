#include "historybar.h"

#include <QDateTimeEdit>
#include <QHBoxLayout>
#include <QLabel>
#include <QPushButton>
#include <QSignalBlocker>
#include <QTimeZone>

HistoryBar::HistoryBar(QWidget *parent) : QWidget(parent)
{
    auto *layout = new QHBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    from_ = new QDateTimeEdit;
    to_ = new QDateTimeEdit;
    at_ = new QDateTimeEdit;
    for (auto *edit : {from_, to_, at_}) {
#if QT_VERSION >= QT_VERSION_CHECK(6, 7, 0)
        edit->setTimeZone(QTimeZone::utc());
#else
        edit->setTimeSpec(Qt::UTC);
#endif
        edit->setDisplayFormat("dd.MM.yyyy HH:mm:ss");
        edit->setCalendarPopup(true);
        edit->setKeyboardTracking(false);
    }
    from_->setAccessibleName(tr("Начало интервала истории, UTC"));
    to_->setAccessibleName(tr("Конец интервала истории, UTC"));
    at_->setAccessibleName(tr("Срез телеметрии, UTC"));
    layout->addWidget(new QLabel(tr("С (UTC)")));
    layout->addWidget(from_);
    layout->addWidget(new QLabel(tr("По")));
    layout->addWidget(to_);
    layout->addWidget(new QLabel(tr("Срез")));
    layout->addWidget(at_);
    auto *previous = new QPushButton(tr("−10 с"));
    auto *next = new QPushButton(tr("+10 с"));
    layout->addWidget(previous);
    layout->addWidget(next);
    layout->addStretch();
    connect(from_, &QDateTimeEdit::dateTimeChanged, this, [this](const QDateTime &value) {
        setRange(value, qMax(value, to()), at());
    });
    connect(to_, &QDateTimeEdit::dateTimeChanged, this, [this](const QDateTime &value) {
        setRange(qMin(value, from()), value, at());
    });
    connect(at_, &QDateTimeEdit::dateTimeChanged, this, &HistoryBar::selectTime);
    connect(previous, &QPushButton::clicked, this, [this] { selectTime(at().addSecs(-10)); });
    connect(next, &QPushButton::clicked, this, [this] { selectTime(at().addSecs(10)); });
    setEnabled(false);
}

void HistoryBar::setBounds(const QDateTime &start, const QDateTime &end)
{
    if (start == minimum_ && end == maximum_) return;
    const bool wasSelected = hasSelection();
    const auto oldFrom = from(), oldTo = to(), oldAt = at();
    minimum_ = start;
    maximum_ = end;
    const bool valid = start.isValid() && end.isValid() && start <= end;
    setEnabled(valid);
    selected_ = false;
    if (!valid) return;
    const QSignalBlocker b1(from_), b2(to_), b3(at_);
    for (auto *edit : {from_, to_, at_}) edit->setDateTimeRange(start, end);
    if (wasSelected) apply(oldFrom, oldTo, oldAt);
}

bool HistoryBar::hasSelection() const { return selected_; }
QDateTime HistoryBar::from() const { return selected_ ? from_->dateTime() : QDateTime{}; }
QDateTime HistoryBar::to() const { return selected_ ? to_->dateTime() : QDateTime{}; }
QDateTime HistoryBar::at() const { return selected_ ? at_->dateTime() : QDateTime{}; }

QDateTime HistoryBar::normalized(const QDateTime &value) const
{
    const auto bounded = qBound(minimum_, value.isValid() ? value : minimum_, maximum_);
    return minimum_.addSecs(minimum_.secsTo(bounded) / 10 * 10);
}

void HistoryBar::apply(const QDateTime &from, const QDateTime &to, const QDateTime &at)
{
    if (!isEnabled()) return;
    const QSignalBlocker b1(from_), b2(to_), b3(at_);
    const auto first = normalized(from);
    const auto last = qMax(first, normalized(to));
    from_->setDateTime(first);
    to_->setDateTime(last);
    at_->setDateTime(qBound(first, normalized(at), last));
    selected_ = true;
}

void HistoryBar::setRange(const QDateTime &from, const QDateTime &to, const QDateTime &at)
{
    apply(from, to, at);
    emit changed();
}

void HistoryBar::selectTime(const QDateTime &at)
{
    setRange(from(), to(), at);
}
