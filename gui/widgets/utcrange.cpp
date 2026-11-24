#include "utcrange.h"

#include <QApplication>
#include <QCalendarWidget>
#include <QSignalBlocker>
#include <QDateEdit>
#include <QHBoxLayout>
#include <QLabel>
#include <QLocale>
#include <QTimeEdit>
#include <QTimeZone>
#include <QVBoxLayout>

UtcRange::UtcRange(const QString &startLabel, const QString &endLabel, QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(5);
    auto *fields = new QHBoxLayout;
    layout->addLayout(fields);
    const auto add = [this, fields](const QString &label, QDateEdit *&date, QTimeEdit *&time) {
        auto *column = new QVBoxLayout;
        column->setSpacing(5);
        column->addWidget(new QLabel(label));
        auto *editors = new QHBoxLayout;
        date = new QDateEdit;
        date->setAccessibleName(label + tr(" · дата"));
        date->setDisplayFormat("dd.MM.yyyy");
        date->setCalendarPopup(true);
        date->setKeyboardTracking(false);
        auto *calendar = date->calendarWidget();
        calendar->setObjectName("utcCalendar");
        calendar->setLocale(QLocale(QLocale::Russian, QLocale::Russia));
        calendar->setFirstDayOfWeek(Qt::Monday);
        calendar->setVerticalHeaderFormat(QCalendarWidget::NoVerticalHeader);
        time = new QTimeEdit;
        time->setAccessibleName(label + tr(" · время UTC"));
        time->setDisplayFormat("HH:mm:ss");
        time->setKeyboardTracking(false);
        editors->addWidget(date, 1);
        editors->addWidget(time);
        column->addLayout(editors);
        fields->addLayout(column, 1);
        connect(date, &QDateEdit::dateChanged, this, &UtcRange::edited);
        connect(time, &QTimeEdit::timeChanged, this, &UtcRange::edited);
    };
    add(startLabel, startDate_, startTime_);
    add(endLabel, endDate_, endTime_);
    error_ = new QLabel;
    error_->setObjectName("validationError");
    error_->setWordWrap(true);
    error_->hide();
    layout->addWidget(error_);
}

QDateTime UtcRange::from() const
{
    return {startDate_->date(), startTime_->time(), QTimeZone::utc()};
}

QDateTime UtcRange::to() const
{
    return {endDate_->date(), endTime_->time(), QTimeZone::utc()};
}

void UtcRange::setPeriod(const QDateTime &from, const QDateTime &to, bool preserveEditing)
{
    if (!from.isValid() || !to.isValid()) return;
    if (preserveEditing && (isAncestorOf(QApplication::focusWidget()) ||
        startDate_->calendarWidget()->isVisible() || endDate_->calendarWidget()->isVisible())) return;
    const QSignalBlocker first(startDate_), firstTime(startTime_), last(endDate_), lastTime(endTime_);
    startDate_->setDate(from.toUTC().date());
    startTime_->setTime(from.toUTC().time());
    endDate_->setDate(to.toUTC().date());
    endTime_->setTime(to.toUTC().time());
    error_->hide();
}

void UtcRange::setBounds(const QDateTime &minimum, const QDateTime &maximum)
{
    minimum_ = minimum;
    maximum_ = maximum;
    if (minimum_.isValid() && maximum_.isValid()) {
        const QSignalBlocker first(startDate_->calendarWidget()), last(endDate_->calendarWidget());
        startDate_->calendarWidget()->setDateRange(minimum_.date(), maximum_.date());
        endDate_->calendarWidget()->setDateRange(minimum_.date(), maximum_.date());
    }
}

bool UtcRange::validate(int stepSeconds)
{
    QString issue;
    if (from() >= to()) issue = tr("Окончание должно быть позже начала.");
    else if (!minimum_.isValid() || !maximum_.isValid() || from() < minimum_ || to() > maximum_)
        issue = tr("Период выходит за границы доступной телеметрии.");
    else if (minimum_.secsTo(from()) % stepSeconds || minimum_.secsTo(to()) % stepSeconds)
        issue = tr("Границы должны попадать на сетку с шагом %1 с.").arg(stepSeconds);
    error_->setText(issue);
    error_->setVisible(!issue.isEmpty());
    return issue.isEmpty();
}
