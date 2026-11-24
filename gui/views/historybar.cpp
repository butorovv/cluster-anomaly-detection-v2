#include "historybar.h"

#include <QDateTimeEdit>
#include <QDialog>
#include <QDialogButtonBox>
#include <QHBoxLayout>
#include <QLabel>
#include <QPushButton>
#include <QSignalBlocker>
#include <QSlider>
#include <QStackedWidget>
#include <QTimeZone>
#include <QVBoxLayout>

namespace {
QString clockText(const QDateTime &time, const char *format = "HH:mm:ss")
{
    return time.isValid() ? time.toUTC().toString(QString::fromLatin1(format)) : QStringLiteral("—");
}
}

HistoryBar::HistoryBar(QWidget *parent) : QWidget(parent)
{
    setObjectName("historyBar");
    auto *row = new QHBoxLayout(this);
    row->setContentsMargins(0, 0, 0, 0);
    row->setSpacing(13);
    auto *left = new QWidget;
    left->setFixedWidth(186);
    auto *moment = new QVBoxLayout(left);
    moment->setContentsMargins(0, 0, 0, 0);
    moment->setSpacing(1);
    caption_ = new QLabel;
    caption_->setObjectName("small");
    selectedTime_ = new QPushButton;
    selectedTime_->setObjectName("timeValue");
    selectedTime_->setFixedHeight(23);
    selectedTime_->setToolTip(tr("Выбрать точные дату и время UTC"));
    moment->addWidget(caption_);
    moment->addWidget(selectedTime_);
    row->addWidget(left);

    range_ = new QStackedWidget;
    auto *scale = new QWidget;
    auto *scaleLayout = new QVBoxLayout(scale);
    scaleLayout->setContentsMargins(0, 0, 0, 0);
    scaleLayout->setSpacing(1);
    slider_ = new QSlider(Qt::Horizontal);
    slider_->setAccessibleName(tr("Момент диагностики"));
    slider_->setObjectName("momentSlider");
    slider_->setFixedHeight(21);
    scaleLayout->addWidget(slider_);
    auto *labels = new QHBoxLayout;
    first_ = new QLabel;
    middle_ = new QLabel;
    last_ = new QLabel;
    for (auto *label : {first_, middle_, last_}) label->setObjectName("small");
    labels->addWidget(first_, 1);
    labels->addWidget(middle_, 1, Qt::AlignCenter);
    labels->addWidget(last_, 1, Qt::AlignRight);
    scaleLayout->addLayout(labels);
    range_->addWidget(scale);
    auto *note = new QLabel(tr("Наблюдение за поступающими результатами"));
    note->setObjectName("small");
    range_->addWidget(note);
    row->addWidget(range_, 1);
    previous_ = new QPushButton(tr("−10 с"));
    next_ = new QPushButton(tr("+10 с"));
    previous_->setObjectName("timeStep");
    next_->setObjectName("timeStep");
    follow_ = new QPushButton;
    follow_->setObjectName("soft");
    for (auto *item : {previous_, next_, follow_}) {
        item->setFixedHeight(32);
        row->addWidget(item);
    }
    connect(previous_, &QPushButton::clicked, this, [this] { selectTime(at_.addSecs(-10)); });
    connect(next_, &QPushButton::clicked, this, [this] { selectTime(at_.addSecs(10)); });
    connect(slider_, &QSlider::valueChanged, this, [this](int step) { selectTime(minimum_.addSecs(step * 10LL)); });
    connect(follow_, &QPushButton::clicked, this, [this] { emit followRequested(historical_); });
    connect(selectedTime_, &QPushButton::clicked, this, &HistoryBar::openTimePicker);
    render();
}

void HistoryBar::setBounds(const QDateTime &start, const QDateTime &end)
{
    if (start == minimum_ && end == maximum_) return;
    minimum_ = start;
    maximum_ = end;
    if (!start.isValid() || !end.isValid() || start > end) {
        at_ = from_ = to_ = {};
    } else if (at_.isValid()) {
        if (from_.isValid()) from_ = normalized(from_);
        if (to_.isValid()) to_ = normalized(to_);
        at_ = normalized(at_);
    }
    render();
}

QDateTime HistoryBar::normalized(const QDateTime &value) const
{
    if (!minimum_.isValid() || !maximum_.isValid() || minimum_ > maximum_) return {};
    const auto selected = qBound(minimum_, value.isValid() ? value : minimum_, maximum_);
    return minimum_.addSecs(minimum_.secsTo(selected) / 10 * 10);
}

void HistoryBar::setRange(const QDateTime &from, const QDateTime &to, const QDateTime &at)
{
    from_ = from.isValid() ? normalized(from) : QDateTime{};
    to_ = to.isValid() ? normalized(to) : QDateTime{};
    at_ = normalized(at);
    if (from_.isValid() && to_.isValid() && from_ <= to_) at_ = qBound(from_, at_, to_);
    render();
}

void HistoryBar::selectTime(const QDateTime &at)
{
    const auto selected = normalized(at);
    if (!selected.isValid() || selected == at_) return;
    at_ = selected;
    render();
    emit changed();
}

void HistoryBar::setPresentation(bool history, bool canFollow, const QDateTime &latest)
{
    historical_ = history;
    canFollow_ = canFollow;
    latest_ = latest;
    render();
}

void HistoryBar::render()
{
    const auto hasBounds = minimum_.isValid() && maximum_.isValid() && minimum_ <= maximum_;
    const auto time = historical_ ? at_ : latest_;
    caption_->setText(historical_ ? tr("Момент диагностики") : tr("Текущее время телеметрии"));
    selectedTime_->setText(time.isValid() ? clockText(time) + " UTC" : QStringLiteral("—"));
    selectedTime_->setEnabled(historical_ && hasBounds);
    range_->setCurrentIndex(historical_ ? 0 : 1);
    const auto scale = hasBounds ? int(minimum_.secsTo(maximum_) / 10) : 0;
    const QSignalBlocker blocker(slider_);
    slider_->setRange(0, scale);
    slider_->setValue(at_.isValid() ? int(minimum_.secsTo(at_) / 10) : 0);
    slider_->setEnabled(scale > 0);
    first_->setText(clockText(minimum_, "HH:mm"));
    middle_->setText(hasBounds ? clockText(minimum_.addSecs(scale * 5LL), "HH:mm") : QString{});
    last_->setText(clockText(maximum_));
    previous_->setVisible(historical_);
    next_->setVisible(historical_);
    previous_->setEnabled(at_.isValid() && at_ > minimum_);
    next_->setEnabled(at_.isValid() && at_ < maximum_);
    follow_->setVisible(canFollow_);
    follow_->setEnabled(historical_ || hasBounds);
    follow_->setText(historical_ ? tr("К текущему") : tr("Выбрать время"));
}

void HistoryBar::openTimePicker()
{
    QDialog dialog(this);
    dialog.setWindowTitle(tr("Момент диагностики (UTC)"));
    auto *layout = new QVBoxLayout(&dialog);
    auto *time = new QDateTimeEdit;
#if QT_VERSION >= QT_VERSION_CHECK(6, 7, 0)
    time->setTimeZone(QTimeZone::utc());
#else
    time->setTimeSpec(Qt::UTC);
#endif
    time->setDisplayFormat("dd.MM.yyyy HH:mm:ss");
    time->setCalendarPopup(true);
    time->setDateTimeRange(minimum_, maximum_);
    time->setDateTime(at_);
    layout->addWidget(time);
    auto *buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
    buttons->button(QDialogButtonBox::Ok)->setText(tr("Выбрать"));
    buttons->button(QDialogButtonBox::Cancel)->setText(tr("Отмена"));
    layout->addWidget(buttons);
    connect(buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);
    if (dialog.exec() == QDialog::Accepted) selectTime(time->dateTime());
}
