#include "dashboard.h"
#include "viewhelpers.h"

#include <QFrame>
#include <QGridLayout>
#include <QHBoxLayout>
#include <QListWidget>
#include <QPushButton>
#include <QVBoxLayout>
#include <array>

namespace {
constexpr std::array<GroupState, 4> states{
    GroupState::Negative, GroupState::Positive, GroupState::Unknown, GroupState::Missing};
}

Dashboard::Dashboard(QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->addWidget(sectionLabel(tr("Обзор кластера")));
    auto *stats = new QHBoxLayout;
    const QStringList titles{tr("Группы"), tr("Норма"), tr("Аномалия"), tr("Неопределённо"), tr("Нет результата")};
    for (qsizetype i = 0; i < titles.size(); ++i) {
        auto *tile = new QFrame;
        tile->setObjectName("panel");
        auto *tileLayout = new QVBoxLayout(tile);
        tileLayout->setContentsMargins(14, 8, 14, 8);
        auto *number = sectionLabel("—");
        if (i > 0) number->setStyleSheet("color:" + stateColor(states[i - 1]));
        tileLayout->addWidget(number);
        tileLayout->addWidget(new QLabel(titles[i]));
        counts_ << number;
        stats->addWidget(tile, 1);
    }
    layout->addLayout(stats);
    time_ = plainLabel();
    layout->addWidget(time_);
    progress_ = new QProgressBar;
    progress_->setAccessibleName(tr("Прогресс обработки"));
    layout->addWidget(progress_);
    layout->addWidget(sectionLabel(tr("Топологические группы")));
    grid_ = new QGridLayout;
    grid_->setSpacing(8);
    for (int column = 0; column < 4; ++column) grid_->setColumnStretch(column, 1);
    layout->addLayout(grid_);
    layout->addWidget(sectionLabel(tr("Последние аномальные состояния")));
    recent_ = new QListWidget;
    recent_->setAccessibleName(tr("Последние аномальные состояния"));
    recent_->setMinimumHeight(92);
    layout->addWidget(recent_, 1);
    connect(recent_, &QListWidget::itemActivated, this, [this](QListWidgetItem *item) {
        const auto at = item->data(Qt::UserRole + 1).toDateTime();
        if (at.isValid()) emit historySelected(item->data(Qt::UserRole).toString(), at);
    });
}

void Dashboard::rebuildCards(const QStringList &groups)
{
    for (auto *card : cards_) delete card;
    cards_.clear();
    for (qsizetype i = 0; i < groups.size(); ++i) {
        const auto &group = groups[i];
        auto *card = new QPushButton;
        card->setMinimumHeight(51);
        card->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Preferred);
        grid_->addWidget(card, int(i / 4), int(i % 4));
        cards_.insert(group, card);
        connect(card, &QPushButton::clicked, this, [this, group] { emit groupSelected(group); });
    }
}

void Dashboard::refresh(const MonitorStore &store, const QDateTime &at)
{
    const auto groups = store.groups();
    if (cards_.keys() != groups) rebuildCards(groups);
    std::array<int, 4> totals{};
    for (const auto &group : groups) {
        const auto *result = store.tickAt(group, at);
        const auto state = result ? result->state() : GroupState::Missing;
        for (size_t i = 0; i < states.size(); ++i)
            if (states[i] == state) ++totals[i];
        const auto color = stateColor(state);
        auto *card = cards_.value(group);
        const auto text = QString("%1  ·  %2").arg(group, stateText(state));
        if (card->text() != text) {
            card->setText(text);
            card->setStyleSheet(QString("QPushButton { text-align:left; padding:10px; font-weight:bold; "
                                       "color:%1; border-left:5px solid %1; }").arg(color));
        }
    }
    counts_.first()->setText(groups.isEmpty() ? QStringLiteral("—") : QString::number(groups.size()));
    for (size_t i = 0; i < totals.size(); ++i)
        counts_[i + 1]->setText(groups.isEmpty() ? QStringLiteral("—") : QString::number(totals[i]));
    time_->setText(tr("Срез телеметрии  %1     ·     Фактический интервал приёма Master  %2")
                       .arg(timeText(at), receiptDuration(store.summary)));
    setProgress(progress_, store.summary);

    const auto latest = store.recentAnomalies(at, 8);
    QStringList desired;
    for (const auto *tick : latest)
        desired << QString("%1   %2   %3").arg(timeText(tick->time), tick->group,
                                               listText(tick->temporal.value("active_votes")));
    if (desired.isEmpty()) desired << tr("Аномальные состояния в полученных данных отсутствуют.");
    QStringList shown;
    for (int i = 0; i < recent_->count(); ++i) shown << recent_->item(i)->text();
    if (shown == desired) return;
    recent_->clear();
    for (qsizetype i = 0; i < desired.size(); ++i) {
        auto *row = new QListWidgetItem(desired[i], recent_);
        if (i < latest.size()) {
            row->setData(Qt::UserRole, latest[i]->group);
            row->setData(Qt::UserRole + 1, latest[i]->time);
        } else {
            row->setFlags(Qt::NoItemFlags);
        }
    }
}
