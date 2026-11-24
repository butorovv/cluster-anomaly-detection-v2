#include "dashboard.h"
#include "viewhelpers.h"

#include <QFrame>
#include <QGridLayout>
#include <QHBoxLayout>
#include <QListWidget>
#include <QPushButton>
#include <QStyle>
#include <QVBoxLayout>
#include <array>

namespace {
constexpr std::array<GroupState, 4> states{
    GroupState::Negative, GroupState::Positive, GroupState::Unknown, GroupState::Missing};

QLabel *small(const QString &text)
{
    auto *label = new QLabel(text);
    label->setObjectName("small");
    return label;
}
}

Dashboard::Dashboard(QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(12);
    auto *stats = new QHBoxLayout;
    stats->setSpacing(12);
    const QStringList titles{tr("Всего групп"), tr("Норма"), tr("Аномалии"), tr("Неопределённо"), tr("Нет результата")};
    for (qsizetype i = 0; i < titles.size(); ++i) {
        auto *tile = new QFrame;
        tile->setObjectName("panel");
        tile->setFixedHeight(86);
        auto *tileLayout = new QVBoxLayout(tile);
        tileLayout->setContentsMargins(15, 12, 12, 10);
        tileLayout->setSpacing(2);
        auto *number = new QLabel("—");
        number->setObjectName("kpi");
        if (i > 0) number->setStyleSheet("color:" + stateColor(states[i - 1]));
        tileLayout->addWidget(small(titles[i]));
        tileLayout->addWidget(number);
        counts_ << number;
        stats->addWidget(tile, 1);
    }
    layout->addLayout(stats);
    layout->addSpacing(4);
    layout->addWidget(sectionLabel(tr("Топологические группы")));
    grid_ = new QGridLayout;
    grid_->setSpacing(10);
    for (int column = 0; column < 4; ++column) grid_->setColumnStretch(column, 1);
    layout->addLayout(grid_);
    layout->addSpacing(16);
    auto *header = new QHBoxLayout;
    recentTitle_ = sectionLabel({});
    header->addWidget(recentTitle_);
    header->addStretch();
    auto *journal = new QPushButton(tr("Журнал аномалий ›"));
    journal->setObjectName("link");
    header->addWidget(journal);
    layout->addLayout(header);
    recent_ = new QListWidget;
    recent_->setAccessibleName(tr("Аномальные состояния"));
    recent_->setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    layout->addWidget(recent_);
    layout->addStretch();
    connect(journal, &QPushButton::clicked, this, &Dashboard::journalSelected);
    const auto open = [this](QListWidgetItem *item) {
        const auto at = item->data(Qt::UserRole + 1).toDateTime();
        if (!at.isValid()) return;
        const auto group = item->data(Qt::UserRole).toString();
        if (historical_) emit groupSelected(group);
        else emit historySelected(group, at);
    };
    connect(recent_, &QListWidget::itemClicked, this, open);
    connect(recent_, &QListWidget::itemActivated, this, open);
}

void Dashboard::rebuildCards(const QStringList &groups)
{
    for (const auto &card : cards_) delete card.button;
    cards_.clear();
    for (qsizetype i = 0; i < groups.size(); ++i) {
        const auto &group = groups[i];
        auto *card = new QPushButton;
        card->setObjectName("groupButton");
        card->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Preferred);
        card->setMinimumHeight(48);
        auto *line = new QHBoxLayout(card);
        line->setContentsMargins(11, 7, 10, 7);
        line->setSpacing(7);
        auto *dot = new QLabel;
        dot->setFixedSize(9, 9);
        line->addWidget(dot);
        auto *identity = new QVBoxLayout;
        identity->setSpacing(2);
        auto *name = new QLabel(group);
        name->setStyleSheet("font-weight:bold");
        identity->addWidget(name);
        auto *votes = small({});
        votes->hide();
        identity->addWidget(votes);
        line->addLayout(identity);
        line->addStretch();
        auto *status = small({});
        line->addWidget(status);
        auto *arrow = small(QStringLiteral("›"));
        line->addWidget(arrow);
        for (auto *child : {dot, name, votes, status, arrow}) child->setAttribute(Qt::WA_TransparentForMouseEvents);
        grid_->addWidget(card, int(i / 4), int(i % 4));
        cards_.insert(group, {card, dot, votes, status});
        connect(card, &QPushButton::clicked, this, [this, group] { emit groupSelected(group); });
    }
}

void Dashboard::refresh(const MonitorStore &store, const QDateTime &at, bool historical)
{
    historical_ = historical;
    recentTitle_->setText(historical ? tr("Аномалии в выбранный момент") : tr("Последние аномальные состояния"));
    const auto groups = store.groups();
    if (cards_.keys() != groups) rebuildCards(groups);
    std::array<int, 4> totals{};
    for (const auto &group : groups) {
        const auto *result = store.tickAt(group, at);
        const auto state = result ? result->state() : GroupState::Missing;
        for (size_t i = 0; i < states.size(); ++i)
            if (states[i] == state) ++totals[i];
        const auto card = cards_.value(group);
        const auto color = stateColor(state);
        const auto status = stateText(state);
        if (card.status->text() != status) {
            card.status->setText(status);
            card.status->setStyleSheet("color:" + color);
            card.dot->setStyleSheet("border-radius:4px; background:" + color);
            card.button->setAccessibleName(group + " · " + status);
            const bool positive = state == GroupState::Positive;
            card.button->setProperty("anomaly", positive);
            card.button->style()->unpolish(card.button);
            card.button->style()->polish(card.button);
            card.votes->setVisible(positive);
            card.votes->setStyleSheet("color:#c43648");
            card.button->setMinimumHeight(positive ? 59 : 48);
        }
        if (state == GroupState::Positive)
            card.votes->setText(listText(result->temporal.value("active_votes")).replace(", ", " · "));
    }
    counts_.first()->setText(groups.isEmpty() ? QStringLiteral("—") : QString::number(groups.size()));
    for (size_t i = 0; i < totals.size(); ++i)
        counts_[i + 1]->setText(groups.isEmpty() ? QStringLiteral("—") : QString::number(totals[i]));

    QList<const GroupTick *> latest;
    for (const auto *tick : store.recentAnomalies(at, historical ? qMax(1, int(groups.size())) : 5))
        if (!historical || tick->time == at) latest << tick;
    QStringList desired;
    for (const auto *tick : latest)
        desired << QString("%1    %2 UTC · %3").arg(tick->group, tick->time.toString("HH:mm:ss"),
                                                   listText(tick->temporal.value("active_votes")).replace(", ", " · "));
    if (desired.isEmpty()) desired << (at.isValid() ? tr("В доступных данных нет аномалий для этого просмотра.")
                                                    : tr("Результаты для просмотра пока недоступны."));
    QStringList shown;
    for (int i = 0; i < recent_->count(); ++i) shown << recent_->item(i)->text();
    if (shown == desired) return;
    recent_->clear();
    for (qsizetype i = 0; i < desired.size(); ++i) {
        auto *row = new QListWidgetItem(desired[i], recent_);
        row->setSizeHint(QSize(0, latest.isEmpty() ? 54 : 62));
        if (i < latest.size()) {
            const auto *tick = latest[i];
            row->setData(Qt::UserRole, tick->group);
            row->setData(Qt::UserRole + 1, tick->time);
            auto *content = new QWidget;
            content->setAttribute(Qt::WA_TransparentForMouseEvents);
            auto *line = new QHBoxLayout(content);
            line->setContentsMargins(15, 7, 15, 7);
            line->setSpacing(10);
            auto *dot = new QLabel;
            dot->setFixedSize(9, 9);
            dot->setStyleSheet("background:#c43648; border-radius:4px");
            line->addWidget(dot);
            auto *identity = new QVBoxLayout;
            identity->setSpacing(1);
            auto *name = new QLabel(tick->group);
            name->setStyleSheet("font-weight:bold");
            identity->addWidget(name);
            identity->addWidget(small(QString("%1 UTC · %2").arg(tick->time.toString("HH:mm:ss"),
                listText(tick->temporal.value("active_votes")).replace(", ", " · "))));
            line->addLayout(identity);
            line->addStretch();
            auto *more = small(tr("Подробнее ›"));
            more->setStyleSheet("color:#295ec5; font-weight:bold");
            line->addWidget(more);
            recent_->setItemWidget(row, content);
        } else {
            row->setFlags(Qt::NoItemFlags);
        }
    }
    recent_->setFixedHeight((latest.isEmpty() ? 54 : 62) * desired.size() + 2);
}
