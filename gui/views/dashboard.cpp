#include "dashboard.h"
#include "viewhelpers.h"

#include <QFrame>
#include <QGridLayout>
#include <QHBoxLayout>
#include <QTableView>
#include <QHeaderView>
#include "models/eventtable.h"
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
    header->addWidget(sectionLabel(tr("Последние завершённые события")));
    header->addStretch();
    journal_ = new QPushButton(tr("Все события (0) ›"));
    journal_->setObjectName("link");
    journal_->setAccessibleName(tr("Все события"));
    header->addWidget(journal_);
    layout->addLayout(header);

    auto *table = new QTableView;
    table->setAccessibleName(tr("Последние завершённые события"));
    table->setObjectName("journalTable");
    recent_ = new EventTable(this);
    table->setModel(recent_);
    table->setSelectionMode(QAbstractItemView::SingleSelection);
    table->setSelectionBehavior(QAbstractItemView::SelectRows);
    table->setShowGrid(false);
    table->verticalHeader()->hide();
    table->verticalHeader()->setDefaultSectionSize(42);
    table->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    table->setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    table->setFixedHeight(250);
    layout->addWidget(table);
    layout->addStretch();

    connect(journal_, &QPushButton::clicked, this, &Dashboard::journalSelected);
    const auto open = [this](const QModelIndex &index) {
        if (const auto *event = recent_->at(index.row()))
            emit eventSelected(event->group, event->detected, event->closed);
    };
    connect(table, &QTableView::clicked, this, open);
    connect(table, &QTableView::activated, this, open);
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

    if (eventCount_ == store.events().size() && eventStore_ == &store) return;
    eventStore_ = &store;
    eventCount_ = store.events().size();
    recent_->replace(store.events(), {}, {}, {}, {}, 5);
    journal_->setText(tr("Все события (%1) ›").arg(eventCount_));
}
