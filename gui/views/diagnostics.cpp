#include "diagnostics.h"
#include "viewhelpers.h"
#include "models/nodetable.h"
#include "widgets/scorechart.h"

#include <QButtonGroup>
#include <QComboBox>
#include <QFrame>
#include <QHeaderView>
#include <QPushButton>
#include <QSignalBlocker>
#include <QTableView>
#include <QVBoxLayout>

namespace {
const QStringList names{"R", "B", "I2"};
const QStringList keys{"r", "b", "i2"};

QFrame *card()
{
    auto *frame = new QFrame;
    frame->setObjectName("panel");
    return frame;
}

QLabel *caption()
{
    auto *label = plainLabel();
    label->setObjectName("small");
    return label;
}

QWidget *disclosure(const QString &title, QWidget *body, QPushButton **control = nullptr)
{
    auto *frame = card();
    auto *stack = new QVBoxLayout(frame);
    stack->setContentsMargins(16, 9, 16, 9);
    stack->setSpacing(8);
    auto *button = new QPushButton(title + QStringLiteral("   ›"));
    button->setObjectName("disclosure");
    button->setCheckable(true);
    body->hide();
    stack->addWidget(button);
    stack->addWidget(body);
    QObject::connect(button, &QPushButton::toggled, body, &QWidget::setVisible);
    if (control) *control = button;
    return frame;
}

QJsonObject rawCriterion(const GroupTick *tick, int index)
{
    if (!tick) return {};
    return index == 2 ? tick->temporal.value("local_i2").toObject() : tick->raw.value(keys[index]).toObject();
}

QString criterionState(const QJsonObject &criterion)
{
    return stateText(readState(criterion.value("state")));
}
}

Diagnostics::Diagnostics(QWidget *parent) : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(12);
    auto *header = card();
    header->setMinimumHeight(77);
    auto *line = new QHBoxLayout(header);
    line->setContentsMargins(15, 10, 15, 10);
    auto *summary = new QVBoxLayout;
    summary->setSpacing(3);
    auto *title = new QHBoxLayout;
    current_ = sectionLabel({});
    title->addWidget(current_);
    state_ = new QLabel;
    title->addWidget(state_);
    title->addStretch();
    summary->addLayout(title);
    votes_ = caption();
    summary->addWidget(votes_);
    line->addLayout(summary, 1);
    groups_ = new QComboBox;
    groups_->setAccessibleName(tr("Группа"));
    groups_->setFixedWidth(92);
    line->addWidget(groups_);
    layout->addWidget(header);

    auto *cards = new QHBoxLayout;
    cards->setSpacing(12);
    auto *selection = new QButtonGroup(this);
    for (int index = 0; index < names.size(); ++index) {
        auto *item = new QPushButton;
        item->setObjectName("criterion");
        item->setAccessibleName(tr("Критерий %1").arg(names[index]));
        item->setCheckable(true);
        item->setFixedHeight(76);
        auto *body = new QVBoxLayout(item);
        body->setContentsMargins(15, 10, 15, 10);
        body->setSpacing(4);
        auto *name = sectionLabel(tr("Критерий %1").arg(names[index]));
        name->setAttribute(Qt::WA_TransparentForMouseEvents);
        body->addWidget(name);
        auto *value = caption();
        value->setAttribute(Qt::WA_TransparentForMouseEvents);
        body->addWidget(value);
        criterionStates_ << value;
        criteria_ << item;
        selection->addButton(item, index);
        cards->addWidget(item, 1);
    }
    criteria_.first()->setChecked(true);
    layout->addLayout(cards);

    auto *chartPanel = card();
    auto *chartLayout = new QVBoxLayout(chartPanel);
    chartLayout->setContentsMargins(17, 13, 17, 10);
    chartLayout->setSpacing(6);
    auto *tools = new QHBoxLayout;
    chartTitle_ = sectionLabel({});
    tools->addWidget(chartTitle_, 1);
    interval_ = new QComboBox;
    interval_->setAccessibleName(tr("Интервал графика"));
    interval_->addItems({tr("Интервал события"), tr("± 1 минута"), tr("± 5 минут")});
    interval_->setCurrentIndex(1);
    tools->addWidget(interval_);
    chartLayout->addLayout(tools);
    explanation_ = caption();
    chartLayout->addWidget(explanation_);
    chart_ = new ScoreChart;
    chartLayout->addWidget(chart_);
    layout->addWidget(chartPanel);

    detailBody_ = new QWidget;
    auto *detailLayout = new QVBoxLayout(detailBody_);
    detailLayout->setContentsMargins(0, 0, 0, 2);
    details_ = plainLabel();
    detailLayout->addWidget(details_);
    localBlock_ = new QWidget;
    auto *local = new QVBoxLayout(localBlock_);
    local->setContentsMargins(0, 4, 0, 0);
    local->addWidget(sectionLabel(tr("Локальный Isolation Forest")));
    auto *ifNote = caption();
    ifNote->setText(tr("Узловые результаты, входящие в локальный I2. Межгрупповое обогащение Master показывает доли IF."));
    local->addWidget(ifNote);
    nodes_ = new NodeTable(this);
    table_ = new QTableView;
    table_->setObjectName("ifTable");
    table_->setModel(nodes_);
    table_->verticalHeader()->hide();
    table_->verticalHeader()->setDefaultSectionSize(30);
    table_->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    table_->setColumnHidden(4, true);
    table_->setSelectionBehavior(QAbstractItemView::SelectRows);
    local->addWidget(table_);
    outside_ = plainLabel();
    local->addWidget(disclosure(tr("Межгрупповые показатели и полнота"), outside_));
    detailLayout->addWidget(localBlock_);
    layout->addWidget(disclosure({}, detailBody_, &detailToggle_));
    evidence_ = plainLabel();
    evidence_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(disclosure(tr("Технические подробности и свидетельства"), evidence_));
    layout->addStretch();

    connect(groups_, &QComboBox::currentTextChanged, this, [this](const QString &group) {
        selectedGroup_ = group;
        emit selectionChanged();
    });
    connect(interval_, &QComboBox::currentIndexChanged, this, &Diagnostics::selectionChanged);
    connect(selection, &QButtonGroup::idClicked, this, [this](int index) {
        chooseCriterion(index);
        emit selectionChanged();
    });
    connect(chart_, &ScoreChart::timeSelected, this, &Diagnostics::timeSelected);
    chooseCriterion(0);
}

void Diagnostics::selectGroup(const QString &group)
{
    selectedGroup_ = group;
    const QSignalBlocker blocker(groups_);
    groups_->setCurrentIndex(groups_->findText(group));
}

void Diagnostics::setOptions(int criterion, int interval)
{
    const QSignalBlocker blocker(interval_);
    interval_->setCurrentIndex(qBound(0, interval, interval_->count() - 1));
    chooseCriterion(qBound(0, criterion, 2));
}

int Diagnostics::interval() const { return interval_->currentIndex(); }

void Diagnostics::chooseCriterion(int index)
{
    const bool changed = criterion_ != index;
    criterion_ = index;
    criteria_[index]->setChecked(true);
    chartTitle_->setText(tr("Динамика критерия %1").arg(names[index]));
    detailToggle_->setText(tr("Подробные показатели критерия %1   ›").arg(names[index]));
    localBlock_->setVisible(index == 2);
    if (changed) detailToggle_->setChecked(index == 2);
}

void Diagnostics::refresh(const MonitorStore &store, const QDateTime &at, const QDateTime &from, const QDateTime &to,
                          const QString &execution)
{
    const auto desired = store.groups();
    QStringList shown;
    for (int i = 0; i < groups_->count(); ++i) shown << groups_->itemText(i);
    if (desired != shown) {
        const QSignalBlocker blocker(groups_);
        groups_->clear();
        groups_->addItems(desired);
        if (!desired.isEmpty() && !desired.contains(selectedGroup_)) selectedGroup_ = desired.first();
        groups_->setCurrentIndex(groups_->findText(selectedGroup_));
    }
    displayTick(store.tickAt(selectedGroup_, at), execution);
    refreshChart(store, at, from, to);
}

void Diagnostics::displayTick(const GroupTick *tick, const QString &execution)
{
    current_->setText(selectedGroup_.isEmpty() ? tr("Группа") : tr("Группа %1").arg(selectedGroup_));
    const auto state = tick ? tick->state() : GroupState::Missing;
    state_->setText(stateText(state));
    state_->setStyleSheet("padding:5px 9px; border-radius:5px; color:" + stateColor(state)
        + "; background:" + (state == GroupState::Positive ? "#fff0f1" : state == GroupState::Negative ? "#e4f5ed" : "#edf1f6"));
    const auto temporal = tick ? tick->temporal : QJsonObject{};
    const auto active = temporal.value("active_votes").toArray();
    votes_->setText(!tick ? tr("Нет результата на выбранный момент")
        : active.isEmpty() ? tr("Итоговое решение · %1").arg(stateText(state))
                           : tr("Активные критерии: %1").arg(listText(active)));
    for (int i = 0; i < names.size(); ++i) {
        const bool voice = active.contains(names[i]);
        auto *text = criterionStates_[i];
        text->setText(!tick ? tr("●  Нет данных") : voice ? tr("●  Активный голос") : tr("●  Нет активного голоса"));
        text->setStyleSheet(voice ? "color:#c43648" : "color:#697b91");
        criteria_[i]->setToolTip(tr("Исходное состояние: %1\nПосле временной обработки: %2")
            .arg(criterionState(rawCriterion(tick, i)), criterionState(temporal.value(keys[i]).toObject())));
    }
    explanation_->setText(criterion_ == 2 ? tr("Локальный I2 · доступные и аномальные результаты IF")
                                          : tr("Групповые оценки и порог · реальные отсчёты телеметрии"));
    const auto raw = rawCriterion(tick, criterion_);
    const auto processed = temporal.value(keys[criterion_]).toObject();
    QString detail = tr("Исходное состояние · %1\nПосле временной обработки · %2")
        .arg(criterionState(raw), criterionState(processed));
    if (criterion_ != 2) {
        detail += tr("\nГрупповая оценка · %1      Порог · %2").arg(numberText(raw.value("score")), numberText(raw.value("threshold")));
        QStringList definingHosts;
        for (const auto &entry : raw.value("defining_pair").toArray())
            definingHosts << entry.toObject().value("hostname").toString();
        if (!definingHosts.isEmpty()) detail += tr("\nОпределяющая пара · %1").arg(definingHosts.join(", "));
    } else {
        detail += tr("\nДоступно IF · %1      Аномально IF · %2").arg(numberText(raw.value("available")), numberText(raw.value("anomalous")));
        nodes_->setTick(tick);
        table_->setFixedHeight(table_->horizontalHeader()->height() + qBound(2, nodes_->rowCount(), 8) * 30 + 4);
        const auto values = tick ? tick->raw : QJsonObject{};
        const auto support = values.value("if").toObject();
        outside_->setText(tr("Полные наблюдения · %1\nУзлы без наблюдений · %2\nВне группы доступно · %3    Аномально · %4\nДоля внутри s · %5    Доля вне outside_s · %6    Разность e · %7\nДругие группы · %8")
            .arg(numberText(values.value("complete_hosts")), listText(values.value("missing_hosts")),
                 numberText(support.value("outside_available")), numberText(support.value("outside_anomalous")),
                 numberText(support.value("s")), numberText(support.value("outside_s")), numberText(support.value("e")),
                 listText(support.value("outside_groups"))));
    }
    details_->setText(detail);
    QStringList witnesses;
    if (tick) for (const auto &entry : tick->evidence.value("witness_refs").toArray()) {
        const auto ref = entry.toObject();
        witnesses << QString("%1 · %2").arg(ref.value("criterion").toString(), timeText(ref.value("timestamp")));
    }
    evidence_->setText(tr("Execution ID · %1\nδ %2 с\nПричина исходного состояния · %3\nСвидетельства · %4\nID события · %5")
        .arg(execution.isEmpty() ? QString("—") : execution, numberText(temporal.value("delta_seconds")),
             raw.value("reason").toString(QStringLiteral("—")), witnesses.isEmpty() ? QString("—") : witnesses.join("\n"),
             tick ? tick->evidence.value("event_id").toString(QStringLiteral("—")) : QString("—")));
}

void Diagnostics::refreshChart(const MonitorStore &store, const QDateTime &at, const QDateTime &from, const QDateTime &to)
{
    QList<ChartSeries> series{{criterion_ == 2 ? tr("Доступно IF") : tr("Оценка"), QColor("#295ec5"), {}},
                             {criterion_ == 2 ? tr("Аномально IF") : tr("Порог"), QColor("#c43648"), {}, criterion_ != 2}};
    if (!from.isValid() || !to.isValid() || from > to) {
        chart_->setSeries({}, from, to, at);
        return;
    }
    const auto &timeline = store.timeline(selectedGroup_);
    const auto last = timeline.upperBound(to.toMSecsSinceEpoch());
    for (auto it = timeline.lowerBound(from.toMSecsSinceEpoch()); it != last; ++it) {
        const auto raw = rawCriterion(&it.value(), criterion_);
        const auto first = raw.value(criterion_ == 2 ? "available" : "score");
        const auto second = raw.value(criterion_ == 2 ? "anomalous" : "threshold");
        if (first.isDouble()) series[0].points.append(QPointF(it.key(), first.toDouble()));
        if (second.isDouble()) series[1].points.append(QPointF(it.key(), second.toDouble()));
    }
    chart_->setSeries(std::move(series), from, to, at);
}
