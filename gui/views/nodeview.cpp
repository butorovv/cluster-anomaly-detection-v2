#include "nodeview.h"
#include "viewhelpers.h"

#include <QComboBox>
#include <QCoreApplication>
#include <QFrame>
#include <QGridLayout>
#include <QJsonArray>
#include <QJsonDocument>
#include <QMouseEvent>
#include <QPainter>
#include <QUrlQuery>
#include <QPushButton>
#include <QSignalBlocker>
#include <QTimer>
#include <QVBoxLayout>
#include <cmath>
#include <functional>
#include <limits>

namespace {
constexpr double missing = std::numeric_limits<double>::quiet_NaN();
const QStringList sections{QStringLiteral("Центральные процессоры"), QStringLiteral("Графические процессоры"),
                           QStringLiteral("Блоки питания")};
const QStringList codes{"CPU", "GPU", "PSU"};
constexpr int count[]{2, 6, 2};
constexpr int powers[]{2, 4, 0};
constexpr int temperatures[]{10, 12, -1};

QFrame *panel()
{
    auto *frame = new QFrame;
    frame->setObjectName("panel");
    return frame;
}

QString quantity(double value, bool temperature)
{
    return std::isfinite(value) ? QString::number(value, 'f', 1) + (temperature ? QStringLiteral("\u00a0°C") : QStringLiteral("\u00a0Вт"))
                                : QStringLiteral("Нет данных");
}
}

class PhysicalPlot final : public QWidget {
public:
    PhysicalPlot(int channel, int category, bool temperature, std::function<void(int)> cursor, QWidget *parent)
        : QWidget(parent), channel(channel), category(category), temperature(temperature), hover(std::move(cursor))
    {
        setMouseTracking(true);
        setMinimumWidth(100);
        setFixedHeight(137);
    }
    int channel, category;
    bool temperature;
    double low = 0, high = 1;
    int cursor = -1;
    QDateTime from, to;
    QList<double> values;
protected:
    void paintEvent(QPaintEvent *) override
    {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing);
        p.setPen(QColor("#dce3ec"));
        p.setBrush(Qt::white);
        p.drawRoundedRect(QRectF(rect()).adjusted(.5, .5, -.5, -.5), 7, 7);
        p.setPen(QColor("#192b40"));
        const double value = cursor >= 0 && cursor < values.size() ? values[cursor] : missing;
        p.drawText(QRect(9, 4, width() - 18, 21), Qt::AlignLeft | Qt::AlignVCenter,
            (temperature ? tr("Температура") : tr("Мощность")) + QStringLiteral(" · ") + quantity(value, temperature));
        const auto area = plotRect();
        if (values.isEmpty()) {
            p.setPen(QColor("#697b91"));
            p.drawText(area, Qt::AlignCenter, tr("Нет данных"));
            return;
        }
        for (int i = 0; i < 3; ++i) {
            const qreal y = area.top() + area.height() * i / 2;
            p.setPen(QColor("#e3e9f1"));
            p.drawLine(QPointF(area.left(), y), QPointF(area.right(), y));
            p.setPen(QColor("#697b91"));
            p.drawText(QRectF(3, y - 8, 38, 17), Qt::AlignRight | Qt::AlignVCenter,
                       QString::number(high - (high - low) * i / 2, 'f', 0));
        }
        const auto x = [&](int i) { return values.size() == 1 ? area.right() : area.left() + area.width() * i / (values.size() - 1); };
        const auto y = [&](double v) { return area.bottom() - area.height() * (v - low) / (high - low); };
        const QColor ink(temperature ? "#295ec5" : "#b97826");
        p.setPen(QPen(ink, 1.7));
        p.setBrush(ink);
        for (int i = 0; i < values.size(); ++i) {
            if (!std::isfinite(values[i])) continue;
            const QPointF point(x(i), y(values[i]));
            if (i > 0 && std::isfinite(values[i - 1])) p.drawLine(QPointF(x(i - 1), y(values[i - 1])), point);
            else p.drawEllipse(point, 1.6, 1.6);
        }
        if (cursor >= 0 && cursor < values.size()) {
            p.setPen(QPen(QColor("#8291a5"), 1, Qt::DashLine));
            p.drawLine(QPointF(x(cursor), area.top()), QPointF(x(cursor), area.bottom()));
            if (std::isfinite(value)) { p.setPen(ink); p.drawEllipse(QPointF(x(cursor), y(value)), 3, 3); }
        }
        p.setPen(QColor("#697b91"));
        p.drawText(QRectF(area.left(), area.bottom() + 4, area.width(), 20), Qt::AlignLeft, from.toString("HH:mm:ss"));
        p.drawText(QRectF(area.left(), area.bottom() + 4, area.width(), 20), Qt::AlignRight, to.toString("HH:mm:ss"));
    }
    void mouseMoveEvent(QMouseEvent *event) override
    {
        if (values.isEmpty()) return;
        const auto area = plotRect();
        hover(qBound(0, qRound((event->position().x() - area.left()) / area.width() * (values.size() - 1)),
                     int(values.size() - 1)));
    }
    void leaveEvent(QEvent *) override { hover(-1); }

private:
    QRectF plotRect() const { return {46, 29, qMax(16, width() - 58) * 1.0, 80}; }
    std::function<void(int)> hover;
};

NodeView::NodeView(QWidget *parent) : QWidget(parent), delay_(new QTimer(this))
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(12);
    auto *header = panel();
    auto *head = new QHBoxLayout(header);
    title_ = sectionLabel(tr("Узел"));
    head->addWidget(title_, 1);
    nodes_ = new QComboBox;
    nodes_->setAccessibleName(tr("Узел"));
    head->addWidget(nodes_);
    layout->addWidget(header);

    auto *inventory = panel();
    auto *items = new QVBoxLayout(inventory);
    items->setContentsMargins(16, 12, 16, 14);
    auto *inventoryHeading = new QHBoxLayout;
    inventoryHeading->addWidget(sectionLabel(tr("Состав вычислительного узла")), 1);
    moment_ = plainLabel();
    moment_->setObjectName("small");
    inventoryHeading->addWidget(moment_);
    items->addLayout(inventoryHeading);

    auto *dynamics = panel();
    auto *plots = new QVBoxLayout(dynamics);
    plots->setContentsMargins(16, 12, 16, 14);
    auto *headDynamics = new QHBoxLayout;
    headDynamics->addWidget(sectionLabel(tr("Динамика показателей узла")), 1);
    interval_ = new QComboBox;
    interval_->setAccessibleName(tr("Интервал графиков узла"));
    interval_->addItems({tr("Последние 2 минуты"), tr("Последние 5 минут"), tr("Последние 10 минут")});
    interval_->setCurrentIndex(1);
    headDynamics->addWidget(interval_);
    plots->addLayout(headDynamics);
    cursorText_ = plainLabel();
    plots->addWidget(cursorText_);
    auto *legend = new QLabel(tr("Температура, °C     ·     Мощность, Вт     ·     Синхронный курсор"));
    legend->setObjectName("small");
    plots->addWidget(legend);
    status_ = plainLabel();
    status_->setObjectName("small");
    plots->addWidget(status_);
    for (int i = 0; i < 18; ++i) values_.append(nullptr);

    for (int group = 0; group < sections.size(); ++group) {
        auto *groupName = new QLabel(sections[group]);
        groupName->setObjectName("small");
        items->addWidget(groupName);
        auto *cards = new QGridLayout;
        cards->setSpacing(7);
        for (int i = 0; i < count[group]; ++i) cards->setColumnStretch(i, 1);
        items->addLayout(cards);

        auto *container = panel();
        auto *categoryLayout = new QVBoxLayout(container);
        categoryLayout->setContentsMargins(8, 4, 8, 8);
        auto *toggle = new QPushButton(sections[group]);
        toggle->setObjectName("disclosure");
        toggle->setCheckable(true);
        toggle->setChecked(true);
        categoryLayout->addWidget(toggle);
        auto *rows = new QWidget;
        auto *chartRows = new QGridLayout(rows);
        chartRows->setContentsMargins(0, 0, 0, 0);
        chartRows->setColumnStretch(1, 1);
        chartRows->setColumnStretch(2, 1);
        categoryLayout->addWidget(rows);
        connect(toggle, &QPushButton::toggled, rows, &QWidget::setVisible);
        plots->addWidget(container);

        for (int i = 0; i < count[group]; ++i) {
            const auto name = codes[group] + QString::number(i);
            auto *box = panel();
            auto *card = new QVBoxLayout(box);
            card->setContentsMargins(8, 6, 8, 7);
            card->addWidget(new QLabel(name));
            cards->addWidget(box, 0, i);
            chartRows->addWidget(new QLabel(name), i, 0, Qt::AlignTop);
            const bool dual = group != 2;
            for (int j = dual ? 0 : 1; j < 2; ++j) {
                const int channel = (j == 0 ? temperatures[group] : powers[group]) + i;
                values_[channel] = new QLabel;
                values_[channel]->setObjectName("small");
                values_[channel]->setWordWrap(true);
                card->addWidget(values_[channel]);
                auto *plot = new PhysicalPlot(channel, group, j == 0, [this](int index) { setCursor(index); }, rows);
                plots_.append(plot);
                chartRows->addWidget(plot, i, dual ? j + 1 : 1, 1, dual ? 1 : 2);
            }
        }
    }
    layout->addWidget(inventory);
    layout->addWidget(dynamics);
    layout->addStretch();
    delay_->setSingleShot(true);
    delay_->setInterval(170);
    connect(delay_, &QTimer::timeout, this, &NodeView::request);
    connect(nodes_, &QComboBox::currentTextChanged, this, [this](const QString &node) {
        node_ = node;
        emit contextChanged();
    });
    connect(interval_, &QComboBox::currentIndexChanged, this, &NodeView::contextChanged);
    display();
}

void NodeView::configure(const QUrl &url) { archiveUrl_ = url; }

void NodeView::selectNode(const QString &node)
{
    node_ = node;
    const QSignalBlocker blocker(nodes_);
    nodes_->setCurrentIndex(nodes_->findText(node));
}

int NodeView::interval() const { return interval_->currentIndex(); }

void NodeView::setInterval(int index)
{
    const QSignalBlocker blocker(interval_);
    interval_->setCurrentIndex(qBound(0, index, 2));
}

void NodeView::refresh(const QStringList &hosts, const QDateTime &minimum, const QDateTime &at, const QString &execution)
{
    QStringList shown;
    for (int i = 0; i < nodes_->count(); ++i) shown << nodes_->itemText(i);
    if (shown != hosts) {
        const QSignalBlocker blocker(nodes_);
        nodes_->clear();
        nodes_->addItems(hosts);
    }
    if (!hosts.isEmpty() && !hosts.contains(node_)) node_ = hosts.first();
    selectNode(node_);
    title_->setText(tr("Узел %1").arg(node_));
    moment_->setText(at.isValid() ? timeText(at) : tr("Нет данных"));
    const int seconds[]{120, 300, 600};
    const bool enabled = MasterClient::validUrl(archiveUrl_) && MasterClient::validExecution(execution) &&
        !node_.isEmpty() && minimum.isValid() && at >= minimum;
    const Request next = enabled ? Request{node_, execution, qMax(minimum, at.addSecs(-seconds[interval()])), at} : Request{};
    if (next == desired_) {
        if (!enabled) status_->setText(tr("Ожидание доступного момента телеметрии."));
        else if (!busy_ && !delay_->isActive() && !samples_.contains(at)) retry();
        return;
    }
    if (cursor_ >= 0) {
        const auto hover = desired_.from.addSecs(cursor_ * 10);
        cursor_ = next.from.isValid() && hover >= next.from && hover <= next.to
            ? int(next.from.secsTo(hover) / 10) : -1;
    }
    desired_ = next;
    ++generation_;
    busy_ = false;
    client_.cancel();
    const auto scope = execution + '|' + node_;
    if (scope != sampleScope_) {
        sampleScope_ = scope;
        samples_.clear();
        source_.clear();
        profile_.clear();
    }
    status_->setText(enabled ? tr("Загрузка исходных отсчётов…") : tr("Ожидание доступного момента телеметрии."));
    display();
    if (enabled) delay_->start(170);
}

void NodeView::request()
{
    if (desired_.node.isEmpty() || busy_ || !isVisible()) return;
    auto requested = desired_;
    while (requested.from <= requested.to && samples_.contains(requested.from))
        requested.from = requested.from.addSecs(10);
    while (requested.to >= requested.from && samples_.contains(requested.to))
        requested.to = requested.to.addSecs(-10);
    if (requested.from > requested.to) {
        status_->setText(tr("Физическая телеметрия · подтверждённые отсчёты"));
        return;
    }
    busy_ = true;
    QUrlQuery query;
    query.addQueryItem("execution_id", requested.execution);
    query.addQueryItem("group_id", requested.node.left(3));
    query.addQueryItem("host_id", requested.node);
    query.addQueryItem("from", requested.from.toUTC().toString(Qt::ISODate));
    query.addQueryItem("to", requested.to.addSecs(10).toUTC().toString(Qt::ISODate));
    client_.get(archiveUrl_, "/v1/telemetry", query, true,
        [this, requested, generation = generation_](HttpResult result) { finish(requested, generation, result); });
}

void NodeView::retry()
{
    if (isVisible()) delay_->start(1200);
}

void NodeView::finish(const Request &request, quint64 generation, const HttpResult &result)
{
    if (generation != generation_) return;
    busy_ = false;
    const auto object = result.json.object();
    const auto source = object.value("source_sha256").toString();
    const auto profile = object.value("profile_sha256").toString();
    if (!result.ok() || !object.value("items").isArray() || source.isEmpty() || profile.isEmpty() ||
        object.value("execution_id").toString() != request.execution) {
        status_->setText(tr("Сервис физической телеметрии недоступен."));
        retry();
        return;
    }
    if ((!source_.isEmpty() && source != source_) || (!profile_.isEmpty() && profile != profile_)) {
        samples_.clear();
    }
    source_ = source;
    profile_ = profile;
    for (const auto &row : object.value("items").toArray()) {
        const auto sampleObject = row.toObject();
        const auto at = readTime(sampleObject.value("time"));
        const auto values = sampleObject.value("values").toArray();
        if (at < request.from || at > request.to) continue;
        const auto status = sampleObject.value("status").toString();
        if (status == "missing_observation") { samples_.insert(at, {}); continue; }
        if (status != "available" || values.size() != 18) continue;
        QList<double> sample;
        for (const auto &value : values) sample << (value.isDouble() ? value.toDouble() : missing);
        samples_.insert(at, sample);
    }
    while (samples_.size() > 320) {
        if (samples_.lastKey() > desired_.to) samples_.remove(samples_.lastKey());
        else samples_.remove(samples_.firstKey());
    }
    bool pending = false;
    for (auto t = desired_.from; t.isValid() && t <= desired_.to; t = t.addSecs(10))
        pending = pending || !samples_.contains(t);
    status_->setText(pending ? tr("Ожидание подтверждения физической телеметрии")
        : object.value("provenance").toString() == "verified_import"
            ? tr("Проверенный архив Summit D2") : tr("Физическая телеметрия · подтверждённые отсчёты"));
    display();
    if (pending) retry();
}

void NodeView::display()
{
    const auto current = samples_.value(desired_.to);
    double low[3][2], high[3][2];
    for (int i = 0; i < 3; ++i)
        for (int j = 0; j < 2; ++j) { low[i][j] = INFINITY; high[i][j] = -INFINITY; }
    for (auto *plot : plots_) {
        const int measure = int(plot->temperature);
        const int group = plot->category;
        auto *value = values_[plot->channel];
        value->setText((plot->temperature ? tr("Температура") : tr("Мощность")) + QStringLiteral(": ") +
                       quantity(current.value(plot->channel, missing), plot->temperature));
        plot->values.clear();
        plot->from = desired_.from;
        plot->to = desired_.to;
        for (auto t = desired_.from; t.isValid() && t <= desired_.to; t = t.addSecs(10)) {
            const auto sample = samples_.value(t).value(plot->channel, missing);
            plot->values << sample;
            if (!std::isfinite(sample)) continue;
            low[group][measure] = qMin(low[group][measure], sample);
            high[group][measure] = qMax(high[group][measure], sample);
        }
    }
    for (auto *plot : plots_) {
        const auto lo = low[plot->category][int(plot->temperature)];
        const auto hi = high[plot->category][int(plot->temperature)];
        const auto margin = qMax(1.0, (hi - lo) * .08);
        plot->low = std::isfinite(lo) ? lo - margin : 0;
        plot->high = std::isfinite(hi) ? hi + margin : 1;
    }
    setCursor(cursor_ >= 0 && !plots_.isEmpty() && cursor_ < plots_.first()->values.size() ? cursor_ : -1);
}

void NodeView::setCursor(int index)
{
    cursor_ = index;
    const auto at = index < 0 ? desired_.to : desired_.from.addSecs(index * 10);
    cursorText_->setText(at.isValid() ? tr("%1–%2 UTC · Курсор: %3 UTC").arg(
        desired_.from.toString("HH:mm:ss"), desired_.to.toString("HH:mm:ss"), at.toString("HH:mm:ss")) : QString{});
    for (auto *plot : plots_) {
        plot->cursor = index < 0 ? int(plot->values.size() - 1) : index;
        plot->update();
    }
}
