#include "monitorstore.h"
#include <QTimeZone>

QString stateText(GroupState state)
{
    switch (state) {
    case GroupState::Negative: return QStringLiteral("Норма");
    case GroupState::Positive: return QStringLiteral("Аномалия");
    case GroupState::Unknown: return QStringLiteral("Неопределённо");
    case GroupState::Missing: return QStringLiteral("Нет результата");
    }
    return {};
}

GroupState readState(const QJsonValue &value)
{
    if (value == "negative") return GroupState::Negative;
    if (value == "positive") return GroupState::Positive;
    if (value == "unknown") return GroupState::Unknown;
    return GroupState::Missing;
}

QDateTime readTime(const QJsonValue &value)
{
    const QDateTime time = QDateTime::fromString(value.toString(), Qt::ISODateWithMs).toUTC();
    return time.date().year() > 1 ? time : QDateTime{};
}

QString timeText(const QDateTime &time)
{
    return time.isValid() ? time.toUTC().toString("dd.MM.yyyy HH:mm:ss 'UTC'") : QStringLiteral("—");
}

QString timeText(const QJsonValue &value)
{
    return timeText(readTime(value));
}

QString numberText(const QJsonValue &value)
{
    return value.isDouble() ? QString::number(value.toDouble(), 'g', 12) : QStringLiteral("—");
}

QString listText(const QJsonValue &value)
{
    QStringList labels;
    for (const auto &entry : value.toArray())
        labels << entry.toString();
    return labels.isEmpty() ? QStringLiteral("—") : labels.join(", ");
}

GroupTick GroupTick::parse(const QJsonObject &object)
{
    auto raw = object.value("raw").toObject();
    const auto temporal = object.value("temporal").toObject();
    const auto evidence = object.value("evidence").toObject();
    const auto group = raw.value("group").toString();
    const auto time = readTime(raw.value("timestamp"));
    raw.remove("host_values");
    raw.remove("vector");
    raw.remove("y");
    for (const auto &key : {QStringLiteral("r"), QStringLiteral("b")}) {
        const auto criterion = raw.value(key).toObject();
        QJsonObject compact{{"state", criterion.value("state")}, {"reason", criterion.value("reason")},
                            {"score", criterion.value("score")}, {"threshold", criterion.value("threshold")}};
        if (key == "r") {
            for (const auto &field : {QStringLiteral("defining_pair"), QStringLiteral("witnesses")}) {
                QJsonArray hosts;
                for (const auto &witness : criterion.value(field).toArray())
                    hosts.append(QJsonObject{{"hostname", witness.toObject().value("hostname")}});
                if (!hosts.isEmpty()) compact.insert(field, hosts);
            }
        }
        raw.insert(key, compact);
    }
    return {group, time, raw, temporal, QJsonObject{{"event_id", evidence.value("event_id")},
                                                   {"witness_refs", evidence.value("witness_refs")}}};
}

std::optional<ResultPage> ResultPage::parse(const QJsonDocument &json, const ResultCursor &after)
{
    const auto object = json.object();
    const auto next = object.value("next").toObject();
    if (!object.value("items").isArray() || !next.value("tick").isString() || !next.value("group_id").isString())
        return std::nullopt;
    ResultPage page{{}, {next.value("tick").toString(), next.value("group_id").toString()}};
    auto previousTime = readTime(after.tick);
    auto previousGroup = after.group;
    for (const auto &item : object.value("items").toArray()) {
        auto tick = GroupTick::parse(item.toObject());
        if (!tick.time.isValid() || tick.group.isEmpty() || tick.state() == GroupState::Missing
            || (previousTime.isValid() && (tick.time < previousTime
                || (tick.time == previousTime && tick.group <= previousGroup))))
            return std::nullopt;
        previousTime = tick.time;
        previousGroup = tick.group;
        page.items.append(std::move(tick));
    }
    if (!page.items.isEmpty() && (page.next == after || readTime(page.next.tick) != previousTime
                                  || page.next.group != previousGroup))
        return std::nullopt;
    return page;
}

std::optional<QList<AnomalyEvent>> AnomalyEvent::parse(const QJsonDocument &json, const QString &after)
{
    if (!json.isArray()) return std::nullopt;
    QList<AnomalyEvent> events;
    QString previous = after;
    for (const auto &item : json.array()) {
        const auto p = item.toObject();
        AnomalyEvent event{p.value("id").toString(), p.value("group").toString(),
                           readTime(p.value("detected")), readTime(p.value("closed")), p};
        if (event.id <= previous || event.group.isEmpty() || !event.detected.isValid()
            || !event.closed.isValid() || event.closed <= event.detected)
            return std::nullopt;
        previous = event.id;
        events.append(std::move(event));
    }
    return events;
}

QString AnomalyEvent::votes() const
{
    QStringList votes;
    for (const auto &value : payload.value("vote_segments").toArray()) {
        const auto segment = value.toObject().value("votes").toString();
        if (!segment.isEmpty() && !votes.contains(segment)) votes << segment;
    }
    return votes.isEmpty() ? QStringLiteral("—") : votes.join(" / ");
}

void MonitorStore::insert(const QList<GroupTick> &ticks)
{
    for (const auto &tick : ticks) {
        const auto ms = tick.time.toMSecsSinceEpoch();
        auto &timeline = timelines_[tick.group];
        if (timeline.contains(ms)) continue;
        timeline.insert(ms, tick);
        slices_[ms].insert(tick.group);
        if (tick.state() == GroupState::Positive) anomalies_.insert(ms, tick.group);
        ++resultCount_;
    }
}

void MonitorStore::insert(const QList<AnomalyEvent> &events)
{
    for (const auto &event : events)
        if (!events_.contains(event.id))
            events_.insert(event.id, event);
}

QStringList MonitorStore::groups() const
{
    QStringList groups;
    const int count = summary.value("group_count").toInt();
    for (int i = 1; i <= count; ++i)
        groups << QString("a%1").arg(i, 2, 10, QLatin1Char('0'));
    for (const auto &group : timelines_.keys())
        if (!groups.contains(group)) groups << group;
    return groups;
}

QDateTime MonitorStore::latestTime() const
{
    return slices_.isEmpty() ? QDateTime{} : QDateTime::fromMSecsSinceEpoch(slices_.lastKey(), QTimeZone(0));
}

QDateTime MonitorStore::earliestTime() const
{
    return slices_.isEmpty() ? QDateTime{} : QDateTime::fromMSecsSinceEpoch(slices_.firstKey(), QTimeZone(0));
}

const GroupTick *MonitorStore::tickAt(const QString &group, const QDateTime &time) const
{
    if (!time.isValid()) return nullptr;
    const auto &ticks = timeline(group);
    const auto it = ticks.find(time.toMSecsSinceEpoch());
    return it == ticks.end() ? nullptr : &it.value();
}

const MonitorStore::Timeline &MonitorStore::timeline(const QString &group) const
{
    static const Timeline empty;
    const auto it = timelines_.find(group);
    return it == timelines_.end() ? empty : it.value();
}

QList<const GroupTick *> MonitorStore::recentAnomalies(const QDateTime &until, int limit) const
{
    QList<const GroupTick *> result;
    if (!until.isValid()) return result;
    auto it = anomalies_.upperBound(until.toMSecsSinceEpoch());
    while (it != anomalies_.cbegin() && result.size() < limit) {
        --it;
        result << tickAt(it.value(), QDateTime::fromMSecsSinceEpoch(it.key(), QTimeZone(0)));
    }
    return result;
}

ResultCursor MonitorStore::confirmedCursor() const
{
    const int count = summary.value("group_count").toInt();
    const auto start = readTime(summary.value("window_start"));
    if (!start.isValid() || count <= 0) return {};
    QDateTime next = confirmed_.isValid() ? confirmed_.addSecs(10) : start;
    for (;;) {
        const auto slice = slices_.constFind(next.toMSecsSinceEpoch());
        if (slice == slices_.cend() || slice->size() != count) break;
        bool complete = true;
        for (int i = 1; i <= count; ++i)
            complete = complete && slice->contains(QString("a%1").arg(i, 2, 10, QLatin1Char('0')));
        if (!complete) break;
        confirmed_ = next;
        next = next.addSecs(10);
    }
    return confirmed_.isValid() ? ResultCursor{confirmed_.toString(Qt::ISODate), QString("a%1").arg(count, 2, 10, QLatin1Char('0'))}
                               : ResultCursor{};
}
