#pragma once

#include <QDateTime>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QMap>
#include <QSet>
#include <optional>

enum class GroupState { Missing, Negative, Positive, Unknown };
QString stateText(GroupState state);
GroupState readState(const QJsonValue &value);
QDateTime readTime(const QJsonValue &value);
QString timeText(const QDateTime &time);
QString timeText(const QJsonValue &value);
QString numberText(const QJsonValue &value);
QString listText(const QJsonValue &value);

struct GroupTick {
    QString group;
    QDateTime time;
    QJsonObject raw;
    QJsonObject temporal;
    QJsonObject evidence;
    static GroupTick parse(const QJsonObject &object);
    GroupState state() const { return readState(temporal.value("vote")); }
};

struct ResultCursor {
    QString tick;
    QString group;
    bool operator==(const ResultCursor &) const = default;
};

struct ResultPage {
    QList<GroupTick> items;
    ResultCursor next;
    static std::optional<ResultPage> parse(const QJsonDocument &json, const ResultCursor &after);
};

struct AnomalyEvent {
    QString id;
    QString group;
    QDateTime detected;
    QDateTime closed;
    QJsonObject payload;
    static std::optional<QList<AnomalyEvent>> parse(const QJsonDocument &json, const QString &after);
    QString votes() const;
};

class MonitorStore {
public:
    using Timeline = QMap<qint64, GroupTick>;

    void insert(const QList<GroupTick> &ticks);
    void insert(const QList<AnomalyEvent> &events);
    QStringList groups() const;
    QDateTime latestTime() const;
    QDateTime earliestTime() const;
    const GroupTick *tickAt(const QString &group, const QDateTime &time) const;
    const Timeline &timeline(const QString &group) const;
    QList<const GroupTick *> recentAnomalies(const QDateTime &until, int limit) const;
    qint64 resultCount() const { return resultCount_; }
    const QMap<QString, AnomalyEvent> &events() const { return events_; }
    ResultCursor confirmedCursor() const;
    QJsonObject summary;
    QJsonObject metrics;

private:
    QMap<qint64, QSet<QString>> slices_;
    QMap<QString, Timeline> timelines_;
    QMap<QString, AnomalyEvent> events_;
    QMultiMap<qint64, QString> anomalies_;
    qint64 resultCount_ = 0;
    mutable QDateTime confirmed_;
};
