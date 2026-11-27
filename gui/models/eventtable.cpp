#include "eventtable.h"

#include <algorithm>
#include <QRegularExpression>

namespace {
const QStringList headers{QStringLiteral("Группа"), QStringLiteral("Начало (UTC)"),
                          QStringLiteral("Окончание (UTC)"), QStringLiteral("Длит."), QStringLiteral("Критерии")};

QStringList voteTags(const AnomalyEvent &event)
{
    QStringList tags;
    for (const auto &tag : event.votes().split(QRegularExpression("[+ /]+"), Qt::SkipEmptyParts))
        if ((tag == "R" || tag == "B" || tag == "I2") && !tags.contains(tag)) tags << tag;
    return tags;
}
}

void EventTable::replace(const QMap<QString, AnomalyEvent> &events, const QString &group,
                         const QDateTime &from, const QDateTime &to, const QString &criterion, int limit)
{
    QList<AnomalyEvent> rows;
    for (const auto &event : events) {
        if (!matches(event, group, criterion) || (from.isValid() && event.detected < from)
            || (to.isValid() && event.detected >= to)) continue;
        rows << event;
    }
    std::sort(rows.begin(), rows.end(), [](const AnomalyEvent &a, const AnomalyEvent &b) {
        return a.detected == b.detected ? a.id < b.id : a.detected > b.detected;
    });
    if (limit > 0 && rows.size() > limit) rows.resize(limit);
    beginResetModel();
    rows_ = std::move(rows);
    endResetModel();
}

int EventTable::rowCount(const QModelIndex &parent) const { return parent.isValid() ? 0 : int(rows_.size()); }

bool EventTable::matches(const AnomalyEvent &event, const QString &group, const QString &criterion)
{
    return (group.isEmpty() || event.group == group) &&
           (criterion.isEmpty() || voteTags(event).contains(criterion));
}

QString EventTable::criteria(const AnomalyEvent &event)
{
    const auto tags = voteTags(event);
    if (tags.isEmpty()) return QStringLiteral("—");
    QStringList canonical;
    for (const QString &tag : {QStringLiteral("R"), QStringLiteral("B"), QStringLiteral("I2")})
        if (tags.contains(tag)) canonical << tag;
    return canonical.join(" · ");
}

QByteArray EventTable::csv(const QList<AnomalyEvent> &events)
{
    QString result = QStringLiteral("\ufeffevent_id,group,start_utc,end_utc,duration_seconds,criteria\r\n");
    for (const auto &event : events) {
        QStringList fields{event.id, event.group, event.detected.toUTC().toString(Qt::ISODate),
            event.closed.toUTC().toString(Qt::ISODate), numberText(event.payload.value("duration_seconds")),
            criteria(event)};
        for (auto &field : fields) field = '"' + field.replace("\"", "\"\"") + '"';
        result += fields.join(',') + "\r\n";
    }
    return result.toUtf8();
}
int EventTable::columnCount(const QModelIndex &parent) const { return parent.isValid() ? 0 : int(headers.size()); }

const AnomalyEvent *EventTable::at(int row) const
{
    return row >= 0 && row < rows_.size() ? &rows_.at(row) : nullptr;
}

int EventTable::rowOf(const QString &id) const
{
    const auto found = std::find_if(rows_.cbegin(), rows_.cend(), [&id](const AnomalyEvent &event) { return event.id == id; });
    return found == rows_.cend() ? -1 : int(found - rows_.cbegin());
}

QVariant EventTable::data(const QModelIndex &index, int role) const
{
    const auto *event = at(index.row());
    if (!event || (role != Qt::DisplayRole && role != Qt::UserRole && role != Qt::ToolTipRole)) return {};
    if (role == Qt::UserRole) {
        if (index.column() == 1) return event->detected;
        if (index.column() == 2) return event->closed;
        if (index.column() == 3) return event->payload.value("duration_seconds").toInteger();
    }
    if (role == Qt::ToolTipRole && index.column() == 1) return timeText(event->detected);
    if (role == Qt::ToolTipRole && index.column() == 2) return timeText(event->closed);
    switch (index.column()) {
    case 0: return event->group;
    case 1: return event->detected.toString("dd.MM HH:mm:ss");
    case 2: return event->closed.toString("dd.MM HH:mm:ss");
    case 3: return numberText(event->payload.value("duration_seconds")) + QStringLiteral(" с");
    case 4: return criteria(*event);
    default: return {};
    }
}

QVariant EventTable::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (role == Qt::DisplayRole && orientation == Qt::Horizontal) return headers.value(section);
    return QAbstractTableModel::headerData(section, orientation, role);
}
