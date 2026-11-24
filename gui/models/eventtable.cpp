#include "eventtable.h"

#include <algorithm>

namespace {
const QStringList headers{QStringLiteral("Группа"), QStringLiteral("Обнаружено (UTC)"),
                          QStringLiteral("Длительность, с"), QStringLiteral("Активные критерии")};
}

void EventTable::replace(const QMap<QString, AnomalyEvent> &events, const QString &group,
                         const QDateTime &from, const QDateTime &to)
{
    QList<AnomalyEvent> rows;
    for (const auto &event : events) {
        if ((!group.isEmpty() && event.group != group) || (from.isValid() && event.closed <= from)
            || (to.isValid() && event.detected > to)) continue;
        rows << event;
    }
    std::sort(rows.begin(), rows.end(), [](const AnomalyEvent &a, const AnomalyEvent &b) {
        return a.detected == b.detected ? a.id < b.id : a.detected > b.detected;
    });
    beginResetModel();
    rows_ = std::move(rows);
    endResetModel();
}

int EventTable::rowCount(const QModelIndex &parent) const { return parent.isValid() ? 0 : int(rows_.size()); }
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
    if (!event || role != Qt::DisplayRole) return {};
    switch (index.column()) {
    case 0: return event->group;
    case 1: return timeText(event->detected);
    case 2: return numberText(event->payload.value("duration_seconds"));
    case 3: return event->votes();
    default: return {};
    }
}

QVariant EventTable::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (role == Qt::DisplayRole && orientation == Qt::Horizontal) return headers.value(section);
    return QAbstractTableModel::headerData(section, orientation, role);
}
